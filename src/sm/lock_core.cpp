#include "w_defines.h"

#define LOCK_CORE_C
#define SM_SOURCE

#ifdef __GNUG__
#pragma implementation "lock_s.h"
#pragma implementation "lock_x.h"
#pragma implementation "lock_core.h"
#endif

#include "st_error_enum_gen.h"
#include "block_alloc.h"

#include "sm_int_1.h"
#include "kvl_t.h"
#include "lock_s.h"
#include "lock_x.h"
#include "lock_core.h"
#include "tls.h"
#include "lock_compt.h"
#include "lock_bucket.h"

#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<unsigned>;
#endif

DECLARE_TLS(block_alloc<lock_queue_t>, lockQueuePool);
DECLARE_TLS(block_alloc<lock_queue_entry_t>, lockEntryPool);
DECLARE_TLS(block_alloc<xct_lock_entry_t>, xctLockEntryPool);

#ifdef SWITCH_DEADLOCK_IMPL
bool g_deadlock_use_waitmap_obsolete = true;
int g_deadlock_dreadlock_interval_ms = 10;
w_rc_t::errcode_t (*g_check_deadlock_impl)(xct_t* xd, lock_request_t *myreq);
#endif // SWITCH_DEADLOCK_IMPL

xct_lock_info_t::xct_lock_info_t() : _head (NULL), _tail (NULL)
{
    _wait_map_obsolete = false;
    init_wait_map(g_me());
}

// allows reuse rather than free/malloc of the structure
xct_lock_info_t* xct_lock_info_t::reset_for_reuse() 
{
    // make sure the lock lists are empty
    w_assert1(_head == NULL);
    w_assert1(_tail == NULL);
    new (this) xct_lock_info_t;
    return this;
}


xct_lock_info_t::~xct_lock_info_t()
{
    w_assert1(_head == NULL);
    w_assert1(_tail == NULL);
}

lock_core_m::lock_core_m(uint sz)
: 
  _htab(0),
  _htabsz(0)
{
    // find _htabsz, a power of 2 greater than sz
    int b=0; // count bits shifted
    for (_htabsz = 1; _htabsz < sz; _htabsz <<= 1) b++;

    w_assert1(!_htab); // just to check size

    w_assert1(_htabsz >= 0x40);
    w_assert1(b >= 6 && b <= 23);
    // if anyone wants a hash table bigger,
    // he's probably in trouble.

    // Now convert to a prime number in that range.
    // get highest prime for that numer:
    b -= 6;

    _htabsz = primes[b];

    _htab = new bucket_t[_htabsz];
    ::memset(_htab, 0, sizeof(_htab));

    w_assert1(_htab);
    
    _lil_global_table = new lil_global_table;
    w_assert1(_lil_global_table);
    _lil_global_table->clear();    
}

lock_core_m::~lock_core_m()
{
    DBGOUT3( << " lock_core_m::~lock_core_m()" );

    delete[] _htab;
    _htab = NULL;
    
    delete _lil_global_table;
    _lil_global_table = NULL;
}


w_rc_t::errcode_t
lock_core_m::acquire_lock(
    xct_t*                 xd,
    const lockid_t&        name,
    lmode_t                mode,
    lmode_t&               prev_mode,
    bool                   check_only,
    timeout_in_ms          timeout)
{
    xct_lock_info_t*       the_xlinfo = xd->lock_info();
    w_assert2(xd == g_xct());

    uint32_t hash = name.hash();
    uint32_t idx = _table_bucket(hash);
    lock_queue_t *lock = _htab[idx].find_lock_queue(hash);

    int acquire_ret = _acquire_lock(xd, lock, mode, prev_mode, check_only, timeout, the_xlinfo);

    w_rc_t::errcode_t funcret;
    if (acquire_ret == RET_SUCCESS) {
        // store the lock queue tag we observed. this is for Safe SX-ELR
        spinlock_read_critical_section cs(&lock->_requests_latch);
        xd->update_read_watermark (lock->x_lock_tag());
        funcret = eOK;
    } else if (acquire_ret == RET_TIMEOUT) {
        funcret = eLOCKTIMEOUT;
    } else {
        w_assert1(acquire_ret == RET_DEADLOCK);
        funcret = eDEADLOCK;
    }
    return funcret;
}

int
lock_core_m::_acquire_lock(
    xct_t*                 xd,
    lock_queue_t*          lock,
    lmode_t                mode,
    lmode_t&               prev_mode,
    bool                   check_only,
    timeout_in_ms          timeout,
    xct_lock_info_t*       the_xlinfo)
{
    // is there already my request?
    smthread_t *thr = g_me();
    lock_queue_entry_t* req = lock->find_request(the_xlinfo);
    if (req == NULL) {
#if W_DEBUG_LEVEL>=3
        {
            spinlock_read_critical_section cs(&lock->_requests_latch);
            for (lock_queue_entry_t* p = lock->_head; p != NULL; p = p->_next) {
                w_assert3(p->_xct != xd);
                w_assert3(p->_thr != thr);
                w_assert3(p->_li != the_xlinfo);
            }
        }
#endif // W_DEBUG_LEVEL>=3
        req = new (*lockEntryPool) lock_queue_entry_t(*xd, *thr, *the_xlinfo, NL, mode);
        if (!check_only) {
            req->_xct_entry = the_xlinfo->link_to_new_request(lock, req);
        }
        lock->append_request(req);
    } else {
        w_assert1(&req->_thr == thr);  // safe if true
        w_assert1(&req->_xct == xd);
        w_assert1(&req->_li == the_xlinfo);
        w_assert1(req->_xct_entry != NULL);
        prev_mode = req->_granted_mode;
        spinlock_write_critical_section cs(&lock->_requests_latch);
        req->_requested_mode = supr[mode][req->_granted_mode];
        w_assert1(req->_requested_mode != LL);
        if (req->_requested_mode == req->_granted_mode) {
            return RET_SUCCESS; // already had the desired lock mode!
        }
    }
    int loop_ret = _acquire_lock_loop(thr, lock, req, check_only, timeout, the_xlinfo);
    
    the_xlinfo->init_wait_map(thr);
    // discard (or downgrade) the failed request. check_only does it even on success.
    if (loop_ret != RET_SUCCESS || check_only) {
        if (req->_granted_mode != NL) {
            // We deny the upgrade but leave the 
            // lock request in place with its former status.
            spinlock_write_critical_section cs(&lock->_requests_latch);
            req->_requested_mode = req->_granted_mode;
        } else {
            // Remove the request
            release_lock (lock, req, lsn_t::null);
        }
    }

    return loop_ret;
}

int
lock_core_m::_acquire_lock_loop(
    smthread_t*            thr,
    lock_queue_t*          lock,
    lock_queue_entry_t*    req,
    bool                   check_only,
    timeout_in_ms          timeout,
    xct_lock_info_t*       the_xlinfo)
{
    const int DREADLOCKS_INTERVAL_MS = g_deadlock_dreadlock_interval_ms;

    timespec   until;
    if (timeout != WAIT_FOREVER && timeout != WAIT_IMMEDIATE) {
        ::clock_gettime(CLOCK_REALTIME, &until);
        until.tv_nsec += (uint64_t) timeout * 1000000;
        until.tv_sec += until.tv_nsec / 1000000000;
        until.tv_nsec = until.tv_nsec % 1000000000;    
    }

    // loop again and again until decisive failure or success
    lock_queue_t::check_grant_result chk_result;
    while (true) {
        lock->check_can_grant(req, chk_result);
        DBGOUT5(<<"check done:" << chk_result.can_be_granted << ","
            << chk_result.deadlock_detected << ","
            << chk_result.deadlock_myself_should_die << ","
            << (void*) chk_result.deadlock_other_victim << ","
            << " fingerprint=" << g_me()->get_fingerprint_map()
            << " new bitmap=" << chk_result.refreshed_wait_map);
        
        if (chk_result.can_be_granted) {
            if (check_only) {
                return RET_SUCCESS;
            }
            bool granted = lock->grant_request(req);
            if (granted) {
                return RET_SUCCESS;
            } else {
                // must be an extremely unlucky case. try again
                DBGOUT1(<<"someone took the lock between check_can_grant and grant_request");
                continue;
            }
        }

        if (chk_result.deadlock_myself_should_die) {
            DBGOUT3(<<"deadlock. I should die! ");
            return RET_DEADLOCK;
        }
        
        if (timeout == WAIT_IMMEDIATE) {
            return RET_TIMEOUT;
        }
        the_xlinfo->refresh_wait_map(chk_result.refreshed_wait_map);
        // either no deadlock or there is a deadlock but 
        // some other xact was selected as victim
        DBGOUT5(<< "blocking:xd=" << xd->tid()  << " mode=" << int(mode) << " timeout=" << timeout);

        w_rc_t::errcode_t rce;
        // TODO: non-rc version of smthread_block
        if (DREADLOCKS_INTERVAL_MS > 0) {
            rce = thr->smthread_block(DREADLOCKS_INTERVAL_MS, 0);
        } else {
            rce = stTIMEOUT; // no wait = spinning
        }

        DBGOUT5(<< "unblocked:xd=" << xd->tid() << " mode="<< int(mode) << " timeout=" << timeout );

        w_assert3(!rce || rce == stTIMEOUT || rce == eDEADLOCK);

        if (rce == eDEADLOCK) {
            return RET_DEADLOCK;
        } else {
            // already past the timeout?
            if (timeout != WAIT_FOREVER) {
                timespec   now;
                ::clock_gettime(CLOCK_REALTIME, &now);
                if (now.tv_sec > until.tv_sec
                    || (now.tv_sec == until.tv_sec && now.tv_nsec >= until.tv_nsec)) {
                    return RET_TIMEOUT;
                }
            }
            // otherwise, once more!
        }
    }
}

void lock_core_m::release_lock(
    lock_queue_t*       lock,
    lock_queue_entry_t* req,
    lsn_t               commit_lsn)
{
    w_assert1(lock);
    w_assert1(req);
    w_assert1(&req->_thr == g_me());  // safe if true
    
    // update lock tag if this is a part of SX-ELR.
    if (commit_lsn.valid()) {
        lmode_t m = req->_granted_mode;
        if (m == XN || m == XS || m == XU || m == XX
            || m == NX || m == SX || m == UX) {
            spinlock_write_critical_section cs(&lock->_requests_latch);
            lock->update_x_lock_tag(commit_lsn);
        }
    }
    lock->detach_request(req);
    //copy these before destroying
    xct_lock_entry_t *xct_entry = req->_xct_entry;
    xct_lock_info_t *li = &req->_li;
    lmode_t released_granted = req->_granted_mode;
    lmode_t released_requested = req->_requested_mode;
    lockEntryPool->destroy_object(req);
    lock->wakeup_waiters(released_granted, released_requested);
    if (xct_entry) {
        li->remove_request(xct_entry);
    }
}


void lock_queue_t::wakeup_waiters(lmode_t released_granted, lmode_t released_requested)
{
    if(g_deadlock_dreadlock_interval_ms == 0) // if ==0 (spin), no need to wakeup
        return;
    if (_head == NULL)
        return; //empty

    if (g_deadlock_use_waitmap_obsolete) {
        // **BEFORE** we wake-up each waiter, we
        // immediately invalidate the wait map so that subsequent
        // spins will suspect this waiter's bitmap might cause false positives.
        // this is only to reduce false positives. not required for consistency.
        // @see xct_lock_info_t::_wait_map_obsolete
        spinlock_read_critical_section cs(&_requests_latch);
        // CRITICAL_SECTION(cs, _requests_latch.read_lock());
        for (lock_queue_entry_t* p = _head; p != NULL; p = p->_next) {
            if (p->_granted_mode != p->_requested_mode
                && !lock_base_t::compat[p->_requested_mode][released_granted]) {
                p->_li.set_wait_map_obsolete(true);
            }
        }
    }
    
    // wakeup a limited number of threads to reduce overhead.
    const int MAX_WAKEUP = 4;
    smthread_t *targets[MAX_WAKEUP];
    int target_count = 0;
    {
        spinlock_read_critical_section cs(&_requests_latch);
        // CRITICAL_SECTION(cs, _requests_latch.read_lock());
        for (lock_queue_entry_t* p = _head; p != NULL; p = p->_next) {
            if (p->_granted_mode != p->_requested_mode
                && !lock_base_t::compat[p->_requested_mode][released_requested]) {
                targets[target_count] = &p->_thr;
                ++target_count;
                if (target_count >= MAX_WAKEUP) {
                    break;
                }
            }
        }
    }
    for (int i = 0; i < target_count; ++i) {
        targets[i]->smthread_unblock(smlevel_0::eOK);
    }
}

rc_t
lock_core_m::release_duration(
    xct_lock_info_t*    the_xlinfo,
    bool                read_lock_only,
    lsn_t               commit_lsn
    )
{
    FUNC(lock_core_m::release_duration);
    DBGOUT4(<<"lock_core_m::release_duration "
            << " tid=" << the_xlinfo->tid()
            << " read_lock_only=" << read_lock_only);

    if (read_lock_only) {
        // releases only read locks
        for (xct_lock_entry_t* p = the_xlinfo->_tail; p != NULL;) {
            xct_lock_entry_t *prev = p->prev; // get this first. release_lock will remove current p
            w_assert1(&p->entry->_thr == g_me());  // safe if true
            lmode_t m = p->entry->_granted_mode;
            if (m == IS || m == SN || m == NS || m == SS
                || m == UN || m == NU || m == UU
                || m == SU || m == US) {
                release_lock(p->queue, p->entry, commit_lsn);
            }
            p = prev;
        }
        // we don't "downgrade" [SU]X/X[SU] to NX/XN for laziness. see ticket:101
    } else {
        //backwards:
        while (the_xlinfo->_tail != NULL)  {
            release_lock(the_xlinfo->_tail->queue, the_xlinfo->_tail->entry, commit_lsn);
        }
    }
    DBGOUT4(<<"lock_core_m::release_duration DONE");
    return RCOK;
}

lock_queue_t* lock_queue_t::allocate_lock_queue(uint32_t hash) {
    return new (*lockQueuePool) lock_queue_t(hash);
}
void lock_queue_t::deallocate_lock_queue(lock_queue_t* obj) {
    lockQueuePool->destroy_object(obj);
}

lock_queue_entry_t* lock_queue_t::find_request (const xct_lock_info_t* myli) {
    spinlock_read_critical_section cs(&_requests_latch);
    // CRITICAL_SECTION(cs, _requests_latch.read_lock()); // read lock suffices
    for (lock_queue_entry_t* p = _head; p != NULL; p = p->_next) {
        if (&p->_li == myli) {
            return p;
        }
    }
    return NULL;
}
void lock_queue_t::append_request (lock_queue_entry_t* myreq) {
    spinlock_write_critical_section cs(&_requests_latch);
    // CRITICAL_SECTION(cs, _requests_latch.write_lock());
    w_assert1(myreq->_granted_mode == smlevel_0::NL);
    if (_head == NULL) {
        _head = myreq;
        _tail = myreq;
    } else {
        _tail->_next = myreq;
        myreq->_prev = _tail;
        _tail = myreq;
    }
}

void lock_queue_t::detach_request (lock_queue_entry_t* myreq) {
    spinlock_write_critical_section cs(&_requests_latch);
    // CRITICAL_SECTION(cs, _requests_latch.write_lock());
#if W_DEBUG_LEVEL>=3
    bool found = false;
    for (lock_queue_entry_t *p = _head; p != NULL; p = p->_next) {
        if (p == myreq) {
            found = true;
            break;
        }
    }
    w_assert3(found);
#endif //W_DEBUG_LEVEL>=3

    if (myreq->_prev == NULL) {
        w_assert1(_head == myreq);
        _head = myreq->_next;
        if (_head != NULL) {
            _head->_prev = NULL;
        }
    } else {
        w_assert1(myreq->_prev->_next == myreq);
        myreq->_prev->_next = myreq->_next;
    }

    if (myreq->_next == NULL) {
        w_assert1(_tail == myreq);
        _tail = myreq->_prev;
        if (_tail != NULL) {
            _tail->_next = NULL;
        }
    } else {
        w_assert1(myreq->_next == _head || myreq->_next->_prev == myreq);
        myreq->_next->_prev = myreq->_prev;
    }
}

bool lock_queue_t::grant_request (lock_queue_entry_t* myreq) {
    spinlock_write_critical_section cs(&_requests_latch);
    // CRITICAL_SECTION(cs, _requests_latch.write_lock());
    w_assert1(&myreq->_thr == g_me());
    bool precedes_me = true;
    lmode_t m = myreq->_requested_mode;
    // check it again.
    for (lock_queue_entry_t* p = _head; p != NULL; p = p->_next) {
        if (p == myreq) {
            precedes_me = false;
            continue;
        }
        bool compatible;
        if (precedes_me) {
            compatible = lock_base_t::compat[p->_requested_mode][m];
        } else {
            compatible = lock_base_t::compat[p->_granted_mode][m];
        }
        if (!compatible) {
            return false;
        }
    }
    // finally granted
    myreq->_granted_mode = myreq->_requested_mode;
    return true;
}

    
void lock_queue_t::check_can_grant (lock_queue_entry_t* myreq, check_grant_result &result) {
    spinlock_read_critical_section cs(&_requests_latch);
    // CRITICAL_SECTION(cs, _requests_latch.read_lock()); // read lock suffices

    // assume here myreq is a member of our queue and hence covered by
    // _request_latch
    const atomic_thread_map_t &myfingerprint = myreq->_thr.get_fingerprint_map();
    result.init(myfingerprint);
    xct_t* myxd = &myreq->_xct;
    bool precedes_me = true;
    lmode_t m = myreq->_requested_mode;

    for (lock_queue_entry_t* p = _head; p != NULL; p = p->_next) {
        if (p == myreq) {
            precedes_me = false;
            continue;
        }
        w_assert1(&p->_li != &myreq->_li);
        w_assert1(&p->_thr != &myreq->_thr);
        bool compatible;
        if (precedes_me) {
            compatible = lock_base_t::compat[p->_requested_mode][m];
        } else {
            compatible = lock_base_t::compat[p->_granted_mode][m];
        }
        if (!compatible) {
            DBGOUT5(<<"incompatible! (pre=" << precedes_me << ")."
                << ", I=" << myfingerprint
                << ", he=" << p->_thr->get_fingerprint_map()
                << ", mine=" << lock_base_t::mode_str[m]
                << ", his:gr=" << lock_base_t::mode_str[p->_granted_mode]
                << "(req=" << lock_base_t::mode_str[p->_requested_mode] << ")");
            result.can_be_granted = false;
            xct_t *theirxd = &p->_xct;
            xct_lock_info_t *theirli = &p->_li;
            if (!theirli->is_wait_map_obsolete()) {
                const atomic_thread_map_t &other = theirli->get_wait_map();
                bool deadlock_detected = other.contains(myfingerprint);

                // Then, take OR with other to update _wait_map (myself)
                result.refreshed_wait_map.merge(other);
                
                if (deadlock_detected) {
                    result.deadlock_detected = true;
                    DBGOUT3(<<"check_can_grant:deadlock!"
                        << " other=" << other
                        << " my fingerprint=" << myfingerprint
                        << " my bitmap=" << result.refreshed_wait_map);

                    uint32_t my_chain_len = myxd->get_xct_chain_len();
                    uint32_t their_chain_len = theirxd->get_xct_chain_len();
                
                    bool killhim;
                    // If one of them is chained transaction, let's victimize
                    // shorter chain because it will quickly converge in dominated lock table.
                    // See ticket:102
                    if (their_chain_len < my_chain_len) {
                        killhim = true;
                    } else if (their_chain_len > my_chain_len) {
                        killhim = false;
                    } else {
                        // if chain length is same, kill younger xct (larger tid)
                        killhim = myxd->tid() < theirxd->tid();
                    }

                    if (killhim) {
                        DBGOUT3(<<"kill him");
                        result.deadlock_other_victim = &p->_thr;
                    } else {
                        DBGOUT3(<<"kill myself");
                        result.deadlock_myself_should_die = true;
                        return;
                    }
                }
            } else {
                DBGOUT4(<<"but ignored as obsolete");
            }
        }
    }
    w_assert1(!precedes_me);
}


xct_lock_entry_t* xct_lock_info_t::link_to_new_request (lock_queue_t *queue, lock_queue_entry_t *entry) {
    xct_lock_entry_t* link = new (*xctLockEntryPool) xct_lock_entry_t();
    link->queue = queue;
    link->entry = entry;
    if (_head == NULL) {
        _head = link;
        _tail = link;
    } else {
        _tail->next = link;
        link->prev = _tail;
        _tail = link;
    }
    return link;
}
void xct_lock_info_t::remove_request (xct_lock_entry_t *entry) {
#if W_DEBUG_LEVEL>=3
    bool found = false;
    for (xct_lock_entry_t *p = _head; p != NULL; p = p->next) {
        if (p == entry) {
            found = true;
            break;
        }
    }
    w_assert3(found);
#endif //W_DEBUG_LEVEL>=3
    if (entry->prev == NULL) {
        // then it should be current head
        w_assert1(_head == entry);
        _head = entry->next;
        if (_head != NULL) {
            _head->prev = NULL;
        }
    } else {
        w_assert1(entry->prev->next == entry);
        entry->prev->next = entry->next;
    }

    if (entry->next == NULL) {
        // then it should be current tail
        w_assert1(_tail == entry);
        _tail = entry->prev;
        if (_tail != NULL) {
            _tail->next = NULL;
        }
    } else {
        w_assert1(entry->next == _head || entry->next->prev == entry);
        entry->next->prev = entry->prev;
    }
    
    xctLockEntryPool->destroy_object(entry);
}
