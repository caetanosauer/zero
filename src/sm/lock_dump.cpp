#include "w_defines.h"

#define LOCK_CORE_C
#define SM_SOURCE

#include "sm_int_1.h"
#include "kvl_t.h"
#include "lock_s.h"
#include "lock_x.h"
#include "lock_core.h"
#include "lock_bucket.h"

#include "w_strstream.h"
#include <sstream>

/** implementation of dump/output/other debug utility functions in lock_core,lock_m. */

// Obviously not mt-safe:
ostream &                        
xct_lock_info_t::dump_locks(ostream &out) const
{
    /*
    const lock_request_t         *req;
    const lock_head_t                 *lh;
    request_list_i   iter(my_req_list); // obviously not mt-safe
    while ((req = iter.next())) {
        w_assert9(req->xd == xct());
        lh = req->get_lock_head();
        out << "Lock: " << lh->name 
            << " Mode: " << int(req->mode())
            << " State: " << int(req->status()) <<endl;
    }
    */
    return out;
}


/*********************************************************************
 *
 *  xct_lock_info_t output operator
 *
 *********************************************************************/
#if 1
ostream& operator<<(ostream &o, const xct_lock_info_t &) { return o; }
#else
ostream &            
operator<<(ostream &o, const xct_lock_info_t &x)
{
        lock_request_t *waiting = x.waiting_request();
        if (waiting) {
                o << " wait: " << *waiting;
        }
        return o;
}
#endif

/*********************************************************************
 *
 *  lock_core_m::dump()
 *
 *  Dump the lock hash table (for debugging).
 *
 *********************************************************************/
#if 1
void lock_core_m::dump(ostream &) {}
#else
/*
// disabled because there's no safe way to iterate over the lock table
// but you can use it in a debugger.  It is used by smsh in
// single-thread cases.
*/
void
lock_core_m::dump(ostream & o)
{
    o << "WARNING: lock_core_m::dump is not thread-safe:" << endl;
    o << "lock_core_m:"
      << " _htabsz=" << _htabsz
      << endl;
    for (unsigned h = 0; h < _htabsz; h++)  {
        ACQUIRE_BUCKET_MUTEX(h);
        chain_list_i i(_htab[h].chain);
        lock_head_t* lock;
        lock = i.next();
        if (lock) {
            o << h << ": ";
        }
        while (lock)  {
            // First, verify the hash function:
            unsigned hh = _table_bucket(lock->name.hash());
            if(hh != h) {
                o << "ERROR!  hash table bucket h=" << h 
                    << " contains lock head " << *lock
                    << " which hashes to " << hh
                    << endl;
            }
            
            ACQUIRE_HEAD_MUTEX(lock); // this is dump
            o << "\t " << *lock << endl;
            lock_request_t* request;
            lock_head_t::safe_queue_iterator_t r(*lock);

            while ((request = r.next()))  {
                o << "\t\t" << *request << endl;
            }
            RELEASE_HEAD_MUTEX(lock); // acquired here NOT through find_lock_head
            lock = i.next();
        }
        RELEASE_BUCKET_MUTEX(h);
    }
}
#endif


void lock_core_m::dump()
{
    dump(cerr);
    cerr << flushl;
}

void
lock_core_m::assert_empty() const
{
    int found_request=0;

    for (uint h = 0; h < _htabsz; h++)   {
        // empty queue is fine. just check leftover requests
        for (lock_queue_t *queue = _htab[h]._queue; queue != NULL; queue = queue->next()) {
            for (lock_queue_entry_t *req = queue->_head; req != NULL; req = req->_next) {
                ++found_request;
                DBGOUT1("leftover lock request(hash=" << queue->hash() << "):" << *req);
            }
        }
    }
    w_assert1(found_request == 0);
}


void
lock_core_m::_dump(ostream &o)
{
    for (uint h = 0; h < _htabsz; h++)  {
        // empty queue is fine. just check leftover requests
        for (lock_queue_t *queue = _htab[h]._queue; queue != NULL; queue = queue->next()) {
            for (lock_queue_entry_t *req = queue->_head; req != NULL; req = req->_next) {
                o << "lock request(hash=" << queue->hash() << "):" << *req << endl;
            }
        }
    }
    o << "--end of lock table--" << endl;
}

ostream& 
operator<<(ostream& o, const lock_queue_entry_t& r)
{
    o << "xct:" << r._li.tid()
      << " granted-mode:" << lock_base_t::mode_str[r._granted_mode]
      << " req-mode:" << lock_base_t::mode_str[r._requested_mode]
      << " thr:" << r._thr.get_fingerprint_map()
      << " wait-map:" << r._li.get_wait_map();

    return o;
}


/*********************************************************************
 *
 *  operator<<(ostream, lockid)
 *
 *  Pretty print a lockid to "ostream".
 *
 *********************************************************************/
ostream& 
operator<<(ostream& o, const lockid_t& i)
{
    stid_t s;
    i.extract_stid(s);
    o << "L(" << s << ": key-hash=" << i.l[1];
    return o << ')';
}
