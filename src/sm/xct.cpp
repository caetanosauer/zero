/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define XCT_C

#include <new>
#include "sm_base.h"

#include "tls.h"

#include "lock.h"
#include <sm_base.h>
#include "xct.h"
#include "lock_x.h"
#include "lock_lil.h"

#include <sm.h>
#include "tls.h"
#include <sstream>
#include "chkpt.h"
#include "logrec.h"
#include "bf_tree.h"
#include "lock_raw.h"
#include "log_lsn_tracker.h"
#include "log_core.h"
#include "xct_logger.h"

#include "allocator.h"

// CS TODO this mutex should not be necessary
// Certain operations have to exclude xcts
static srwlock_t          _begin_xct_mutex;

// Template specializations for sm_tls_allocator
DECLARE_TLS(block_pool<xct_t>, xct_pool);
DECLARE_TLS(block_pool<xct_t::xct_core>, xct_core_pool);

template<>
xct_t* sm_tls_allocator::allocate<xct_t>(size_t)
{
    return (xct_t*) xct_pool->acquire();
}

template<>
void sm_tls_allocator::release(xct_t* p, size_t)
{
    xct_pool->release(p);
}

template<>
xct_t::xct_core* sm_tls_allocator::allocate<xct_t::xct_core>(size_t)
{
    return (xct_t::xct_core*) xct_core_pool->acquire();
}

template<>
void sm_tls_allocator::release(xct_t::xct_core* p, size_t)
{
    xct_core_pool->release(p);
}

#define DBGX(arg) DBG(<< "tid." << _tid  arg)

// If we run into btree shrinking activity, we'll bump up the
// fudge factor, b/c to undo a lot of btree removes (incremental
// tree removes) takes about 4X the logging...
extern double logfudge_factors[logrec_t::t_max_logrec]; // in logstub.cpp
#define UNDO_FUDGE_FACTOR(t, nbytes) int((logfudge_factors[t])*(nbytes))

#ifdef W_TRACE
extern "C" void debugflags(const char *);
void
debugflags(const char *a)
{
   _w_debug.setflags(a);
}
#endif /* W_TRACE */

int auto_rollback_t::_count = 0;

/*********************************************************************
 *
 *  The xct list is sorted for easy access to the oldest and
 *  youngest transaction. All instantiated xct_t objects are
 *  in the list.
 *
 *  Here are the transaction list and the mutex that protects it.
 *
 *********************************************************************/

queue_based_lock_t        xct_t::_xlist_mutex;

w_descend_list_t<xct_t, queue_based_lock_t, tid_t>
        xct_t::_xlist(W_KEYED_ARG(xct_t, _tid,_xlink), &_xlist_mutex);

bool xct_t::xlist_mutex_is_mine()
{
     bool is =
        smthread_t::get_xlist_mutex_node()._held
        &&
        (smthread_t::get_xlist_mutex_node()._held->
            is_mine(&smthread_t::get_xlist_mutex_node()));
     return is;
}
void xct_t::assert_xlist_mutex_not_mine()
{
    w_assert1(
            (smthread_t::get_xlist_mutex_node()._held == 0)
           ||
           (smthread_t::get_xlist_mutex_node()._held->
               is_mine(&smthread_t::get_xlist_mutex_node())==false));
}
void xct_t::assert_xlist_mutex_is_mine()
{
#if W_DEBUG_LEVEL > 1
    bool res =
     smthread_t::get_xlist_mutex_node()._held
        && (smthread_t::get_xlist_mutex_node()._held->
            is_mine(&smthread_t::get_xlist_mutex_node()));
    if(!res) {
        fprintf(stderr, "held: %p\n",
             smthread_t::get_xlist_mutex_node()._held );
        if ( smthread_t::get_xlist_mutex_node()._held  )
        {
        fprintf(stderr, "ismine: %d\n",
            smthread_t::get_xlist_mutex_node()._held->
            is_mine(&smthread_t::get_xlist_mutex_node()));
        }
        w_assert1(0);
    }
#else
     w_assert1(smthread_t::get_xlist_mutex_node()._held
        && (smthread_t::get_xlist_mutex_node()._held->
            is_mine(&smthread_t::get_xlist_mutex_node())));
#endif
}

w_rc_t  xct_t::acquire_xlist_mutex()
{
     assert_xlist_mutex_not_mine();
     _xlist_mutex.acquire(&smthread_t::get_xlist_mutex_node());
     assert_xlist_mutex_is_mine();
     return RCOK;
}

void  xct_t::release_xlist_mutex()
{
     assert_xlist_mutex_is_mine();
     _xlist_mutex.release(smthread_t::get_xlist_mutex_node());
     assert_xlist_mutex_not_mine();
}

/*********************************************************************
 *
 *  _nxt_tid is used to generate unique transaction id
 *  _1thread_name is the name of the mutex protecting the xct_t from
 *          multi-thread access
 *
 *********************************************************************/
tid_t                                 xct_t::_nxt_tid = 0;

/*********************************************************************
 *
 *  _oldest_tid is the oldest currently-running tx (well, could be
 *  committed by now - the xct destructor updates this)
 *  This corresponds to the Shore-MT paper section 7.3, top of
 *  2nd column, page 10.
 *
 *********************************************************************/
tid_t                                xct_t::_oldest_tid = 0;

inline bool   xct_t::should_consume_rollback_resv(int t) const
{
     if(state() == xct_aborting) {
         w_assert0(_rolling_back);
     } // but not the reverse: rolling_back
     // could be true while we're active
     // _core->xct_aborted means we called abort but
     // we might be in freeing_space state right now, in
     // which case, _rolling_back isn't true.
    return
        // _rolling_back means in rollback(),
        // which can be in abort or in
        // rollback_work.
        _rolling_back || _core->_xct_aborting
        // compensate is a special case:
        // consume rollback space
        || t == logrec_t::t_compensate ;
 }

struct lock_info_ptr {
    xct_lock_info_t* _ptr;

    lock_info_ptr() : _ptr(0) { }

    xct_lock_info_t* take() {
        if(xct_lock_info_t* rval = _ptr) {
            _ptr = 0;
            return rval;
        }
        return new xct_lock_info_t;
    }
    void put(xct_lock_info_t* ptr) {
        if(_ptr)
            delete _ptr;
        _ptr = ptr? ptr->reset_for_reuse() : 0;
    }

    ~lock_info_ptr() { put(0); }
};

DECLARE_TLS(lock_info_ptr, agent_lock_info);

struct lil_lock_info_ptr {
    lil_private_table* _ptr;

    lil_lock_info_ptr() : _ptr(0) { }

    lil_private_table* take() {
        if(lil_private_table* rval = _ptr) {
            _ptr = 0;
            return rval;
        }
        return new lil_private_table;
    }
    void put(lil_private_table* ptr) {
        if(_ptr)
            delete _ptr;
        if (ptr) {
            ptr->clear();
        }
        _ptr = ptr;
    }

    ~lil_lock_info_ptr() { put(0); }
};

DECLARE_TLS(lil_lock_info_ptr, agent_lil_lock_info);

// Define customized new and delete operators for sm allocation
DEFINE_SM_ALLOC(xct_t);
DEFINE_SM_ALLOC(xct_t::xct_core);

xct_t::xct_core::xct_core(tid_t const &t, state_t s, int timeout)
    :
    _tid(t),
    _timeout(timeout),
    _warn_on(true),
    _lock_info(agent_lock_info->take()),
    _lil_lock_info(agent_lil_lock_info->take()),
    _raw_lock_xct(NULL),
    _state(s),
    _read_only(false),
    _xct_ended(0), // for assertions
    _xct_aborting(0)
{
    _lock_info->set_tid(_tid);
    w_assert1(_tid == _lock_info->tid());

    w_assert1(_lil_lock_info);
    if (smlevel_0::lm) {
        _raw_lock_xct = smlevel_0::lm->allocate_xct();
    }

    INC_TSTAT(begin_xct_cnt);

}

/*********************************************************************
 *
 *  xct_t::xct_t(that, type)
 *
 *  Begin a transaction. The transaction id is assigned automatically,
 *  and the xct record is inserted into _xlist.
 *
 *********************************************************************/
xct_t::xct_t(sm_stats_info_t* stats, int timeout, bool sys_xct,
           bool single_log_sys_xct, const tid_t& given_tid, const lsn_t& last_lsn,
           const lsn_t& undo_nxt, bool loser_xct
            )
    :
    _core(new xct_core(
                given_tid == 0 ? ++_nxt_tid : given_tid,
                xct_active, timeout)),
    __stats(stats),
    __saved_lockid_t(0),
    _tid(_core->_tid),
    _xct_chain_len(0),
    _ssx_chain_len(0),
    _query_concurrency (smlevel_0::t_cc_none),
    _query_exlock_for_select(false),
    _piggy_backed_single_log_sys_xct(false),
    _sys_xct (sys_xct),
    _single_log_sys_xct (single_log_sys_xct),
    _inquery_verify(false),
    _inquery_verify_keyorder(false),
    _inquery_verify_space(false),
    // _first_lsn, _last_lsn, _undo_nxt,
    _last_lsn(last_lsn),
    _undo_nxt(undo_nxt),
    _read_watermark(lsn_t::null),
    _elr_mode (elr_none),
    // _last_log(0),
#if CHECK_NESTING_VARIABLES
#endif
    // _log_buf(0),
    _rolling_back(false),
    _in_compensated_op(0)
#if W_DEBUG_LEVEL > 2
    ,
    _had_error(false)
#endif
{
    w_assert3(state() == xct_active);
    if (given_tid != 0 && _nxt_tid < given_tid) {
        _nxt_tid = given_tid;
    }

    w_assert1(tid() == _core->_tid);
    w_assert3(tid() <= _nxt_tid);
    w_assert2(tid() <= _nxt_tid);
    w_assert1(tid() == _core->_lock_info->tid());

    if (true == loser_xct)
        _loser_xct = loser_true;  // A loser transaction
    else
        _loser_xct = loser_false; // Not a loser transaction

    // _log_buf = new logrec_t; // deleted when xct goes away
    // _log_buf_for_piggybacked_ssx = new logrec_t;

// #ifdef ZERO_INIT
//     memset(_log_buf, '\0', sizeof(logrec_t));
//     memset(_log_buf_for_piggybacked_ssx, '\0', sizeof(logrec_t));
// #endif

    // if (!_log_buf || !_log_buf_for_piggybacked_ssx)  {
    //     W_FATAL(eOUTOFMEMORY);
    // }

    if (timeout_c() == timeout_t::WAIT_SPECIFIED_BY_THREAD) {
        // override in this case
        set_timeout(smthread_t::lock_timeout());
    }
    w_assert9(timeout_c() >= 0 || timeout_c() == timeout_t::WAIT_FOREVER);

    put_in_order();

    w_assert3(state() == xct_active);

    if (given_tid == 0) {
        smthread_t::attach_xct(this);
    }
    else {
        w_assert1(smthread_t::xct() == 0);
    }

    w_assert3(state() == xct_active);
    _begin_tstamp = std::chrono::high_resolution_clock::now();
}

void xct_t::begin(bool sys_xct, bool single_log_sys_xct, int timeout,
        sm_stats_info_t *_stats)
{
    w_assert1(!single_log_sys_xct || sys_xct); // SSX is always system-transaction

    // system transaction can be a nested transaction, so
    // xct() could be non-NULL
    if (!sys_xct && xct() != NULL) {
        W_COERCE(RC(eINTRANS));
    }

    xct_t* x;
    if (sys_xct) {
        x = xct();
        if (single_log_sys_xct && x) {
            // in this case, we don't need an independent transaction object.
            // we just piggy back on the outer transaction
            if (x->is_piggy_backed_single_log_sys_xct()) {
                // SSX can't nest SSX, but we can chain consecutive SSXs.
                ++(x->ssx_chain_len());
            } else {
                x->set_piggy_backed_single_log_sys_xct(true);
            }
            return;
        }
        x = new xct_t(_stats, timeout, sys_xct, single_log_sys_xct, false);
    } else {
        spinlock_read_critical_section cs(&_begin_xct_mutex);
        x = new xct_t(_stats, timeout, sys_xct, single_log_sys_xct, false);
        if(log) {
            // This transaction will make no events related to LSN
            // smaller than this. Used to control garbage collection, etc.
            log->get_oldest_lsn_tracker()->enter(reinterpret_cast<uintptr_t>(x), log->curr_lsn());
        }
    }

    w_assert3(xct() == x);
    w_assert3(x->state() == xct_t::xct_active);
}

xct_t::xct_core::~xct_core()
{
    w_assert3(_state == xct_ended);
    if(_lock_info) {
        agent_lock_info->put(_lock_info);
    }
    if (_lil_lock_info) {
        agent_lil_lock_info->put(_lil_lock_info);
    }
    if (_raw_lock_xct) {
        smlevel_0::lm->deallocate_xct(_raw_lock_xct);
    }
}

/*********************************************************************
 *
 *  xct_t::~xct_t()
 *
 *  Clean up and free up memory used by the transaction. The
 *  transaction has normally ended (committed or aborted)
 *  when this routine is called.
 *
 *********************************************************************/
xct_t::~xct_t()
{
    w_assert9(__stats == 0);

    if (!_sys_xct && smlevel_0::log) {
        smlevel_0::log->get_oldest_lsn_tracker()->leave(
                reinterpret_cast<uintptr_t>(this));
    }
    LOGREC_ACCOUNTING_PRINT // see logrec.h

    _teardown(false);
    w_assert3(_in_compensated_op==0);

    if (shutdown_clean)  {
        // if this transaction is system transaction,
        // the thread might be still conveying another thread
        w_assert1(is_sys_xct() || smthread_t::xct() == 0);
    }

    // clean up what's stored in the thread
    smthread_t::no_xct(this);

    if(__saved_lockid_t)  {
        delete[] __saved_lockid_t;
        __saved_lockid_t=0;
    }

        if(_core)
            delete _core;
        _core = NULL;
    // if (LATCH_NL != latch().mode())
    // {
    //     // Someone is accessing this txn, wait until it finished
    //     w_rc_t latch_rc = latch().latch_acquire(LATCH_EX, timeout_t::WAIT_FOREVER);

    //     // Now we can delete the core, no one can acquire latch on this txn after this point
    //     // since transaction is being destroyed

    //     if (false == latch_rc.is_error())
    //     {
    //         // CS TODO if _core is nullified above, latch() causes segfault!
    //         if (latch().held_by_me())
    //             latch().latch_release();
    //     }
    // }
}

/*
 * Clean up existing transactions at ssm shutdown.
 * -- called from ~ss_m, so this should never be
 * subject to multiple threads using the xct list.
 */
void xct_t::cleanup(bool allow_abort)
{
    bool        changed_list;
    xct_t*      xd;
    W_COERCE(acquire_xlist_mutex());
    do {
        /*
         *  We cannot delete an xct while iterating. Use a loop
         *  to iterate and delete one xct for each iteration.
         */
        xct_i i(false); // do acquire the list mutex. Noone
        // else should be iterating over the xcts at this point.
        changed_list = false;
        xd = i.next();
        if (xd) {
            // Release the mutex so we can delete the xd if need be...
            release_xlist_mutex();
            switch(xd->state()) {
            case xct_active: {
                    smthread_t::attach_xct(xd);
                    if (allow_abort) {
                        W_COERCE( xd->abort() );
                    } else {
                        W_COERCE( xd->dispose() );
                    }
                    delete xd;
                    changed_list = true;
                }
                break;

            case xct_freeing_space:
            case xct_ended: {
                    DBG(<< xd->tid() <<"deleting "
                            << " w/ state=" << xd->state() );
                    delete xd;
                    changed_list = true;
                }
                break;

            default: {
                    DBG(<< xd->tid() <<"skipping "
                            << " w/ state=" << xd->state() );
                }
                break;

            } // switch on xct state
            W_COERCE(acquire_xlist_mutex());
        } // xd not null
    } while (xd && changed_list);

    release_xlist_mutex();
}




/*********************************************************************
 *
 *  xct_t::num_active_xcts()
 *
 *  Return the number of active transactions (equivalent to the
 *  size of _xlist.
 *
 *********************************************************************/
uint32_t
xct_t::num_active_xcts()
{
    uint32_t num;
    W_COERCE(acquire_xlist_mutex());
    num = _xlist.num_members();
    release_xlist_mutex();
    return  num;
}



/*********************************************************************
 *
 *  xct_t::look_up(tid)
 *
 *  Find the record for tid and return it. If not found, return 0.
 *
 *********************************************************************/
xct_t*
xct_t::look_up(const tid_t& tid)
{
    xct_t* xd;
    xct_i iter(true);

    while ((xd = iter.next())) {
        if (xd->tid() == tid) {
            return xd;
        }
    }
    return 0;
}

xct_lock_info_t*
xct_t::lock_info() const {
    return _core->_lock_info;
}

lil_private_table* xct_t::lil_lock_info() const
{
    return _core->_lil_lock_info;
}
RawXct* xct_t::raw_lock_xct() const {
    return _core->_raw_lock_xct;
}

int
xct_t::timeout_c() const {
    return _core->_timeout;
}

/*********************************************************************
 *
 *  xct_t::oldest_tid()
 *
 *  Return the tid of the oldest active xct.
 *
 *********************************************************************/
tid_t
xct_t::oldest_tid()
{
    return _oldest_tid;
}


rc_t
xct_t::abort(bool save_stats_structure /* = false */)
{
    if(is_instrumented() && !save_stats_structure) {
        delete __stats;
        __stats = 0;
    }
    return _abort();
}

void
xct_t::force_nonblocking()
{
//    lock_info()->set_nonblocking();
}

rc_t
xct_t::commit(bool lazy,lsn_t* plastlsn)
{
    // removed because a checkpoint could
    // be going on right now.... see comments
    // in log_prepared and chkpt.cpp

    return _commit(t_normal | (lazy ? t_lazy : t_normal), plastlsn);
}

rc_t
xct_t::commit_as_group_member()
{
    w_assert1(smthread_t::xct() == this);
    return _commit(t_normal|t_group);
}

/* Group commit: static; write the list of xct ids in the single loc record */
rc_t
xct_t::group_commit(const xct_t *list[], int listlen)
{
    // Log the whole bunch.
    Logger::log<xct_end_group_log>(list, listlen);
    return RCOK;
}

rc_t
xct_t::chain(bool lazy)
{
    return _commit(t_chain | (lazy ? t_lazy : t_chain));
}

tid_t
xct_t::youngest_tid()
{
    ASSERT_FITS_IN_LONGLONG(tid_t);
    return _nxt_tid;
}

void
xct_t::update_youngest_tid(const tid_t &t)
{
    if (_nxt_tid < t) _nxt_tid = t;
}


void
xct_t::put_in_order() {
    W_COERCE(acquire_xlist_mutex());
    _xlist.put_in_order(this);
    _oldest_tid = _xlist.last()->_tid;
    release_xlist_mutex();

// TODO(Restart)... enable the checking in retail build, also generate error in retail
//                           this is to prevent missing something in retail and weird error
//                           shows up in retail build much later

// #if W_DEBUG_LEVEL > 2
    W_COERCE(acquire_xlist_mutex());
    {
        // make sure that _xlist is in order
        w_list_i<xct_t, queue_based_lock_t> i(_xlist);
        tid_t t = 0;
        xct_t* xd;
        while ((xd = i.next()))  {
            if (t >= xd->_tid)
                ERROUT(<<"put_in_order: failed to satisfy t < xd->_tid, t: " << t << ", xd->tid: " << xd->_tid);
            w_assert1(t < xd->_tid);
        }
        if (t > _nxt_tid)
            ERROUT(<<"put_in_order: failed to satisfy t <= _nxt_tid, t: " << t << ", _nxt_tid: " << _nxt_tid);
        w_assert1(t <= _nxt_tid);
    }
    release_xlist_mutex();
// #endif
}

void
xct_t::dump(ostream &out)
{
    W_COERCE(acquire_xlist_mutex());
    out << "xct_t: "
            << _xlist.num_members() << " transactions"
        << endl;
    w_list_i<xct_t, queue_based_lock_t> i(_xlist);
    xct_t* xd;
    while ((xd = i.next()))  {
        out << "********************" << "\n";
        out << *xd << endl;
    }
    release_xlist_mutex();
}

void
xct_t::set_timeout(int t)
{
    _core->_timeout = t;
}



/*********************************************************************
 *
 *  Print out tid and status
 *
 *********************************************************************/
ostream&
operator<<(ostream& o, const xct_t& x)
{
    o << "tid="<< x.tid();

    o << "\n" << " state=" << x.state() << "\n" << "   ";

    // o << " defaultTimeout=";
    // print_timeout(o, x.timeout_c());
    o << " first_lsn=" << x._first_lsn << " last_lsn=" << x._last_lsn << "\n" << "   ";

    o << " in_compensated_op=" << x._in_compensated_op << " anchor=" << x._anchor;

    if(x.raw_lock_xct()) {
         x.raw_lock_xct()->dump_lockinfo(o);
    }

    return o;
}

// common code needed by _commit(t_chain) and ~xct_t()
void
xct_t::_teardown(bool is_chaining) {
    W_COERCE(acquire_xlist_mutex());

    _xlink.detach();
    if(is_chaining) {
        _tid = _core->_tid = ++_nxt_tid;
        _core->_lock_info->set_tid(_tid); // WARNING: duplicated in
        // lock_x and in core
        _xlist.put_in_order(this);
    }

    // find the new oldest xct
    xct_t* xd = _xlist.last();
    _oldest_tid = xd ? xd->_tid : _nxt_tid;
    release_xlist_mutex();
}

/*********************************************************************
 *
 *  xct_t::change_state(new_state)
 *
 *  Change the status of the transaction to new_state.
 *
 *********************************************************************/
void
xct_t::change_state(state_t new_state)
{
    // Acquire a write latch, the traditional read latch is used by checkpoint
    w_rc_t latch_rc = latch().latch_acquire(LATCH_EX, timeout_t::WAIT_FOREVER);
    if (latch_rc.is_error())
    {
        // Unable to the read acquire latch, cannot continue, raise an internal error
        DBGOUT2 (<< "Unable to acquire LATCH_EX for transaction object. tid = "
                 << tid() << ", rc = " << latch_rc);
        W_FATAL_MSG(fcINTERNAL, << "unable to write latch a transaction object to change state");
        return;
    }

    w_assert2(_core->_state != new_state);
    w_assert2((new_state > _core->_state) ||
            (_core->_state == xct_chaining && new_state == xct_active));

    // state_t old_state = _core->_state;
    _core->_state = new_state;
    switch(new_state) {
        case xct_aborting: _core->_xct_aborting = true; break;
        // the whole poiint of _xct_aborting is to
        // preserve it through xct_freeing space
        // rather than create two versions of xct_freeing_space, which
        // complicates restart
        case xct_freeing_space: break;
        case xct_ended: break; // arg see comments and logic in xct_t::_abort
        default: _core->_xct_aborting = false; break;
    }

    // Release the write latch
    latch().latch_release();

}


/**\todo Figure out how log space warnings will interact with mtxct */
void
xct_t::log_warn_disable()
{
    _core->_warn_on = true;
}

void
xct_t::log_warn_resume()
{
    _core->_warn_on = false;
}

bool
xct_t::log_warn_is_on() const
{
    return _core->_warn_on;
}

rc_t
xct_t::_pre_commit(uint32_t flags)
{
    // "normal" means individual commit; not group commit.
    // Group commit cannot be lazy or chained.
    bool individual = ! (flags & xct_t::t_group);
    w_assert2(individual || ((flags & xct_t::t_chain) ==0));
    w_assert2(individual || ((flags & xct_t::t_lazy) ==0));

    w_assert1(_core->_state == xct_active);

    w_assert1(_core->_xct_ended++ == 0);

//    W_DO( ConvertAllLoadStoresToRegularStores() );

    change_state(flags & xct_t::t_chain ? xct_chaining : xct_committing);

    if (_last_lsn.valid() || !smlevel_0::log)  {
        /*
         *  If xct generated some log, write a synchronous
         *  Xct End Record.
         *  Do this if logging is turned off. If it's turned off,
         *  we won't have a _last_lsn, but we still have to do
         *  some work here to complete the tx; in particular, we
         *  have to destroy files...
         *
         *  Logging a commit must be serialized with logging
         *  prepares (done by chkpt).
         */

        // Does not wait for the checkpoint to finish, checkpoint is a non-blocking operation
        // chkpt_serial_m::read_acquire();

        // OLD: don't allow a chkpt to occur between changing the
        // state and writing the log record,
        // since otherwise it might try to change the state
        // to the current state (which causes an assertion failure).
        // NEW: had to allow this below, because the freeing of
        // locks needs to happen after the commit log record is written.
        //
        // Note freeing the locks and log flush occur after 'log_xct_end',
        // and then change state.
        // if the logic changes here, need to visit chkpt logic which is
        // depending on the logic here when recording active transactions

        state_t old_state = _core->_state;
        change_state(xct_freeing_space);
        rc_t rc = RCOK;
        if (!is_sys_xct()) { // system transaction has nothing to free, so this log is not needed
            Logger::log<xct_freeing_space_log>();
        }

        // Does not wait for the checkpoint to finish, checkpoint is a non-blocking operation
        // chkpt_serial_m::read_release();

        // CS TODO exceptions!
        // if(rc.is_error()) {
            // Log insert failed.
            // restore the state.
            // Do this by hand; we'll fail the asserts if we
            // use change_state.
            // _core->_state = old_state;
            // return rc;
        // }

        // We should always be able to insert this log
        // record, what with log reservations.
        if(individual && !is_single_log_sys_xct()) { // is commit record fused?
            Logger::log<xct_end_log>();
        }
        // now we have xct_end record though it might not be flushed yet. so,
        // let's do ELR
        W_DO(early_lock_release());
    }

    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::commit(flags)
 *
 *  Commit the transaction. If flag t_lazy, log is not synced.
 *  If flag t_chain, a new transaction is instantiated inside
 *  this one, and inherits all its locks.
 *
 *  In *plastlsn it returns the lsn of the last log record for this
 *  xct.
 *
 *********************************************************************/
rc_t
xct_t::_commit(uint32_t flags, lsn_t* plastlsn /* default NULL*/)
{
    // when chaining, we inherit the read_watermark from the previous xct
    // in case the next transaction are read-only.
    lsn_t inherited_read_watermark;

    // Static thread-local variables used to measure transaction latency
    static thread_local unsigned long _accum_latency = 0;
    static thread_local unsigned int _latency_count = 0;

    W_DO(_pre_commit(flags));

    if (_last_lsn.valid() || !smlevel_0::log)  {
        if (!(flags & xct_t::t_lazy))  {
            _sync_logbuf();
        }
        else { // IP: If lazy, wake up the flusher but do not block
            _sync_logbuf(false, !is_sys_xct()); // if system transaction, don't even wake up flusher
        }

        // IP: Before destroying anything copy last_lsn
        if (plastlsn != NULL) *plastlsn = _last_lsn;

        change_state(xct_ended);

        // Free all locks. Do not free locks if chaining.
        bool individual = ! (flags & xct_t::t_group);
        if(individual && ! (flags & xct_t::t_chain) && _elr_mode != elr_sx)  {
            W_DO(commit_free_locks());
        }

        if(flags & xct_t::t_chain)  {
            // in this case the dependency is the previous xct itself, so take the commit LSN.
            inherited_read_watermark = _last_lsn;
        }
    }  else  {
        W_DO(_commit_read_only(flags, inherited_read_watermark));
    }

    INC_TSTAT(commit_xct_cnt);

    smthread_t::detach_xct(this);        // no transaction for this thread

    /*
     *  Xct is now committed
     */

    if (flags & xct_t::t_chain)  {
        w_assert0(!is_sys_xct()); // system transaction cannot chain (and never has to)

        w_assert1(! (flags & xct_t::t_group));

        ++_xct_chain_len;
        /*
         *  Start a new xct in place
         */
        _teardown(true);
        _first_lsn = _last_lsn = _undo_nxt = lsn_t::null;
        if (inherited_read_watermark.valid()) {
            _read_watermark = inherited_read_watermark;
        }
        // we do NOT reset _read_watermark here. the last xct of the chain
        // is responsible to flush if it's read-only. (if read-write, it anyway flushes)
        _core->_xct_ended = 0;
        w_assert1(_core->_xct_aborting == false);
        // _last_log = 0;

        // should already be out of compensated operation
        w_assert3( _in_compensated_op==0 );

        smthread_t::attach_xct(this);
        INC_TSTAT(begin_xct_cnt);
        _core->_state = xct_chaining; // to allow us to change state back
        // to active: there's an assert about this where we don't
        // have context to know that it's where we're chaining.
        change_state(xct_active);
    } else {
        _xct_chain_len = 0;
    }

    auto end_tstamp = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end_tstamp - _begin_tstamp);
    _accum_latency += elapsed.count();
    _latency_count++;
    // dump average latency every 100 commits
    if (_latency_count % 100 == 0) {
        Logger::log_sys<xct_latency_dump_log>(_accum_latency / _latency_count);
        _accum_latency = 0;
        _latency_count = 0;
    }

    return RCOK;
}

rc_t
xct_t::_commit_read_only(uint32_t flags, lsn_t& inherited_read_watermark)
{
    // Nothing logged; no need to write a log record.
    change_state(xct_ended);

    bool individual = ! (flags & xct_t::t_group);
    if(individual && !is_sys_xct() && ! (flags & xct_t::t_chain)) {
        W_DO(commit_free_locks());

        // however, to make sure the ELR for X-lock and CLV is
        // okay (ELR for S-lock is anyway okay) we need to make
        // sure this read-only xct (no-log=read-only) didn't read
        // anything not yet durable. Thus,
        if ((_elr_mode==elr_sx || _elr_mode==elr_clv) &&
                _query_concurrency != t_cc_none && _query_concurrency != t_cc_bad && _read_watermark.valid()) {
            // to avoid infinite sleep because of dirty pages changed by aborted xct,
            // we really output a log and flush it
            bool flushed = false;
            timeval start, now, result;
            ::gettimeofday(&start,NULL);
            while (true) {
                W_DO(log->flush(_read_watermark, false, true, &flushed));
                if (flushed) {
                    break;
                }

                // in some OS, usleep() has a very low accuracy.
                // So, we check the current time rather than assuming
                // elapsed time = ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC.
                ::gettimeofday(&now,NULL);
                timersub(&now, &start, &result);
                int elapsed = (result.tv_sec * 1000000 + result.tv_usec);
                if (elapsed > ELR_READONLY_WAIT_MAX_COUNT * ELR_READONLY_WAIT_USEC) {
#if W_DEBUG_LEVEL>0
                    // this is NOT an error. it's fine.
                    cout << "ELR timeout happened in readonly xct's watermark check. outputting xct_end log..." << endl;
#endif // W_DEBUG_LEVEL>0
                    break; // timeout
                }
                ::usleep(ELR_READONLY_WAIT_USEC);
            }

            if (!flushed) {
                // now we suspect that we might saw a bogus tag for some reason.
                // so, let's output a real xct_end log and flush it.
                // See jira ticket:99 "ELR for X-lock" (originally trac ticket:101).
                // NOTE this should not be needed now that our algorithm is based
                // on lock bucket tag, which is always exact, not too conservative.
                // should consider removing this later, but for now keep it.
                //W_COERCE(log_xct_end());
                //_sync_logbuf();
                W_FATAL_MSG(fcINTERNAL,
                        << "Reached part of the code that was supposed to be dead."
                        << " Please uncomment the lines and remove this error");
            }
            _read_watermark = lsn_t::null;
        }
    } else {
        if(flags & xct_t::t_chain)  {
            inherited_read_watermark = _read_watermark;
        }
        // even if chaining or grouped xct, we can do ELR
        W_DO(early_lock_release());
    }

    return RCOK;
}

rc_t
xct_t::commit_free_locks(bool read_lock_only, lsn_t commit_lsn)
{
    // system transaction doesn't acquire locks
    if (!is_sys_xct()) {
        W_COERCE( lm->unlock_duration(read_lock_only, commit_lsn) );
    }
    return RCOK;
}

rc_t xct_t::early_lock_release() {
    if (!_sys_xct) { // system transaction anyway doesn't have locks
        switch (_elr_mode) {
            case elr_none: break;
            case elr_s:
                // release only S and U locks
                W_DO(commit_free_locks(true));
                break;
            case elr_sx:
            case elr_clv: // TODO see below
                // simply release all locks
                // update tag for safe SX-ELR with _last_lsn which should be the commit lsn
                // (we should have called log_xct_end right before this)
                W_DO(commit_free_locks(false, _last_lsn));
                break;
                // TODO Controlled Lock Violation is tentatively replaced with SX-ELR.
                // In RAW-style lock manager, reading the permitted LSN needs another barrier.
                // In reality (not concept but dirty impl), they are doing the same anyways.
                /*
            case elr_clv:
                // release no locks, but give permission to violate ours:
                lm->give_permission_to_violate(_last_lsn);
                break;
                */
            default:
                w_assert1(false); // wtf??
        }
    }
    return RCOK;
}


rc_t
xct_t::_pre_abort()
{
    // The transaction abort function is shared by :
    // 1. Normal transaction abort, in such case the state would be in xct_active,
    //     xct_committing, or xct_freeing_space, and the _loser_xct flag off
    // 2. UNDO phase in Recovery, in such case the state would be in xct_active
    //     but the _loser_xct flag is on to indicating a loser transaction
    // Note that if we open the store for new transaction during Recovery
    // we could encounter normal transaction abort while Recovery is going on,
    // in such case the aborting transaction state would fall into case #1 above

    if (!is_loser_xct()) {
        // Not a loser txn
        w_assert1(_core->_state == xct_active
                || _core->_state == xct_committing /* if it got an error in commit*/
                || _core->_state == xct_freeing_space /* if it got an error in commit*/
                );
        if(_core->_state != xct_committing && _core->_state != xct_freeing_space) {
            w_assert1(_core->_xct_ended++ == 0);
        }
    }

    if (true == is_loser_xct())
    {
        // Loser transaction rolling back, set the flag to indicate the status
        set_loser_xct_in_undo();
    }

#if X_LOG_COMMENT_ON
    // Do this BEFORE changing state so that we
    // have, for log-space-reservations purposes,
    // ensured that we inserted a record during
    // forward processing, thereby reserving something
    // for aborting, even if this is a read-only xct.
    {
        // w_ostrstream s;
        // s << "aborting... ";
        // TODO, bug... commenting this out, it appears
        // Single Page Recovery when collecting log records
        // it might have bugs dealing with this log record (before txn aborting)
//        W_DO(log_comment(s.c_str()));
    }
#endif

    change_state(xct_aborting);

    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::abort()
 *
 *  Abort the transaction by calling rollback().
 *
 *********************************************************************/
rc_t
xct_t::_abort()
{
    W_DO(_pre_abort());

    /*
     * clear the list of load stores as they are going to be destroyed
     */
    //ClearAllLoadStores();

    W_DO( rollback(lsn_t::null) );

    // if this is not part of chain or both-SX-ELR mode,
    // we can safely release all locks at this point.
    bool all_lock_released = false;
    if (_xct_chain_len == 0 || _elr_mode == elr_sx) {
        W_COERCE( commit_free_locks());
        all_lock_released = true;
    } else {
        // if it's a part of chain, we have to make preceding
        // xcts durable. so, unless it's SX-ELR, we can release only S-locks
        W_COERCE( commit_free_locks(true));
    }

    if (_last_lsn.valid()) {
        LOGREC_ACCOUNT_END_XCT(true); // see logrec.h
        /*
         *  If xct generated some log, write a Xct End Record.
         *  We flush because if this was a prepared
         *  transaction, it really must be synchronous
         */

        // don't allow a chkpt to occur between changing the state and writing
        // the log record, since otherwise it might try to change the state
        // to the current state (which causes an assertion failure).

        // NOTE: you cannot insert a log comment here; it'll break
        // on an assertion having to do with the xct state. Wait until
        // state is changed from aborting to something else.

        // Does not wait for the checkpoint to finish, checkpoint is a non-blocking operation
        // chkpt_serial_m::read_acquire();

        change_state(xct_freeing_space);
        Logger::log<xct_freeing_space_log>();

        // Does not wait for the checkpoint to finish, checkpoint is a non-blocking operation
        // chkpt_serial_m::read_release();

        if (_xct_chain_len > 0) {
            // we need to flush only if it's chained or prepared xct
            _sync_logbuf();
        } else {
            // otherwise, we don't have to flush
            _sync_logbuf(false);
        }

        // don't allow a chkpt to occur between changing the state and writing
        // the log record, since otherwise it might try to change the state
        // to the current state (which causes an assertion failure).

        // Does not wait for the checkpoint to finish, checkpoint is a non-blocking operation
        // chkpt_serial_m::read_acquire();

        // Log transaction abort for both cases: 1) normal abort, 2) UNDO
        change_state(xct_ended);
        Logger::log<xct_abort_log>();

        // Does not wait for the checkpoint to finish, checkpoint is a non-blocking operation
        // chkpt_serial_m::read_release();
    }  else  {
        change_state(xct_ended);
    }

    if (!all_lock_released) {
        W_COERCE( commit_free_locks());
    }

    _core->_xct_aborting = false; // couldn't have xct_ended do this, arg
                                  // CS: why not???
    _xct_chain_len = 0;

    smthread_t::detach_xct(this);        // no transaction for this thread
    INC_TSTAT(abort_xct_cnt);
    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::save_point(lsn)
 *
 *  Generate and return a save point in "lsn".
 *
 *********************************************************************/
rc_t
xct_t::save_point(lsn_t& lsn)
{
    lsn = _last_lsn;
    return RCOK;
}


/*********************************************************************
 *
 *  xct_t::dispose()
 *
 *  Make the transaction disappear.
 *  This is only for simulating crashes.  It violates
 *  all tx semantics.
 *
 *********************************************************************/
rc_t
xct_t::dispose()
{
    delete __stats;
    __stats = 0;

    W_COERCE( commit_free_locks());
    // ClearAllStoresToFree();
    // ClearAllLoadStores();
    _core->_state = xct_ended; // unclean!
    smthread_t::detach_xct(this);
    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::_sync_logbuf()
 *
 *  Force log entries up to the most recently written to disk.
 *
 *  block: If not set it does not block, but kicks the flusher. The
 *         default is to block, the no block option is used by AsynchCommit
 * signal: Whether we even fire the log buffer
 *********************************************************************/
w_rc_t
xct_t::_sync_logbuf(bool block, bool signal)
{
    if(log) {
        INC_TSTAT(xct_log_flush);
        return log->flush(_last_lsn,block,signal);
    }
    return RCOK;
}

// rc_t xct_t::get_logbuf(logrec_t*& ret)
// {
//     // then , use tentative log buffer.
//     // CS: system transactions should also go through log reservation,
//     // since they are consuming space which user transactions think
//     // is available for rollback. This is probably a bug.
//     if (is_piggy_backed_single_log_sys_xct()) {
//         ret = _log_buf_for_piggybacked_ssx;
//         return RCOK;
//     }


//     // Instead of flushing here, we'll flush at the end of give_logbuf()
//     // and assert here that we've got nothing buffered:
//     w_assert1(!_last_log);
//     ret = _last_log = _log_buf;

//     return RCOK;
// }

rc_t xct_t::update_last_logrec(logrec_t* l, lsn_t lsn)
{
    // _last_log = 0;
    _last_lsn = lsn;

    LOGREC_ACCOUNT(*l, !consuming); // see logrec.h

    // log insert effectively set_lsn to the lsn of the *next* byte of
    // the log.
    if ( ! _first_lsn.valid())  _first_lsn = _last_lsn;

    if (!l->is_single_sys_xct()) {
        _undo_nxt = (l->is_cpsn() ? l->undo_nxt() : _last_lsn);
    }

    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::release_anchor(and_compensate)
 *
 *  stop critical sections vis-a-vis compensated operations
 *  If and_compensate==true, it makes the _last_log a clr
 *
 *********************************************************************/
void
xct_t::release_anchor( bool and_compensate ADD_LOG_COMMENT_SIG )
{

#if X_LOG_COMMENT_ON
    if(and_compensate) {
        // w_ostrstream s;
        // s << "release_anchor at "
        //     << debugmsg;
        // Logger::log<comment_log>(s.c_str());
    }
#endif
    DBGX(
            << " RELEASE ANCHOR "
            << " in compensated op==" << _in_compensated_op
    );

    w_assert3(_in_compensated_op>0);

    if(_in_compensated_op == 1) { // will soon be 0

        // NB: this whole section could be made a bit
        // more efficient in the -UDEBUG case, but for
        // now, let's keep in all the checks

        // don't flush unless we have popped back
        // to the last compensate() of the bunch

        // Now see if this last item was supposed to be
        // compensated:
        if(and_compensate && (_anchor != lsn_t::null)) {
           // if(_last_log) {
           //     if ( _last_log->is_cpsn()) {
           //          DBGX(<<"already compensated");
           //          w_assert3(_anchor == _last_log->undo_nxt());
           //     } else {
           //         DBGX(<<"SETTING anchor:" << _anchor);
           //         w_assert3(_anchor <= _last_lsn);
           //         _last_log->set_clr(_anchor);
           //     }
           // } else {
               DBGX(<<"no _last_log:" << _anchor);
               /* Can we update the log record in the log buffer ? */
               if( log &&
                   !log->compensate(_last_lsn, _anchor).is_error()) {
                   // Yup.
                    INC_TSTAT(compensate_in_log);
               } else {
                   // Nope, write a compensation log record.
                   // Really, we should return an rc from this
                   // method so we can W_DO here, and we should
                   // check for eBADCOMPENSATION here and
                   // return all other errors  from the
                   // above log->compensate(...)

                   Logger::log<compensate_log>(_anchor);
                   INC_TSTAT(compensate_records);
               }
            // }
        }

        _anchor = lsn_t::null;

    }
    // UN-PROTECT
    _in_compensated_op -- ;

    DBGX(
        << " out compensated op=" << _in_compensated_op
    );
}

/*********************************************************************
 *
 *  xct_t::anchor( bool grabit )
 *
 *  Return a log anchor (begin a top level action).
 *
 *  If argument==true (most of the time), it stores
 *  the anchor for use with compensations.
 *
 *  When the  argument==false, this is used (by I/O monitor) not
 *  for compensations, but only for concurrency control.
 *
 *********************************************************************/
const lsn_t&
xct_t::anchor(bool grabit)
{
    // PROTECT
    _in_compensated_op ++;

    INC_TSTAT(anchors);
    DBGX(
            << " GRAB ANCHOR "
            << " in compensated op==" << _in_compensated_op
    );


    if(_in_compensated_op == 1 && grabit) {
        // _anchor is set to null when _in_compensated_op goes to 0
        w_assert3(_anchor == lsn_t::null);
        _anchor = _last_lsn;
        DBGX(    << " anchor =" << _anchor);
    }
    DBGX(    << " anchor returns " << _last_lsn );

    return _last_lsn;
}


/*********************************************************************
 *
 *  xct_t::compensate_undo(lsn)
 *
 *  compensation during undo is handled slightly differently--
 *  the gist of it is the same, but the assertions differ, and
 *  we have to acquire the mutex first
 *********************************************************************/
void
xct_t::compensate_undo(const lsn_t& lsn)
{
    DBGX(    << " compensate_undo (" << lsn << ") -- state=" << state());

    w_assert3(_in_compensated_op);
    // w_assert9(state() == xct_aborting); it's active if in sm::rollback_work

    // _compensate(lsn, _last_log?_last_log->is_undoable_clr() : false);
    _compensate(lsn, false);
}

/*********************************************************************
 *
 *  xct_t::compensate(lsn, bool undoable)
 *
 *  Generate a compensation log record to compensate actions
 *  started at "lsn" (commit a top level action).
 *  Generates a new log record only if it has to do so.
 *
 *********************************************************************/
void
xct_t::compensate(const lsn_t& lsn, bool undoable ADD_LOG_COMMENT_SIG)
{
    DBGX(    << " compensate(" << lsn << ") -- state=" << state());

    _compensate(lsn, undoable);

    release_anchor(true ADD_LOG_COMMENT_USE);
}

/*********************************************************************
 *
 *  xct_t::_compensate(lsn, bool undoable)
 *
 *
 *  Generate a compensation log record to compensate actions
 *  started at "lsn" (commit a top level action).
 *  Generates a new log record only if it has to do so.
 *
 *  Special case of undoable compensation records is handled by the
 *  boolean argument. (NOT USED FOR NOW -- undoable_clrs were removed
 *  in 1997 b/c they weren't needed anymore;they were originally
 *  in place for an old implementation of extent-allocation. That's
 *  since been replaced by the dealaying of store deletion until end
 *  of xct).  The calls to the methods and infrastructure regarding
 *  undoable clrs was left in place in case it must be resurrected again.
 *  The reason it was removed is that there was some complexity involved
 *  in hanging onto the last log record *in the xct* in order to be
 *  sure that the compensation happens *in the correct log record*
 *  (because an undoable compensation means the log record holding the
 *  compensation isn't being compensated around, whereas turning any
 *  other record into a clr or inserting a stand-alone clr means the
 *  last log record inserted is skipped on undo).
 *  That complexity remains, since log records are flushed to the log
 *  immediately now (which was precluded for undoable_clrs ).
 *
 *********************************************************************/
void
xct_t::_compensate(const lsn_t& lsn, bool undoable)
{
    DBGX(    << "_compensate(" << lsn << ") -- state=" << state());

    bool done = false;
    // if ( _last_log ) {
    //     // We still have the log record here, and
    //     // we can compensate it.
    //     // NOTE: we used to use this a lot but now the only
    //     // time this is possible (due to the fact that we flush
    //     // right at insert) is when the logging code is hand-written,
    //     // rather than Perl-generated.

    //     /*
    //      * lsn is got from anchor(), and anchor() returns _last_lsn.
    //      * _last_lsn is the lsn of the last log record
    //      * inserted into the log, and, since
    //      * this log record hasn't been inserted yet, this
    //      * function can't make a log record compensate to itself.
    //      */
    //     w_assert3(lsn <= _last_lsn);
    //     _last_log->set_clr(lsn);
    //     INC_TSTAT(compensate_in_xct);
    //     done = true;
    // } else {
        /*
        // Log record has already been inserted into the buffer.
        // Perhaps we can update the log record in the log buffer.
        // However,  it's conceivable that nothing's been written
        // since _last_lsn, and we could be trying to compensate
        // around nothing.  This indicates an error in the calling
        // code.
        */
        if( lsn >= _last_lsn) {
            INC_TSTAT(compensate_skipped);
        }
        if( log && (! undoable) && (lsn < _last_lsn)) {
            if(!log->compensate(_last_lsn, lsn).is_error()) {
                INC_TSTAT(compensate_in_log);
                done = true;
            }
        }
    // }

    if( !done && (lsn < _last_lsn) ) {
        /*
        // If we've actually written some log records since
        // this anchor (lsn) was grabbed,
        // force it to write a compensation-only record
        // either because there's no record on which to
        // piggy-back the compensation, or because the record
        // that's there is an undoable/compensation and will be
        // undone (and we *really* want to compensate around it)
        */

        Logger::log<compensate_log>(lsn);
        INC_TSTAT(compensate_records);
    }
}

/*********************************************************************
 *
 *  xct_t::rollback(savept)
 *
 *  Rollback transaction up to "savept".
 *
 *********************************************************************/
rc_t
xct_t::rollback(const lsn_t &save_pt)
{
    DBGTHRD(<< "xct_t::rollback to " << save_pt);

    if(!log) {
        cerr
        << "Cannot roll back with logging turned off. "
        << endl;
        return RC(eNOABORT);
    }

    w_rc_t            rc;

    if(_in_compensated_op > 0) {
        w_assert3(save_pt >= _anchor);
    } else {
        w_assert3(_anchor == lsn_t::null);
    }

    DBGX( << " in compensated op depth " <<  _in_compensated_op
            << " save_pt " << save_pt << " anchor " << _anchor);
    _in_compensated_op++;

    // rollback is only one type of compensated op, and it doesn't nest
    w_assert0(!_rolling_back);
    _rolling_back = true;

    // undo_nxt is the lsn of last recovery log for this txn
    lsn_t nxt = _undo_nxt;
    W_DO(log->flush(nxt));

    DBGOUT3(<<"Initial rollback, from: " << nxt << " to: " << save_pt);

    logrec_t* lrbuf = new logrec_t;

    while (save_pt < nxt)
    {
        rc =  log->fetch(nxt, lrbuf, 0, true);
        if(rc.is_error() && rc.err_num()==eEOF)
        {
            DBGX(<< " fetch returns EOF" );
            goto done;
        }
        w_assert3(!lrbuf->is_skip());
        logrec_t& r = *lrbuf;

        DBGOUT1(<<"Rollback, current undo lsn: " << nxt);

        if (r.is_undo())
        {
            w_assert0(!r.is_cpsn());
           w_assert1(nxt == r.lsn_ck());
            // r is undoable
            w_assert1(!r.is_single_sys_xct());
            w_assert1(!r.is_multi_page()); // All multi-page logs are SSX, so no UNDO.
            /*
             *  Undo action of r.
             */

            fixable_page_h page;

            r.undo(page.is_fixed() ? &page : 0);

            // Not a compensation log record, use xid_prev() which is
            // previous logrec of this xct
            nxt = r.xid_prev();
            DBGOUT1(<<"Rollback, log record is not compensation, xid_prev: " << nxt);
        }
        else  if (r.is_cpsn())
        {
            if (r.is_single_sys_xct())
            {
                nxt = lsn_t::null;
            }
            else
            {
                nxt = r.undo_nxt();
            }
            // r.xid_prev() could just as well be null

        }
        else
        {
            // r is not undoable
            if (r.is_single_sys_xct())
            {
                nxt = lsn_t::null;
            }
            else
            {
                nxt = r.xid_prev();
            }
            // w_assert9(r.undo_nxt() == lsn_t::null);
        }
    }

    delete lrbuf;

    _undo_nxt = nxt;
    _read_watermark = lsn_t::null;
    _xct_chain_len = 0;

done:

    DBGX( << "leaving rollback: compensated op " << _in_compensated_op);
    _in_compensated_op --;
    _rolling_back = false;
    w_assert3(_anchor == lsn_t::null ||
                _anchor == save_pt);

    if(save_pt != lsn_t::null) {
        INC_TSTAT(rollback_savept_cnt);
    }

    DBGTHRD(<< "xct_t::rollback done to " << save_pt);
    return rc;
}

ostream &
xct_t::dump_locks(ostream &out) const
{
    raw_lock_xct()->dump_lockinfo(out);
    return out;
}

sys_xct_section_t::sys_xct_section_t(bool single_log_sys_xct)
{
    _original_xct_depth = smthread_t::get_tcb_depth();
    xct_t::begin(true, single_log_sys_xct);
}
sys_xct_section_t::~sys_xct_section_t()
{
    size_t xct_depth = smthread_t::get_tcb_depth();
    if (xct_depth > _original_xct_depth) {
        W_COERCE(ss_m::abort_xct());
    }
}
rc_t sys_xct_section_t::end_sys_xct (rc_t result)
{
    if (result.is_error()) {
        W_DO (ss_m::abort_xct());
    } else {
        W_DO (ss_m::commit_sys_xct());
    }
    return RCOK;
}
