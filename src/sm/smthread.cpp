/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define SMTHREAD_C

#include <sm_base.h>

#include <w_strstream.h>

SmthreadFunc::~SmthreadFunc()
{
}

/*
 * Thread map that will contain OR of all the smthread thread maps.
 * This is so that we can determine if we have got too dense and have
 * to change the number of bits in the map and recompile.
 */

static atomic_thread_map_t all_fingerprints;

// number of threads that are assigned fingerprints.
// used for a trivial optimization to assign better fingerprints
int s_num_assigned_threads = 0;
queue_based_lock_t s_num_assigned_threads_lock; // to protect s_num_assigned_threads

extern "C"
void clear_all_fingerprints()
{
    // called from smsh because smsh has points at which
    // this is safe to do, and because it forks off so many threads;
    // it can't really create a pool of smthreads.
    smthread_t::init_fingerprint_map() ;
}

/**\brief Called on tcb_t constructor.
 */
void
smthread_t::tcb_t::create_TL_stats() {
    _TL_stats = new sm_stats_info_t;
    // These things have no constructor
    clear_TL_stats();
}

/**\brief Called on tcb_t destructor.
 */
void
smthread_t::tcb_t::destroy_TL_stats() {
    if(_TL_stats) {
        if (smlevel_0::statistics_enabled) {
            // Global stats are protected by a mutex
            smlevel_0::add_to_global_stats(TL_stats()); // before detaching them
        }
        delete _TL_stats;
        _TL_stats = NULL;
    }
}

static smthread_init_t smthread_init;

int smthread_init_t::count = 0;
/*
 *  smthread_init_t::smthread_init_t()
 */
smthread_init_t::smthread_init_t()
{
}



/*
 *        smthread_init_t::~smthread_init_t()
 */
smthread_init_t::~smthread_init_t()
{
}

/*********************************************************************
 *
 *  Constructor and destructor for smthread_t::tcb_t
 *
 *********************************************************************/

void
smthread_t::tcb_t::clear_TL_stats()
{
    // Global stats are protected by a mutex
    smlevel_0::add_to_global_stats(TL_stats()); // before clearing them
    memset(&TL_stats(),0, sizeof(sm_stats_info_t));
}

/* Non-thread-safe add from the per-thread copy to another struct.
 * The caller must ensure thread-safety.
 * As it turns out, this gets called only from ss_m::gather_stats, which
 * assumes that the sm_stats_info_t structure passed in is local or
 * proteded by the vas, so the safety of argument w is ok, but
 * the thread_local stats might be being updated while this is
 * going on.
 */
void
smthread_t::add_from_TL_stats(sm_stats_info_t &w) const
{
    const sm_stats_info_t &x = tcb().TL_stats_const();
    w += x;

    // pick these up from the sthread_t stats structure:
    w.sm.rwlock_r_waits += this->SthreadStats.rwlock_r_wait;
    w.sm.rwlock_w_waits += this->SthreadStats.rwlock_w_wait;

    w.sm.need_latch_condl += this->SthreadStats.needs_latch_condl;
    w.sm.latch_condl_nowaits += this->SthreadStats.latch_condl_nowait;
    w.sm.need_latch_uncondl += this->SthreadStats.needs_latch_uncondl;
    w.sm.latch_uncondl_nowaits += this->SthreadStats.latch_uncondl_nowait;
}

/*********************************************************************
 *
 *  smthread_t::smthread_t
 *
 *  Create an smthread_t.
 *
 *********************************************************************/
smthread_t::smthread_t(
    st_proc_t* f,
    void* arg,
    priority_t priority,
    const char* name,
    timeout_in_ms lockto,
    unsigned stack_size)
: sthread_t(priority, name, stack_size),
  _proc(f),
  _arg(arg),
  _gen_log_warnings(true)
{
    tcb_t *empty_tcb = new tcb_t(NULL);
    w_assert1 (empty_tcb != NULL);
    _tcb_tail = empty_tcb;

    // For now, user pointing to this object
    // indicates that the sthread is an smthread. Grot.
    user = (void *)&smthread_init;

    lock_timeout(lockto);
    if(lockto > WAIT_NOT_USED) _initialize_fingerprint();
}

// Used by internal sm threads, e.g., bf_prefetch_thread.
// Uses run() method instead of a method given as argument.
// Does NOT acquire a fingerprint so it cannot acquire locks.
smthread_t::smthread_t(
    priority_t priority,
    const char* name,
    timeout_in_ms lockto,
    unsigned stack_size
    )
: sthread_t(priority, name, stack_size),
  _proc(0),
  _arg(0),
  _gen_log_warnings(true)
{
    tcb_t *empty_tcb = new tcb_t(NULL);
    w_assert1 (empty_tcb != NULL);
    _tcb_tail = empty_tcb;

    // For now, user pointing to this object
    // indicates that the sthread is an smthread. Grot.
    user =(void *) &smthread_init;
    lock_timeout(lockto);
    if(lockto > WAIT_NOT_USED) _initialize_fingerprint();
}

void smthread_t::_initialize_fingerprint()
{
// We can see if we might be getting false positives here.
// If we make the finger print maps unique, we can eliminate that
// possibility.
#define DEBUG_FINGERPRINTS 0
#if DEBUG_FINGERPRINTS
    int tries=0;
    const int trylimit = 50;
    bool bad=true;
    while ( (bad = _try_initialize_fingerprint()) )
    {
        _uninitialize_fingerprint();
        if(++tries > trylimit) {
            fprintf(stderr,
    "Could not make non-overlapping fingerprint after %d tries; %d out of %d bits are inuse\n",
            tries, all_fingerprints.num_bits_set(), all_fingerprints.num_bits());
            // note: there's a race here but if servers are
            // creating a pool of threads at start-up, this
            // is still useful info:
            if(all_fingerprints.is_full()) {
                fprintf(stderr,
            "collective thread map is full: increase #bits an recompile.\n");
            }
            W_FATAL(eTHREADMAPFULL);
        }
    }
#else
    (void) _try_initialize_fingerprint();
#endif
}


bool smthread_t::_try_initialize_fingerprint()
{
    int copied_num_assigned_threads;
    {
        CRITICAL_SECTION(cs, s_num_assigned_threads_lock);
        copied_num_assigned_threads = s_num_assigned_threads;
        ++s_num_assigned_threads;
    }

    if (copied_num_assigned_threads < SM_DREADLOCK_BITCOUNT / FINGER_BITS) {
        // if s_num_assigned_threads is enough large, just assign a sequence
        for(int i = 0; i < FINGER_BITS; ++i) {
            _fingerprint_map.set_bit(FINGER_BITS * copied_num_assigned_threads + i);
        }
    } else {
        /*
        Initialize the random fingerprint for this lock_info.
        It consists of FINGER_BITS selected uniformly without
        replacement from the possible bits in the thread_map.
        */
        for( int i=0; i < FINGER_BITS; i++) {
        retry:
            int rval = me()->randn(atomic_thread_map_t::BITS);
            for(int j=0; j < i; j++) {
                if(rval == _fingerprint[j])
                    goto retry;
            }
            _fingerprint[i] = rval;
        }

        // Initialize this thread's _fingerprint_map
        for(int i=0; i < FINGER_BITS; i++) {
            _fingerprint_map.set_bit(_fingerprint[i]);
        }
    }

#ifndef PROHIBIT_FALSE_POSITIVES
    return false;
#else
    // This uniqueness check is left in for possible turning on
    // when debugging deadlocks; it is so that we can tell if
    // we are getting duplicated bits and thus possibly false-positives.
    // As long as we are running tests
    // that pass this check, we know that we don't have false-positives.
    // To turn it on, define PROHIBIT_FALSE_POSITIVES above.

    /* Note also that the global map is unable to recycle
       fingerprints, putting a hard limit on the number of threads the
       system can ever spawn when using this restrictive check.
     */

    all_fingerprints.lock_for_write();
    atomic_thread_map_t _tmp;
    bool first_time = all_fingerprints.is_empty();

    int  matches = _fingerprint_map.words_overlap(_tmp, all_fingerprints);
    bool nonunique = (matches == _fingerprint_map.num_words());
    bool failure = (nonunique && !first_time);

    if(!failure) {
        all_fingerprints.copy(_tmp);
    }
    all_fingerprints.unlock_writer();

    if(failure) {
        // INC_TSTAT(nonunique_fingerprints);
        tcb()._TL_stats->sm.nonunique_fingerprints++;
        // fprintf(stderr,
        // "Phooey! overlapping fingerprint map : %d bits used\n",
        // all_fingerprints.num_bits_set());
    } else {
        // INC_TSTAT(unique_fingerprints);
        tcb()._TL_stats->sm.unique_fingerprints++;
    }

#define DEBUG_DEADLOCK 0
#if DEBUG_DEADLOCK
    {
        short a=_fingerprint[0];
        short b=_fingerprint[1];
        short c=_fingerprint[2];

        w_ostrstream s;

        s << "all_fingerprints " ;
        all_fingerprints.print(s);
        s << endl;

        s << "num_bits_set " << all_fingerprints.num_bits_set()  << endl;

        if(all_fingerprints.is_full()) {
            s << " FULL! "  << endl;
        }
        s
            << "matches=" << matches
            << " num_words()=" << all_fingerprints.num_words()
            << " nonunique=" << nonunique
            << " first_time=" << first_time
        << " failure="  << failure << endl;

        s << "_fingerprint_map " ;
        _fingerprint_map.print(s) ;
        s << endl;

        fprintf(stderr,
        "%s ------ fingerprint %d.%d.%d\n", s.c_str(), a,b,c);
    }
#endif

    return failure;
#endif
}

// called from constructor
void smthread_t::init_fingerprint_map()
{
    //all_fingerprints.lock_for_write();
    all_fingerprints.clear();
    //all_fingerprints.unlock_writer();
    {
        //CRITICAL_SECTION(cs, s_num_assigned_threads_lock);
        //s_num_assigned_threads = 0;
    }
}
void smthread_t::_uninitialize_fingerprint()
{
    _fingerprint_map.clear();
}

/*********************************************************************
 *
 *  smthread_t::join()
 *
 *  invoke sthread_t::join if it's safe to do so
 *
 *********************************************************************/
w_rc_t
smthread_t::join(timeout_in_ms timeout)
{
    w_rc_t rc = this->sthread_t::join(timeout);

    if(tcb().xct != NULL) {
        return RC(eINTRANS);
    }
    if(tcb().pin_count != 0)
    {
        return RC(ePINACTIVE);
    }
    if( tcb()._xct_log != NULL ) {
        fprintf(stderr, "non-null _xct_log on join\n");
        return RC(eINTRANS);
    }

    return rc;
}
/*********************************************************************
 *
 *  smthread_t::~smthread_t()
 *
 *  Destroy smthread. Thread is already defunct the object is
 *  destroyed.
 *
 *********************************************************************/
smthread_t::~smthread_t()
{
    user = NULL;

    if(lock_timeout() > WAIT_NOT_USED) {
        _uninitialize_fingerprint();
    }

    // revoke transaction objects
    w_assert2( get_tcb_depth() == 1); // otherwise some transaction is running!
    while (_tcb_tail) {
        // this should be the empty tcb_t as dummy!
        w_assert2( _tcb_tail->xct == NULL);
        w_assert2( _tcb_tail->pin_count == 0);
        w_assert2( _tcb_tail->_xct_log == 0 );
        tcb_t* old = _tcb_tail;
        _tcb_tail = _tcb_tail->_outer;
        delete old;
    }
}

// There's something to be said for having the smthread_unblock
// unblock only those threads that blocked with smthread_block.
// This is to deal with races in the deadlock detection.
// It's possible that a thread that blocked this way will be awakened
// by another force such as timeout, but we need to be sure that we don't
// try here to unblock a thread that didn't block via smthread_block
w_error_codes  smthread_t::_smthread_block(
      timeout_in_ms timeout,
      const char * const W_IFDEBUG9(blockname))
{
    _waiting = true;
    // rval is set by the unblocker
    w_error_codes rval = sthread_t::block(timeout);
    _waiting = false;
    return rval;
}

w_rc_t    smthread_t::_smthread_unblock(w_error_codes e)
{
    // We should never be unblocking ourselves.
    w_assert1(me() != this);

    // tried to unblock the wrong thread
    if(!_waiting) {
        return RC(eNOTBLOCKED);
    }

    return  this->sthread_t::unblock(e); // should return RCOK to the caller
}

/* thread-compatability block() and unblock.  Use the per-smthread _block
   as the synchronization primitive. */
w_error_codes   smthread_t::smthread_block(timeout_in_ms timeout,
      const char * const caller,
      const void *)
{
    return _smthread_block(timeout, caller);
}

w_rc_t   smthread_t::smthread_unblock(w_error_codes e)
{
    return _smthread_unblock(e);
}


void smthread_t::before_run() {
    sthread_t::before_run();
    latch_t::on_thread_init(this); // called after constructor
}
void smthread_t::after_run() { // called before destructor
    latch_t::on_thread_destroy(this);
    sthread_t::after_run();
}

smthread_t*
smthread_t::dynamic_cast_to_smthread()
{
    if(user == (void *)&smthread_init) return this;
    return NULL;
}


const smthread_t*
smthread_t::dynamic_cast_to_const_smthread() const
{
    if(user == (void *)&smthread_init) return this;
    return NULL;
}


class SelectSmthreadsFunc : public ThreadFunc
{
    public:
    SelectSmthreadsFunc(SmthreadFunc& func) : f(func) {};
    void operator()(const sthread_t& thread) {
        if (const smthread_t* smthread = thread.dynamic_cast_to_const_smthread())
        {
            f(*smthread);
        }
    }
    private:
    SmthreadFunc&    f;
};

void
smthread_t::for_each_smthread(SmthreadFunc& f)
{

    SelectSmthreadsFunc g(f);
    for_each_thread(g);
}


void
smthread_t::attach_xct(xct_t* x)
{
    w_assert0(get_tcb_depth() == 1 || x->is_sys_xct()); // only system transactions can be nested
    // add the given transaction as top (outmost) transaction
    tcb_t *new_outmost = new tcb_t(_tcb_tail);
    w_assert0(new_outmost != NULL);
    _tcb_tail = new_outmost;
    new_outmost->xct = x;

    w_assert0(me()->xct() != NULL);
    w_assert0(me()->xct() == x);

    x->attach_thread();
    // descends to xct_impl::attach_thread()
    // which grabs the 1thread mutex, calls new_xct, releases the mutex.
}


void
smthread_t::detach_xct(xct_t* x)
{
    // removes the top (outmost) transaction from this thread
    if (x != _tcb_tail->xct) {
        // are you removing something else??
        W_FATAL(eNOTRANS);
    }

    // descends to xct_impl::detach_thread()
    // which grabs the 1thread mutex, calls no_xct, releases the mutex.
    x->detach_thread();

    // pop the outmost tcb_t after the above which cleans up some property of tcb_t
    tcb_t *outmost = _tcb_tail;
    _tcb_tail = _tcb_tail->_outer;
    delete outmost;
    w_assert0(get_tcb_depth() >= 1);
}

/*
 * We're attaching x to this thread
 */
void
smthread_t::new_xct(xct_t *x)
{
    w_assert1(x);

    /* Get the three caches. If the xct doesn't have these stashed,
     * it will malloc them for us.
     */
    DBG(<<"new_xct: id=" << me()->id);
    x->steal(tcb()._xct_log);
}

/**\brief Called to effect a detach_xct().
 *
 * \details
 * N Threads point to 1 xct_t; xct_ts do not point to threads because
 * of the 1:N relationship.
 *
 * A thread holds some cached info on behalf of a transaction.
 * This is in 3 structures.  If a thread were attached to a transaction
 * for the transaction's duration, we wouldn't go to this trouble, but
 * because threads attach/detach, reattach/detach and perhaps several
 * threads act for an xct at once, we try to avoid the excess heap
 * activity and cache-repopulation that would result.
 *
 * When a thread/xct relationship is broken, the thread tries to stash
 * its caches in the xct_t structure.  If the xct subsequently goes
 * away, the xct deletes these caches and returns them to the global heap.
 * If another thread attaches to the xct, it will grab these structures
 * from the xct at attach-time.
 *
 * This smthread can only stash these caches in the xct_t if the xct_t
 * doesn't already have some stashed. In other words, if 3 threads
 * detach from the same xct in succession, the first thread's caches will
 * be stashed in the xct and the other 2 will be returned to the heap.
 * If these 3 sthreads subsequently reattach to the same xct, the first
 * one to attach will steal back the caches and the next two will
 * allocate from the heap.
 *
 * In addition to these 3 caches, the thread holds statistics for
 * an instrumented transaction.
 */
void
smthread_t::no_xct(xct_t *x)
{
    w_assert3(x);
    /* collect summary statistics */

    // Don't collect again if we already detached. If we did
    // already detach, the stats values should be 0 to it would
    // be correct if we did this,  but it's needless work.
    //
    if(tcb().xct == x)
    {
        if(x->is_instrumented())
        {
            // NOTE: thread-safety comes from the fact that this is called from
            // xct_impl::detach_thread, which first grabs the 1thread-at-a-time
            // mutex.
            sm_stats_info_t &s = x->stats_ref();
            /*
            * s refers to the __stats passed in on begin_xct() for an
            * instrumented transaction.
            * We add in the per-thread stats and zero out the per-thread copy.
            * This means that if we are collecting stats on a per-xct basis,
            * these stats don't get counted in the global stats.
            *
            * Note also that this is a non-atomic add.
            */
            s += TL_stats(); // sm_stats_info_t

            /*
            * The stats have been added into the xct's structure,
            * so they must be cleared for the thread.
            */
            tcb().clear_TL_stats();
        }

        /* See comments in smthread_t::new_xct() */
        x->stash(tcb()._xct_log);
    }
}

void
smthread_t::_dump(ostream &o) const
{
    sthread_t *t = (sthread_t *)this;
    t->sthread_t::_dump(o);

    o << "smthread_t: " << (char *)(is_in_sm()?"in sm ":"") << endl;
    o << "transactions in this thread (from bottom):" << endl;
    for (tcb_t* tcb = _tcb_tail; tcb != NULL; tcb = tcb->_outer) {
        o << "xct ";
        if (tcb->xct) {
            o << tcb->xct->tid() << (tcb->xct->is_sys_xct() ? "(sys_xct)" : "(usr_xct)");
        } else {
            o << "<NULL xct>";
        }
        o << endl;
    }
}



class PrintBlockedThread : public ThreadFunc
{
    public:
                        PrintBlockedThread(ostream& o) : out(o) {};
                        ~PrintBlockedThread() {};
        void                operator()(const sthread_t& thread)
                        {
                            if (thread.status() == sthread_t::t_blocked)  {
                                out << "*******" << endl;
                                thread._dump(out);
                            }
                        };
    private:
        ostream&        out;
};

void
DumpBlockedThreads(ostream& o)
{
    PrintBlockedThread f(o);
    sthread_t::for_each_thread(f);
}
