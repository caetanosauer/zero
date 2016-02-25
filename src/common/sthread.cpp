/* -*- mode:C++; c-basic-offset:4 -*-
   Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore'>

 $Id: sthread.cpp,v 1.333 2010/12/08 17:37:50 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/


/*
 *   NewThreads is Copyright 1992, 1993, 1994, 1995, 1996, 1997 by:
 *
 *    Josef Burger    <bolo@cs.wisc.edu>
 *    Dylan McNamee    <dylan@cse.ogi.edu>
 *    Ed Felten       <felten@cs.princeton.edu>
 *
 *   All Rights Reserved.
 *
 *   NewThreads may be freely used as long as credit is given
 *   to the above authors and the above copyright is maintained.
 */

/*
 * The base thread functionality of Shore Threads is derived
 * from the NewThreads implementation wrapped up as c++ objects.
 */

#include <w.h>

#include <w_debug.h>
#include <w_stream.h>
#include <cstdlib>
#include <sched.h>
#include <cstring>

#include <ctime>

#include <sys/wait.h>
#include <new>

#include <sys/stat.h>
#include <sys/resource.h>
#include "tls.h"

#include "sthread.h"
#include "rand48.h"
#include "sthread_stats.h"
#include "stcore_pthread.h"

/* thread-local random number generator -- see rand48.h  */

/**\var static __thread rand48 tls_rng
 * \brief A 48-bit pseudo-random number generator
 * \ingroup TLS
 * \details
 * Thread-safety is achieved by having one per thread.
 */
static __thread rand48 tls_rng = RAND48_INITIALIZER;

int sthread_t::rand() { return tls_rng.rand(); }
double sthread_t::drand() { return tls_rng.drand(); }
int sthread_t::randn(int max) { return tls_rng.randn(max); }

class sthread_stats SthreadStats;

extern "C" void dumpthreads();
extern "C" void threadstats();

/*********************************************************************
 *
 * Class sthread_init_t
 *  Internal --- responsible for initialization
 *
 *********************************************************************/
/**\brief Responsible for initialization of Shore threads
 *
 * A static instance initializes the package by calling its static method
 * \code
   do_init()
 * \endcode
 *
 * This is also called on every sthread fork() and by
 * \code
    sthread_t::initialize_sthreads_package()
 * \endcode
 * which is called by the storage manager constructor.
 *
 * All this in lieu of using a Schwartz counter.
 *
 */
class sthread_init_t : public sthread_base_t {
public:
    NORET            sthread_init_t();
    static void      do_init();
    NORET            ~sthread_init_t();
private:
    static uint32_t           initialized;
    static w_pthread_lock_t  init_mutex;
};

static sthread_init_t sthread_init;

uint32_t    sthread_init_t::initialized = 0;
w_pthread_lock_t     sthread_init_t::init_mutex;
int64_t     sthread_t::max_os_file_size;

bool sthread_t::isStackOK(const char * /*file*/, int /*line*/) const
{
// NOTE: for now, I haven't found a way to get the current frame off
// a pthread stack other than the current one (me()), so this is
// not possible
    return true;
}


/* check all threads */
void sthread_t::check_all_stacks(const char *file, int line)
{
    w_list_i<sthread_t, queue_based_lock_t> i(*_class_list);
    unsigned    corrupt = 0;

    while (i.next())  {
        if (! i.curr()->isStackOK(file, line))
            corrupt++;
    }

    if (corrupt > 0) {
        cerr << "sthread_t::check_all: " << corrupt
        << " thread stacks, dieing" << endl;
        W_FATAL(fcINTERNAL);
    }
}


/* Give an estimate if the stack will overflow if a function with
   a stack frame of the requested size is called. */

// For debugger breakpoint:
extern "C" void stackoverflowed() {}

bool    sthread_t::isStackFrameOK(size_t size)
{
    bool ok;
    void *stack_top     =  &ok;
    void *_stack_top     = &ok - size;

    w_assert1(this->_danger < this->_start_frame);
    void *absolute_bottom = (void *)((char *)_start_frame - _stack_size);

    if( stack_top  < _danger) {
    if( stack_top  <= absolute_bottom) {
    fprintf(stderr,
// In order of values:
    "STACK OVERFLOW frame (offset -%lld) %p bottom %p danger %p top %p stack_size %lld \n",
    // cast so it works for -m32 and -m64
     (long long) size, _stack_top, absolute_bottom, _danger, _start_frame,
     (long long) _stack_size);
    } else {
    fprintf(stderr,
// In order of values:
    "STACK IN GUARD AREA bottom %p frame (offset -%lld) %p danger %p top %p stack_size %lld \n",
    // cast so it works for -m32 and -m64
     absolute_bottom, (long long) size, _stack_top, _danger, _start_frame,
     (long long) _stack_size);
    }
    return false;
    }

    return true;
}



/*********************************************************************
 *
 *  Class static variable intialization
 *
 *********************************************************************/


sthread_t*              sthread_t::_main_thread = 0;
const uint32_t MAIN_THREAD_ID(1);
uint32_t       sthread_t::_next_id = MAIN_THREAD_ID;
sthread_list_t*         sthread_t::_class_list = 0;
queue_based_lock_t      sthread_t::_class_list_lock;

stime_t                 sthread_t::boot_time = stime_t::now();

/*
 * sthread_t::cold_startup()
 *
 * Initialize system threads from cold-iron.  The main thread will run
 * on the stack startup() is called on.
 */

w_rc_t    sthread_t::cold_startup()
{

    _class_list = new sthread_list_t(W_LIST_ARG(sthread_t, _class_link),
                        &_class_list_lock);
    if (_class_list == 0)
        W_FATAL(fcOUTOFMEMORY);

    // initialize the global RNG
    struct timeval now;
    gettimeofday(&now, NULL);
    // Set the seed for the clib random-number generator, which
    // we use to seed the per-thread RNG
    ::srand(now.tv_usec);

    /*
     * Boot the main thread onto the current (system) stack.
     */
    sthread_main_t *main = new sthread_main_t;
    if (!main)
        W_FATAL(fcOUTOFMEMORY);
    me_lval() = _main_thread = main;
    W_COERCE( main->fork() );

    if (me() != main)
        W_FATAL(stINTERNAL);

    return RCOK;
}

/*
 * sthread_t::shutdown()
 *
 * Shutdown the thread system.  Must be called from the context of
 * the main thread.
 */

w_rc_t sthread_t::shutdown()
{
    if (me() != _main_thread) {
        cerr << "sthread_t::shutdown(): not main thread!" << endl;
        return RC(stINTERNAL);
    }

    return RCOK;
}


/**\cond skip */
/*
 *  sthread_main_t::sthread_main_t()
 *
 * Create the main thread.  It is a placeholder for the
 * thread which uses the system stack.
 */

sthread_main_t::sthread_main_t()
: sthread_t(t_regular, "main_thread", 0)
{
    /*
    fprintf(stderr, "sthread_main_t constructed, this %p\n", this);
    fflush(stderr);
    */
}


/*
 *  sthread_main_t::run()
 *
 *  This is never called.  It's an artifact of the thread architecture.
 *  It is a virtual function so that derived thread types put their guts here.
 */

void sthread_main_t::run()
{
}

/**\endcond skip */

/*
 *  sthread_t::set_priority(priority)
 *
 *  Sets the priority of current thread.  The thread must not
 *  be in the ready Q.
 */

w_rc_t sthread_t::set_priority(priority_t priority)
{
    CRITICAL_SECTION(cs, _wait_lock);
    _priority = priority;

    // in this statement, <= is used to keep gcc -Wall quiet
    if (_priority <= min_priority) _priority = min_priority;
    if (_priority > max_priority) _priority = max_priority;

    if (_status == t_ready)  {
        cerr << "sthread_t::set_priority()  :- "
            << "cannot change priority of ready thread" << endl;
        W_FATAL(stINTERNAL);
    }

    return RCOK;
}



/*
 *  sthread_t::sleep(timeout)
 *
 *  Sleep for timeout milliseconds.
 */

void sthread_t::sleep(timeout_in_ms timeout, const char *reason)
{
    reason = (reason && *reason) ? reason : "sleep";

    /* FRJ: even though we could just use the posix sleep() call,
       we'll stick to the sthreads way and block on a cond
       var. That way the sthreads debug stuff will keep
       working. Besides, we're here to waste time, right?
    */
    CRITICAL_SECTION(cs, _wait_lock);
    _sleeping = true;
    (void) _block(timeout, reason, this); // W_IGNORE
    _sleeping = false;
}

/*
 *  sthread_t::wakeup()
 *
 *  Cancel sleep
 */

void sthread_t::wakeup()
{
    CRITICAL_SECTION(cs, _wait_lock);
    if(_sleeping) _unblock(w_error_ok);
}


/*
 *  Wait for this thread to end. This method returns when this thread
 *  ends.  Timeout is no longer available.
 */

w_rc_t
sthread_t::join(timeout_in_ms /*timeout*/)
{
    w_rc_t rc;
    {
        CRITICAL_SECTION(cs, _start_terminate_lock);

        /* A thread that hasn't been forked can't be wait()ed for.
           It's not a thread until it has been fork()ed.
        */
        if (!_forked) {
            rc =  RC(stOS);
        } else
        {
            cs.exit();
            /*
             *  Wait for thread to finish.
             */
            sthread_core_exit(_core, _terminated);

        }
    }

    return rc;
}


/*
 * sthread_t::fork()
 *
 * Turn the "chunk of memory" into a real-live thread.
 */

w_rc_t    sthread_t::fork()
{
    {
        sthread_init_t::do_init();
        CRITICAL_SECTION(cs, _start_terminate_lock);
        /* can only fork a new thread */
        if (_forked)
            return RC(stOS);

        /* Add us to the list of threads, unless we are the main thread */
        if(this != _main_thread)
        {
            CRITICAL_SECTION(cs, _class_list_lock);
            _class_list->append(this);
        }


        _forked = true;
        if(this == _main_thread) {
            // happens at global constructor time
            CRITICAL_SECTION(cs_thread, _wait_lock);
            _status = t_running;
        } else    {
            // happens after main() called
            DO_PTHREAD( pthread_cond_signal(_start_cond) );
        }
    }

    return RCOK;
}


/*
 *  sthread_t::sthread_t(priority, name)
 *
 *  Create a thread.  Until it starts running, a created thread
 *  is just a memory object.
 */

sthread_t::sthread_t(priority_t        pr,
             const char     *nm,
             unsigned        stack_size)
: sthread_named_base_t(nm),
  user(0),
  id(_next_id++), // make it match the gdb threads #. Origin 1
  _start_terminate_lock(new pthread_mutex_t),
  _start_cond(new pthread_cond_t),
  _sleeping(false),
  _forked(false),
  _terminated(false),
  _unblock_flag(false),
  _core(0),
  _status(t_virgin),
  _priority(pr)
{
    if(!_start_terminate_lock || !_start_cond )
        W_FATAL(fcOUTOFMEMORY);

    DO_PTHREAD(pthread_cond_init(_start_cond, NULL));
    DO_PTHREAD(pthread_mutex_init(_start_terminate_lock, NULL));

    _core = new sthread_core_t;
    if (!_core)
        W_FATAL(fcOUTOFMEMORY);
    _core->sthread = (void *)this;  // not necessary, but might
                                    // be useful for debugging

    /*
     *  Set a valid priority level
     */
    if (_priority > max_priority)
        _priority = max_priority;
    else if (_priority <= min_priority)
        _priority = min_priority;

    /*
     *  Initialize the core.
     */
    DO_PTHREAD(pthread_mutex_init(&_wait_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_wait_cond, NULL));

    /*
     * stash the procedure (sthread_t::_start)
     * and arg (this)
     * in the core structure, along with
     * status info.
     * and if this is not the _main_thread (running in
     * the system thread, i.e., in an already-running pthread)
     * then create a pthread for it and give it a starting function
     // TODO: should probably merge sthread_core_pthread.cpp in here
     */
    if (sthread_core_init(_core, __start, this, stack_size) == -1) {
        cerr << "sthread_t: cannot initialize thread core" << endl;
        W_FATAL(stINTERNAL);
    }
}



/*
 *  sthread_t::~sthread_t()
 *
 *  Destructor. Thread must have already exited before its object
 *  is destroyed.
 */

sthread_t::~sthread_t()
{
    /*
    fprintf(stderr, "sthread_t %s destructed, this %p core %p pthread %p\n",
            name(), this, _core, (void *)myself());
    fflush(stderr);
    */
    {
    CRITICAL_SECTION(cs, _wait_lock);
    /* Valid states for destroying a thread are ...
       1) It was never started
       2) It ended.
       3) There is some braindamage in that that blocked threads
       can be deleted.  This is sick and wrong, and it
       can cause race conditions.  It is enabled for compatibility,
       and hopefully the warning messages will tell you if
       something is wrong. */
    w_assert1(_status == t_virgin
          || _status == t_defunct
          || _status == t_blocked
                  );

    if (_link.member_of()) {
        cerr << "sthread_t(" << (long)this << ") " << name()
            << " destroying a thread on a list!" << endl;
    }
    }
    sthread_core_exit(_core, _terminated);

    delete _core;
    _core = 0;

    DO_PTHREAD(pthread_cond_destroy(_start_cond));
    delete _start_cond;
    _start_cond = 0;

    DO_PTHREAD(pthread_mutex_destroy(_start_terminate_lock));
    delete _start_terminate_lock;
    _start_terminate_lock = 0; // clean up for valgrind

}

#ifndef PTHREAD_STACK_MIN
// This SHOULD be defined in <limits.h> (included from w_defines.h)
// but alas, I found that not to be the case on or solaris platform...
// so here's a workaround.
size_t get_pthread_stack_min()
{
   static size_t gotit(0);
   if(!gotit) {
      gotit = sysconf(_SC_THREAD_STACK_MIN);
      if(!gotit) {
          const char *errmsg =
         "Platform does not appear to conform to POSIX 1003.1c-1995 re: limits";

          W_FATAL_MSG(fcINTERNAL, << errmsg);
      }
      w_assert1(gotit > 0);
   }
   return gotit;
}
#endif



/* A springboard from "C" function + argument into an object */
void    sthread_t::__start(void *arg)
{
    sthread_t* t = (sthread_t*) arg;
    me_lval() = t;
    t->_start_frame = &t; // used to gauge danger of stack overflow

#ifndef PTHREAD_STACK_MIN
    size_t PTHREAD_STACK_MIN = get_pthread_stack_min();
#endif

#ifdef HAVE_PTHREAD_ATTR_GETSTACKSIZE
    pthread_attr_t attr;
    size_t         sz=0;
    int e = pthread_attr_init(&attr);
    if(e) {
        fprintf(stderr,"Cannot init pthread_attr e=%d\n", e);
        ::exit(1);
    }
    else
    {
        e = pthread_attr_getstacksize( &attr, &sz);
        if(e || sz==0) {
#ifdef HAVE_PTHREAD_ATTR_GETSTACK
            void *voidp(NULL);
            e = pthread_attr_getstack( &attr, &voidp, &sz);
            if(e || sz == 0)
#endif
            {
#if W_DEBUG_LEVEL > 2
                fprintf(stderr,"Cannot get pthread stack size e=%d, sz=%lld\n",
                e, (long long)sz);
#endif
                sz = PTHREAD_STACK_MIN;
            }
        }
    }
#define GUARD 8192*4
    if(sz <  GUARD) {
       // fprintf(stderr,"pthread stack size too small: %lld\n", (int64_t)sz);
#ifndef PTHREAD_STACK_MIN_SUBSTITUTE
// How did I come up with this number?  It's from experimenting with
// tests/thread1 on chianti, which seems not to be compliant in any way,
// not giving me any way to find out what the pthreads stack size is.
#define PTHREAD_STACK_MIN_SUBSTITUTE 0x100000
#endif
       sz = PTHREAD_STACK_MIN_SUBSTITUTE;
#if W_DEBUG_LEVEL > 2
       fprintf(stderr,"using  %lld temporarily\n", (long long)sz);
#endif
    }
    t->_stack_size = sz;

    // Lop off a few pages for a guard
    // though we're not actually mem-protecting these pages.
    // Rather, for debugging, we'll zero these pages, or a chunk of
    // them, and then we can check later to see if they got overwritten.
    sz -= GUARD;
    t->_danger = (void *)((char *)t->_start_frame - sz);

#endif
    w_assert1(t->_danger < t->_start_frame);
    w_assert1(t->_stack_size > 0);

    t->_start();
}

/*
 *  sthread_t::_start()
 *
 *  All *non-system* threads start and end here.
 */

void sthread_t::_start()
{
    tls_tricks::tls_manager::thread_init();
    w_assert1(me() == this);

    // assertions: will call stackoverflowed() if !ok and will return false
    w_assert1(isStackFrameOK(0));
    {
        CRITICAL_SECTION(cs, _start_terminate_lock);
        if(_forked) {
            // If the parent thread gets to fork() before
            // the child can get to _start(), then _forked
            // will be true. In this case, skip the condition wait.
            CRITICAL_SECTION(cs_thread, _wait_lock);
            _status = t_running;
        } else {
            DO_PTHREAD(pthread_cond_wait(_start_cond, _start_terminate_lock));
            CRITICAL_SECTION(cs_thread, _wait_lock);
            _status = t_running;
        }
    }

    {
        // thread checker complains about this not being reentrant
        // so we'll protect it with a mutex.
        // We could use reentrant rand_r but then we need to seed it.
        // and the whole point here is to use rand() to seed each thread
        // differently.
        // to protect non-reentrant rand()
        static queue_based_lock_t rand_mutex;

        long seed1, seed2;
        {
            CRITICAL_SECTION(cs, rand_mutex);
            seed1 = ::rand();
            seed2 = ::rand();
        }
        tls_rng.seed( (seed1 << 24) ^ seed2);
    }


    {
        /* do not save sigmask */
        w_assert1(me() == this);
#ifdef STHREAD_CXX_EXCEPTION
        // NOTE: this is not tested in SHORE-MT; it is old code.
        // TODO: exception-handling.

        /* Provide a "backstop" exception handler to catch uncaught
           exceptions in the thread.  This prevents them from going
           into never-never land. */
        try {
            before_run();
            run();
            after_run();
        }
        catch (...) {
            cerr << endl
                 << "sthread_t(id = " << id << "  name = " << name()
                 << "): run() threw an exception."
                 << endl
                 << endl;
        }
#else
        before_run();
        run();
        after_run();
#endif
    }

    /* Returned from run(). Current thread is ending. */
    {
        CRITICAL_SECTION(cs, _wait_lock);
        w_assert3(me() == this);
        _status = t_defunct;
        _link.detach();
    }
    {
        CRITICAL_SECTION(cs, _class_list_lock);
        _class_link.detach();
    }

    w_assert3(this == me());

    {
        w_assert1(_status == t_defunct);
        // wake up any thread that joined on us
        tls_tricks::tls_manager::thread_fini();
        pthread_exit(0);
    }

    W_FATAL(stINTERNAL);    // never reached
}



/*********************************************************************
 *
 *  sthread_t::block(&lock, timeout, list, caller, id)
 *  sthread_t::_block(*lock, timeout, list, caller, id)
 *
 *  Block the current thread and puts it on list.
 *
 * NOTE: the caller is assumed to already hold the lock(first arg)
 *
 *********************************************************************/
w_rc_t
sthread_t::block(
    pthread_mutex_t     &lock,
    timeout_in_ms       timeout,
    sthread_list_t*     list,        // list for thread after blocking
    const char* const   caller,        // for debugging only
    const void *        id)
{
    w_error_codes rce = _block(&lock, timeout, list, caller, id);
    if(rce) return RC(rce);
    return RCOK;
}

w_error_codes
sthread_t::block(int32_t timeout /*= WAIT_FOREVER*/)
{
    return  _block(NULL, timeout);
}

w_error_codes
sthread_t::_block(
    pthread_mutex_t     *lock,
    timeout_in_ms       timeout,
    sthread_list_t*     list,        // list for thread after blocking
    const char* const   caller,        // for debugging only
    const void *        id)
{
    w_error_codes rce = w_error_ok;
    sthread_t* self = me();
    {
        CRITICAL_SECTION(cs, self->_wait_lock);

        /*
         *  Put on list
         */
        w_assert3(self->_link.member_of() == 0); // not in other list
        if (list)  {
            list->put_in_order(self);
        }

        if(lock) {
            // the caller expects us to unlock this
            DO_PTHREAD(pthread_mutex_unlock(lock));
        }
        rce = _block(timeout, caller, id);
    }
    if(rce == stTIMEOUT) {
        if(lock) {
            CRITICAL_SECTION(outer_cs, &lock);

            CRITICAL_SECTION(cs, self->_wait_lock);
            self->_link.detach(); // we timed out and removed ourself from the waitlist
        } else {
            CRITICAL_SECTION(cs, self->_wait_lock);
            self->_link.detach(); // we timed out and removed ourself from the waitlist
        }
    }

    return rce;
}

void sthread_t::timeout_to_timespec(timeout_in_ms timeout, struct timespec &when)
{
    w_assert1(timeout != WAIT_IMMEDIATE);
    w_assert1(timeout != sthread_t::WAIT_FOREVER);
    if(timeout > 0) {
        ::clock_gettime(CLOCK_REALTIME, &when);
        when.tv_nsec += (uint64_t) timeout * 1000000;
        when.tv_sec += when.tv_nsec / 1000000000;
        when.tv_nsec = when.tv_nsec % 1000000000;
    }
}

w_error_codes
sthread_t::_block(
    timeout_in_ms    timeout,
    const char* const
        ,        // for debugging only
    const void        *
        )
{
// ASSUMES WE ALREADY LOCKED self->_wait_lock

    /*
     *  Sanity checks
     */
    sthread_t* self = me();
    w_assert1(timeout != WAIT_IMMEDIATE);   // not 0 timeout



    // wait...
    status_t old_status = self->_status;
    self->_status = t_blocked;

    int error = 0;
    self->_unblock_flag = false;
    if(timeout > 0) {
        timespec when;
        timeout_to_timespec(timeout, when);
        // ta-ta for now
        // pthread_cond_timedwait should return ETIMEDOUT when the
        // timeout has passed, so we should drop out if timed out,
        // and it should return 0 if we were signalled.
        while(!error && !self->_unblock_flag)  {
            error = pthread_cond_timedwait(&self->_wait_cond,
                    &self->_wait_lock, &when);
            w_assert1(error == ETIMEDOUT || error == 0);
            // Break out if we were signalled
            if(!error) break;
            // did we timeout? (TODO tentative fix. not sure what the original code did. should overhaul this function)
            timespec now;
            ::clock_gettime(CLOCK_REALTIME, &now);
            if (now.tv_sec > when.tv_sec || (now.tv_sec == when.tv_sec && now.tv_nsec >= when.tv_nsec)) {
                error = ETIMEDOUT;
                break;
            }
        }
    }
    else {
        // wait forever... no other abstract timeout should have gotten here
        w_assert1(timeout == sthread_t::WAIT_FOREVER);
        // wait until someone else unblocks us (sets _unblock_flag)
        // pthread_cond_wait should return 0 if no error, that is,
        // if we were signalled
        while(!error && !self->_unblock_flag)
                                     // condition          // mutex
            error = pthread_cond_wait(&self->_wait_cond, &self->_wait_lock);
    }
    // why did we wake up?
    switch(error) {
    case ETIMEDOUT:
        // FRJ: Not quite sure why this one thinks it's not being checked...
        W_COERCE(self->_unblock(stTIMEOUT));
        // fall through
    case 0:
        /* somebody called unblock(). We don't need to lock because
         * locking only matters to make sure the thread doesn't
         * perform its initial block() after it is told to fork().
         */
        self->_status = old_status;
        return self->_rce;
    default:
        self->_status = old_status;
        return stOS;
    }
}



/*********************************************************************
 *
 *  sthread_t::unblock(rc)
 *
 *  Unblock the thread with an error in rc.
 *
 *********************************************************************/
w_rc_t
sthread_t::unblock(w_error_codes e)
{
    CRITICAL_SECTION(cs, _wait_lock);

    /* Now that we hold both the list mutex (our caller did that) and
       the thread mutex, we can remove ourselves from the waitlist. To
       be honest, the list lock might be enough by itself, but we have
       to grab both locks anyway, so we may as well be doubly sure.
    */
    _link.detach();
    return _unblock(e);

}

// this version assumes caller holds _lock
w_rc_t
sthread_t::_unblock(w_error_codes e)
{
    _status = t_ready;

    /*
     *  Save rc (will be returned by block())
     */
    if (e)
        _rce = e;
    else
        _rce = w_error_ok;

    /*
     *  Thread is again ready.
     */
    _unblock_flag = true;
    lintel::atomic_thread_fence(lintel::memory_order_release); // make sure the unblock_flag is visible
    DO_PTHREAD(pthread_cond_signal(&_wait_cond));
    _status = t_running;

    return RCOK;
}



/*********************************************************************
 *
 *  sthread_t::yield()
 *  if do_select==true, we'll allow a select w/ 0 timeout
 *  to occur if the ready queue is empty
 *
 *  Give up CPU. Maintain ready status.
 *
 *  Used only in tests, nowhere in basic sm.
 *
 *********************************************************************/
void sthread_t::yield()
{
    sthread_t* self = me();
    CRITICAL_SECTION(cs, self->_wait_lock);
    w_assert3(self->_status == t_running);
    self->_status = t_ready;
    cs.pause();
    sched_yield();
    cs.resume();
    self->_status = t_running;
}

/* print all threads */
void sthread_t::dumpall(const char *str, ostream &o)
{
    if (str)
        o << str << ": " << endl;

    dumpall(o);
}

void sthread_t::dumpall(ostream &o)
{
// We've put this into a huge critical section
// to make it thread-safe, even though it's probably not necessary
// when used in the debugger, which is the only place this is used...
    CRITICAL_SECTION(cs, _class_list_lock);
    w_list_i<sthread_t, queue_based_lock_t> i(*_class_list);

    while (i.next())  {
        o << "******* ";
        if (me() == i.curr())
            o << " --->ME<---- ";
        o << endl;

        i.curr()->_dump(o);
    }
}


/* XXX individual thread dump function... obsoleted by print method */
void sthread_t::_dump(ostream &o) const
{
    o << *this << endl;
}

/* XXX it is not a bug that you can sometime see >100% cpu utilization.
   Don't even think about hacking something to change it.  The %CPU
   is an *estimate* developed by statistics gathered by the process,
   not something solid given by the kernel. */

static void print_time(ostream &o, const sinterval_t &real,
               const sinterval_t &user, const sinterval_t &kernel)
{
    sinterval_t    total(user + kernel);
    double    pcpu = ((double)total / (double)real) * 100.0;
    double     pcpu2 = ((double)user / (double)real) * 100.0;

    o << "\t" << "real: " << real
        << endl;
    o << "\tcpu:"
        << "  kernel: " << kernel
        << "  user: " << user
        << "  total: " << total
        << endl;
    o << "\t%CPU:"
        << " " << setprecision(3) << pcpu
        << "  %user: " << setprecision(2) << pcpu2;
        o
        << endl;
}

void sthread_t::dump_stats(ostream &o)
{
    o << me()->SthreadStats;

    /* To be moved somewhere else once I put some other infrastructure
       into place.  Live with it in the meantime, the output is really
       useful for observing ad-hoc system performance. */
    struct    rusage    ru;
    int                 n;

    stime_t    now(stime_t::now());
    n = getrusage(RUSAGE_SELF, &ru);
    if (n == -1) {
        w_rc_t    e = RC(fcOS);
        cerr << "getrusage() fails:" << endl << e << endl;
        return;
    }

    sinterval_t    real(now - boot_time);
    sinterval_t    kernel(ru.ru_stime);
    sinterval_t    user(ru.ru_utime);

    /* Try to provide some modicum of recent cpu use. This will eventually
       move into the class, once a "thread handler" arrives to take
       care of it. */
    static    sinterval_t    last_real;
    static    sinterval_t    last_kernel;
    static    sinterval_t    last_user;
    static    bool last_valid = false;

    o << "TIME:" << endl;
    print_time(o, real, user, kernel);
    if (last_valid) {
        sinterval_t    r(real - last_real);
        sinterval_t    u(user - last_user);
        sinterval_t    k(kernel - last_kernel);
        o << "RECENT:" << endl;
        print_time(o, r, u, k);
    }
    else
        last_valid = true;

    last_kernel = kernel;
    last_user = user;
    last_real = real;

    o << endl;
}

void sthread_t::reset_stats()
{
    me()->SthreadStats.clear();
}


const char *sthread_t::status_strings[] = {
    "defunct",
    "virgin",
    "ready",
    "running",
    "blocked",
    "boot"
};

const char *sthread_t::priority_strings[]= {
    "idle_time",
    "fixed_low",
    "regular",
    "time_critical"
};


ostream& operator<<(ostream &o, const sthread_t &t)
{
    return t.print(o);
}


/*
 *  sthread_t::print(stream)
 *
 *  Print thread status to an stream
 */
ostream &sthread_t::print(ostream &o) const
{
    o << "thread id = " << id ;

    if (name()) {
        o << ", name = " << name() ? name() : "anonymous";
    };

    o
    << ", addr = " <<  (void *) this
    << ", core = " <<  (void *) _core << endl;
    o
    << "priority = " << sthread_t::priority_strings[priority()]
    << ", status = " << sthread_t::status_strings[status()];
    o << endl;

    if (user)
        o << "user = " << user << endl;

    if ((status() != t_defunct)  && !isStackOK(__FILE__,__LINE__))
    {
        cerr << "***  warning:  Thread stack overflow  ***" << endl;
    }

    return o;
}



/*********************************************************************
 *
 *  sthread_t::for_each_thread(ThreadFunc& f)
 *
 *  For each thread in the system call the function object f.
 *
 *********************************************************************/
void sthread_t::for_each_thread(ThreadFunc& f)
{
// We've put this into a huge critical section
// to make it thread-safe, even though it's probably not necessary
// when used in the debugger, which is the only place this is used...
    CRITICAL_SECTION(cs, _class_list_lock);
    w_list_i<sthread_t, queue_based_lock_t> i(*_class_list);

    while (i.next())  {
        f(*i.curr());
    }
}

void print_timeout(ostream& o, const sthread_base_t::timeout_in_ms timeout)
{
    if (timeout > 0)  {
    o << timeout;
    }  else if (timeout >= -5)  {
    static const char* names[] = {"WAIT_IMMEDIATE",
                      "WAIT_FOREVER",
                      "WAIT_ANY", // DEAD
                      "WAIT_ALL", // DEAD
                      "WAIT_SPECIFIED_BY_THREAD",
                      "WAIT_SPECIFIED_BY_XCT"};
    o << names[-timeout];
    }  else  {
    o << "UNKNOWN_TIMEOUT_VALUE(" << timeout << ")";
    }
}

occ_rwlock::occ_rwlock()
    : _active_count(0)
{
    _write_lock._lock = _read_lock._lock = this;
    DO_PTHREAD(pthread_mutex_init(&_read_write_mutex, NULL));
    DO_PTHREAD(pthread_cond_init(&_read_cond, NULL));
    DO_PTHREAD(pthread_cond_init(&_write_cond, NULL));
}

occ_rwlock::~occ_rwlock()
{
    DO_PTHREAD(pthread_mutex_destroy(&_read_write_mutex));
    DO_PTHREAD(pthread_cond_destroy(&_read_cond));
    DO_PTHREAD(pthread_cond_destroy(&_write_cond));
    _write_lock._lock = _read_lock._lock = NULL;
}

void occ_rwlock::release_read()
{
    lintel::atomic_thread_fence(lintel::memory_order_release);
    w_assert1(READER <= (int) _active_count);
    unsigned count = lintel::unsafe::atomic_fetch_sub(const_cast<unsigned*>(&_active_count), (unsigned)READER) - READER;
    if(count == WRITER) {
        // wake it up
        CRITICAL_SECTION(cs, _read_write_mutex);
        DO_PTHREAD(pthread_cond_signal(&_write_cond));
    }
}

void occ_rwlock::acquire_read()
{
    unsigned count = lintel::unsafe::atomic_fetch_add(const_cast<unsigned*>(&_active_count), (unsigned)READER) + READER;
    while(count & WRITER) {
        // block
        count = lintel::unsafe::atomic_fetch_sub(const_cast<unsigned*>(&_active_count), (unsigned)READER) - READER;
        {
            CRITICAL_SECTION(cs, _read_write_mutex);

            // nasty race: we could have fooled a writer into sleeping...
            if(count == WRITER) {
                DO_PTHREAD(pthread_cond_signal(&_write_cond));
            }

            while(*&_active_count & WRITER) {
                DO_PTHREAD(pthread_cond_wait(&_read_cond, &_read_write_mutex));
            }
        }
        count = lintel::unsafe::atomic_fetch_add(const_cast<unsigned*>(&_active_count), (unsigned)READER) - READER;
    }
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
}

void occ_rwlock::release_write()
{
    w_assert9(_active_count & WRITER);
    CRITICAL_SECTION(cs, _read_write_mutex);
    lintel::unsafe::atomic_fetch_sub(const_cast<unsigned*>(&_active_count), (unsigned)WRITER);
    DO_PTHREAD(pthread_cond_broadcast(&_read_cond));
}

void occ_rwlock::acquire_write()
{
    // only one writer allowed in at a time...
    CRITICAL_SECTION(cs, _read_write_mutex);
    while(*&_active_count & WRITER) {
        DO_PTHREAD(pthread_cond_wait(&_read_cond, &_read_write_mutex));
    }

    // any lurking writers are waiting on the cond var
    unsigned count = lintel::unsafe::atomic_fetch_add(const_cast<unsigned*>(&_active_count), (unsigned)WRITER) + WRITER;
    w_assert1(count & WRITER);

    // drain readers
    while(count != WRITER) {
        DO_PTHREAD(pthread_cond_wait(&_write_cond, &_read_write_mutex));
        count = *&_active_count;
    }
}

/**\cond skip */

sthread_name_t::sthread_name_t()
{
    memset(_name, '\0', sizeof(_name));
}

sthread_name_t::~sthread_name_t()
{
}

void
sthread_name_t::rename(
    // can't have n2 or n3 without n1
    // can have n1,0,n3 or n1,n2,0
    const char*        n1,
    const char*        n2,
    const char*        n3)
{
    const int sz = sizeof(_name) - 1;
    size_t len = 0;
    _name[0] = '\0';
    if (n1)  {
#if W_DEBUG_LEVEL > 2
        len = strlen(n1);
        if(n2) len += strlen(n2);
        if(n3) len += strlen(n3);
        len++;
        if(len>sizeof(_name)) {
            cerr << "WARNING-- name too long for sthread_named_t: "
                << n1 << n2 << n3;
        }
#endif

        // only copy as much as will fit
        strncpy(_name, n1, sz);
        len = strlen(_name);
        if (n2 && (int)len < sz)  {
            strncat(_name, n2, sz - len);
            len = strlen(_name);
            if (n3 && (int)len < sz)
                strncat(_name, n3, sz - len);
        }

        _name[sz] = '\0';
    }

}

void
sthread_named_base_t::unname()
{
    rename(0,0,0);
}

void
sthread_named_base_t::rename(
    // can't have n2 or n3 without n1
    // can have n1,0,n3 or n1,n2,0
    const char*        n1,
    const char*        n2,
    const char*        n3)
{
    _name.rename(n1,n2,n3);
}

sthread_named_base_t::~sthread_named_base_t()
{
    unname();
}

/**\endcond skip */

/**\cond skip */

// if you really are a sthread_t return 0
smthread_t* sthread_t::dynamic_cast_to_smthread()
{
    return 0;
}


const smthread_t* sthread_t::dynamic_cast_to_const_smthread() const
{
    return 0;
}

/**\endcond skip */

/*********************************************************************
 *
 *  dumpthreads()
 *  For debugging, but it's got to be
 *  present in servers compiled without debugging.
 *
 *********************************************************************/
void dumpthreads()
{
    sthread_t::dumpall("dumpthreads()", cerr);
    sthread_t::dump_io(cerr);

}

void threadstats()
{
    sthread_t::dump_stats(cerr);
}



static    void    get_large_file_size(int64_t &max_os_file_size)
{
    /*
     * Get limits on file sizes imposed by the operating
     * system and shell.
     */
    os_rlimit_t    r;
    int        n;

    n = os_getrlimit(RLIMIT_FSIZE, &r);
    if (n == -1) {
        w_rc_t e = RC(fcOS);
        cerr << "getrlimit(RLIMIT_FSIZE):" << endl << e << endl;
        W_COERCE(e);
    }
    if (r.rlim_cur < r.rlim_max) {
        r.rlim_cur = r.rlim_max;
        n = os_setrlimit(RLIMIT_FSIZE, &r);
        if (n == -1) {
            w_rc_t e = RC(fcOS);
            cerr << "setrlimit(RLIMIT_FSIZE, " << r.rlim_cur
                << "):" << endl << e << endl;
                cerr << e << endl;
            W_FATAL(fcINTERNAL);
        }
    }
    max_os_file_size = int64_t(r.rlim_cur);
    /*
     * Unfortunately, sometimes this comes out
     * negative, since r.rlim_cur is unsigned and
     * fileoff_t is signed (sigh).
     */
    if (max_os_file_size < 0) {
        max_os_file_size = uint64_t(r.rlim_cur) >> 1;
        w_assert1( max_os_file_size > 0);
    }
}

/* XXX this doesn't work, neither does the one in sdisk, because
   the constructor order isn't guaranteed.  The only important
   use before main() runs is the one right above here. */


/*********************************************************************
 *
 *  sthread_init_t::sthread_init_t()
 *
 *  Initialize the sthread environment. The first time this method
 *  is called, it sets up the environment
 *
 *********************************************************************/

// We'll have the ss_m constructor do this and just to be safe,
// we'll have fork also do this init.
// Cleanup is done in global destructors.
void  sthread_t::initialize_sthreads_package()
{   sthread_init_t::do_init(); }

NORET
sthread_init_t::sthread_init_t() { }

void
sthread_init_t::do_init()
{
    // This should not ever get initialized more than once
    if (sthread_init_t::initialized == 0)
    {
        CRITICAL_SECTION(cs, init_mutex);

        // check again
        if (sthread_init_t::initialized == 0)
        {
            sthread_init_t::initialized ++;

            get_large_file_size(sthread_t::max_os_file_size);

            W_COERCE(sthread_t::cold_startup());
        }
    }
}



/*********************************************************************
 *
 *  sthread_init_t::~sthread_init_t()
 *
 *  Destructor. Does not do much.
 *
 *********************************************************************/
NORET
sthread_init_t::~sthread_init_t()
{
    CRITICAL_SECTION(cs, init_mutex);

    // This should not ever get initialized more than once
    // Could be that it never got initialized.
    w_assert1 (sthread_init_t::initialized <= 1)  ;
    if (--sthread_init_t::initialized == 0)
    {

        W_COERCE(sthread_t::shutdown());

        // Must delete the main thread before you delete the class list,
        // since it'll not be empty until main thread is gone.
        //
        /* note: me() is main thread */
        sthread_t::_main_thread->_status = sthread_t::t_defunct;

        delete sthread_t::_main_thread; // clean up for valgrind
        sthread_t::_main_thread = 0;

        delete sthread_t::_class_list; // clean up for valgrind
        sthread_t::_class_list = 0;
    }
}

pthread_t sthread_t::myself() { return _core->pthread; }

