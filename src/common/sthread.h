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
/*<std-header orig-src='shore' incl-file-exclusion='STHREAD_H'>

 $Id: sthread.h,v 1.208 2010/12/09 15:20:17 nhall Exp $

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

/*  -- do not edit anything above this line --   </std-header>*/

/*
 * The SHORE threads layer has some historical roots in the
 * the NewThreads implementation wrapped up as c++ objects.
 *
 * With release 6.0 of the SHORE Storage Manager, the NewThreads
 * functionality was substantially obviated.  Some bits and pieces
 * of the SHORE threads layer remains in the synchronization variables
 * in the sthread_t API.
 *
 * To the extent that any NewThreads code remains here,
 * the following copyright applies:
 *
 *   NewThreads is Copyright 1992, 1993, 1994, 1995, 1996, 1997 by:
 *
 *    Josef Burger    <bolo@cs.wisc.edu>
 *    Dylan McNamee   <dylan@cse.ogi.edu>
 *    Ed Felten       <felten@cs.princeton.edu>
 *
 *   All Rights Reserved.
 *
 *   NewThreads may be freely used as long as credit is given
 *   to the above authors and the above copyright is maintained.
 */

/**\file sthread.h
 *\ingroup MACROS
 *
 * This file contains the Shore Threads API.
 */

#ifndef STHREAD_H
#define STHREAD_H

#include "w_defines.h"
#include "w_rc.h"
#include "AtomicCounter.hpp"
#include "w_strstream.h"
#include "stime.h"
#include "gethrtime.h"
#include <w_list.h>
#include <vector>

// this #include reflects the fact that sthreads is now just a pthreads wrapper
#include <w_pthread.h>
#include <sthread_stats.h>

class sthread_t;
class smthread_t;


#ifndef SDISK_H
#include <sdisk.h>
#endif

class vtable_row_t;
class vtable_t;

struct sthread_core_t;

extern "C" void         dumpthreads(); // for calling from debugger


/**\brief Base class for sthreads.  See \ref timeout_in_ms, \ref timeout_t
 */
class sthread_base_t : public w_base_t {
public:
/**\cond skip */
    typedef unsigned int w_thread_id_t; // TODO REMOVE
    typedef w_thread_id_t id_t;
/**\endcond skip */

    /* XXX this is really something for the SM, not the threads package;
       only WAIT_IMMEDIATE should ever make it to the threads package. */

    /**\enum timeout_t
     * \brief Special values for timeout_in_ms.
     *
     * \details sthreads package recognizes 2 WAIT_* values:
     * == WAIT_IMMEDIATE
     * and != WAIT_IMMEDIATE.
     *
     * If it's not WAIT_IMMEDIATE, it's assumed to be
     * a positive integer (milliseconds) used for the
     * select timeout.
     * WAIT_IMMEDIATE: no wait
     * WAIT_FOREVER:   may block indefinitely
     * The user of the thread (e.g., sm) had better
     * convert timeout that are negative values (WAIT_* below)
     * to something >= 0 before calling block().
     *
     * All other WAIT_* values other than WAIT_IMMEDIATE
     * are handled by sm layer:
     * WAIT_SPECIFIED_BY_THREAD: pick up a timeout_in_ms from the smthread.
     * WAIT_SPECIFIED_BY_XCT: pick up a timeout_in_ms from the transaction.
     * Anything else: not legitimate.
     *
     * \sa timeout_in_ms
     */
    enum timeout_t {
    WAIT_IMMEDIATE     = 0,
    WAIT_FOREVER     = -1,
    WAIT_SPECIFIED_BY_THREAD     = -4, // used by lock manager
    WAIT_SPECIFIED_BY_XCT = -5, // used by lock manager
    WAIT_NOT_USED = -6 // indicates last negative number used by sthreads
    };
    /* XXX int would also work, sized type not necessary */
    /**\typedef int32_t timeout_in_ms;
     * \brief Timeout in milliseconds if > 0
     * \details
     * sthread_t blocking methods take a timeout in milliseconds.
     * If the value is < 0, then it's expected to be a member of the
     * enumeration type timeout_t.
     *
     * \sa timeout_t
     */
    typedef int32_t timeout_in_ms;

/**\cond skip */

    /* import sdisk base */
    typedef sdisk_base_t::fileoff_t    fileoff_t;
    typedef sdisk_base_t::filestat_t   filestat_t;
    typedef sdisk_base_t::iovec_t      iovec_t;


    /* XXX magic number */
    enum { iovec_max = 8 };

    enum {
    OPEN_RDWR = sdisk_base_t::OPEN_RDWR,
    OPEN_RDONLY = sdisk_base_t::OPEN_RDONLY,
    OPEN_WRONLY = sdisk_base_t::OPEN_WRONLY,

    OPEN_SYNC = sdisk_base_t::OPEN_SYNC,
    OPEN_TRUNC = sdisk_base_t::OPEN_TRUNC,
    OPEN_CREATE = sdisk_base_t::OPEN_CREATE,
    OPEN_EXCL = sdisk_base_t::OPEN_EXCL,
    OPEN_APPEND = sdisk_base_t::OPEN_APPEND,
    OPEN_DIRECT = sdisk_base_t::OPEN_DIRECT
    };
    enum {
    SEEK_AT_SET = sdisk_base_t::SEEK_AT_SET,
    SEEK_AT_CUR = sdisk_base_t::SEEK_AT_CUR,
    SEEK_AT_END = sdisk_base_t::SEEK_AT_END
    };
/**\endcond skip */
};

/**\cond skip */
class sthread_name_t {
public:
    enum { NAME_ARRAY = 64 };

    char        _name[NAME_ARRAY];

    sthread_name_t();
    ~sthread_name_t();

    void rename(const char *n1, const char *n2=0, const char *n3=0);
};

class sthread_named_base_t: public sthread_base_t
{
public:
    NORET            sthread_named_base_t(
    const char*            n1 = 0,
    const char*            n2 = 0,
    const char*            n3 = 0);
    NORET            ~sthread_named_base_t();

    void            rename(
    const char*            n1,
    const char*            n2 = 0,
    const char*            n3 = 0);

    const char*            name() const;
    void                   unname();

private:
    sthread_name_t        _name;
};

inline NORET
sthread_named_base_t::sthread_named_base_t(
    const char*        n1,
    const char*        n2,
    const char*        n3)
{
    rename(n1, n2, n3);

}

inline const char*
sthread_named_base_t::name() const
{
    return _name._name;
}

class sthread_main_t;

/**\endcond skip */

/**\brief A callback class for traversing the list of all sthreads.
 * \details
 * Use with for_each_thread. Somewhat costly because it's thread-safe.
 */
class ThreadFunc
{
    public:
    virtual void operator()(const sthread_t& thread) = 0;
    virtual NORET ~ThreadFunc() {}
};


class sthread_init_t;
class sthread_main_t;

// these macros allow us to notify the SunStudio race detector about lock acquires/releases

#include "tatas.h"

/**\brief Wrapper for pthread mutexes, with a queue-based lock API.
 *
 * When the storage manager is configured with the default,
 * --enable-pthread-mutex, this lock uses a Pthreads mutex for the lock.
 * In this case, it is not a true queue-based lock, since
 * release doesn't inform the next node in the queue, and in fact the
 * nodes aren't kept in a queue.
 * It just gives pthread mutexes the same API as the other
 * queue-based locks so that we use the same idioms for
 * critical sections based on different kinds of locks.
 * By configuring with pthreads mutexes implementing this class, the
 * server can spawn any number of threads, regardless of the number
 * of hardware contexts available; threads will block as necessary.
 *
 * When the storage manager is configured with
 * --disable-pthread-mutex, this lock uses an MCS (\ref MCS1) queue-based
 * lock for the lock.
 * In this case, it is a true queue-based lock.
 * By configuring with MCS locks implementing this class, if the
 * server spawn many more threads than hardware contexts, time can be wasted
 * spinning; threads will not block until the operating system (or underlying
 * thread scheduler) determines to block the thread.
 *
 * The idiom for using these locks is
 * that the qnode is on a threads's stack, so the qnode
 * implicitly identifies the owning thread.
 *
 * This allows us to add an is_mine() capability that otherwise
 * the pthread mutexen don't have.
 *
 * Finally, using this class ensures that the pthread_mutex_init/destroy
 * is done (in the --enable-pthread-mutex case).
 *
 *  See also: \ref REFSYNC
 *
 */
struct w_pthread_lock_t
{
    /**\cond skip */
    struct ext_qnode {
        w_pthread_lock_t* _held;
    };
#define PTHREAD_EXT_QNODE_INITIALIZER { NULL }
#define PTHREAD_EXT_QNODE_INITIALIZE(x) (x)._held =  NULL

    typedef ext_qnode volatile* ext_qnode_ptr;
    /**\endcond skip */

private:
    pthread_mutex_t     _mutex; // w_pthread_lock_t blocks on this
    /// Holder is this struct if acquire is successful.
    w_pthread_lock_t *  _holder;

public:
    w_pthread_lock_t() :_holder(0) { pthread_mutex_init(&_mutex, 0); }

    ~w_pthread_lock_t()
    {
////////////////////////////////////////
// TODO(Restart)... comment out the assertion in debug mode for 'instant restart' testing purpose
//                    if we are using simulated crash shutdown, this assertion might fire if
//                    we are in the middle of taking a checkpoint
//                    this is for mutex chkpt_serial_m::write_release();
//                    need a way to ignore _holder checking if using simulated system crash
//
//                    For now, comment out the assertion, although we might miss other
//                    bugs by comment out the assertion
////////////////////////////////////////

//        w_assert1(!_holder);

        pthread_mutex_destroy(&_mutex);
    }

    /// Returns true if success.
    bool attempt(ext_qnode* me) {
        if(attempt( *me)) {
            me->_held = this;
            _holder = this;
            return true;
        }
        return false;
    }

private:
    /// Returns true if success. Helper for attempt(ext_qnode *).
    bool attempt(ext_qnode & me) {
        w_assert1(!is_mine(&me));
        w_assert0( me._held == 0 );  // had better not
        // be using this qnode for another lock!
        return pthread_mutex_trylock(&_mutex) == 0;
    }

public:
    /// Acquire the lock and set the qnode to refer to this lock.
    void* acquire(ext_qnode* me) {
        w_assert1(!is_mine(me));
        w_assert1( me->_held == 0 );  // had better not
        // be using this qnode for another lock!
        pthread_mutex_lock(&_mutex);
        me->_held = this;
        _holder = this;
#if W_DEBUG_LEVEL > 0
        {
            lintel::atomic_thread_fence(lintel::memory_order_acquire); // needed for the assert
            w_assert1(is_mine(me)); // TODO: change to assert2
        }
#endif
        return 0;
    }

    /// Release the lock and clear the qnode.
    void release(ext_qnode &me) { release(&me); }

    /// Release the lock and clear the qnode.
    void release(ext_qnode_ptr me) {
        // assert is_mine:
        w_assert1( _holder == me->_held );
        w_assert1(me->_held == this);
         me->_held = 0;
        _holder = 0;
        pthread_mutex_unlock(&_mutex);
#if W_DEBUG_LEVEL > 10
        // This is racy since the containing structure could
        // have been freed by the time we do this check.  Thus,
        // we'll remove it.
        {
            lintel::atomic_thread_fence(lintel::memory_order_acquire);// needed for the assertions?
            w_pthread_lock_t *h =  _holder;
            w_pthread_lock_t *m =  me->_held;
            w_assert1( (h==NULL && m==NULL)
                || (h  != m) );
        }
#endif
    }

    /**\brief Return true if this thread holds the lock.
     *
     * This method doesn't actually check for this pthread
     * holding the lock, but it checks that the qnode reference
     * is to this lock.
     * The idiom for using these locks is
     * that the qnode is on a threads's stack, so the qnode
     * implicitly identifies the owning thread.
     */

    bool is_mine(ext_qnode* me) const {
       if( me->_held == this ) {
           // only valid if is_mine
          w_assert1( _holder == me->_held );
          return true;
       }
       return false;
    }
};

/**\def USE_PTHREAD_MUTEX
 * \brief If defined and value is 1, use pthread-based mutex for queue_based_lock_t
 *
 * \details
 * The Shore-MT release contained alternatives for scalable locks in
 * certain places in the storage manager; it was released with
 * these locks replaced by pthreads-based mutexes.
 *
 * You can disable the use of pthreads-based mutexes and use the
 * mcs-based locks by configuring with --disable-pthread-mutex.
 */

/**\defgroup SYNCPRIM Synchronization Primitives
 *\ingroup UNUSED
 *
 * sthread/sthread.h: As distributed, a queue-based lock
 * is a w_pthread_lock_t,
 * which is a wrapper around a pthread lock to give it a queue-based-lock API.
 * True queue-based locks are not used, nor are time-published
 * locks.
 * Code for these implementations is included for future
 * experimentation, along with typedefs that should allow
 * easy substitution, as they all should have the same API.
 *
 * We don't offer the spin implementations at the moment.
 */
/*
 * These typedefs are included to allow substitution at some  point.
 * Where there is a preference, the code should use the appropriate typedef.
 */

typedef w_pthread_lock_t queue_based_block_lock_t; // blocking impl always ok
#define QUEUE_BLOCK_EXT_QNODE_INITIALIZER PTHREAD_EXT_QNODE_INITIALIZER
// non-static initialize:
#define QUEUE_BLOCK_EXT_QNODE_INITIALIZE(x) x._held = NULL

#ifdef USE_PTHREAD_MUTEX
typedef w_pthread_lock_t queue_based_spin_lock_t; // spin impl preferred
typedef w_pthread_lock_t queue_based_lock_t; // might want to use spin impl
#define QUEUE_SPIN_EXT_QNODE_INITIALIZER PTHREAD_EXT_QNODE_INITIALIZER
#define QUEUE_EXT_QNODE_INITIALIZER      PTHREAD_EXT_QNODE_INITIALIZER
// non-static initialize:
#define QUEUE_EXT_QNODE_INITIALIZE(x) x._held = NULL;
#else
#include <mcs_lock.h>
typedef mcs_lock queue_based_spin_lock_t; // spin preferred
typedef mcs_lock queue_based_lock_t;
#define QUEUE_SPIN_EXT_QNODE_INITIALIZER MCS_EXT_QNODE_INITIALIZER
#define QUEUE_EXT_QNODE_INITIALIZER      MCS_EXT_QNODE_INITIALIZER
// non-static initialize:
#define QUEUE_EXT_QNODE_INITIALIZE(x) MCS_EXT_QNODE_INITIALIZE(x)
#endif

#ifndef SRWLOCK_H
#include <srwlock.h>
#endif

/**\brief A multiple-reader/single-writer lock based on pthreads (blocking)
 *
 * Use this to protect data structures that get hammered by
 *  reads and where updates are very rare.
 * It is used in the storage manager by the histograms (histo.cpp),
 * and in place of some mutexen, where strict exclusion isn't required.
 *
 * This lock is used in the storage manager by the checkpoint thread
 * (the only acquire-writer) and other threads to be sure they don't
 * do certain nasty things when a checkpoint is going on.
 *
 * The idiom for using these locks is
 * that the qnode is on a threads's stack, so the qnode
 * implicitly identifies the owning thread.
 *
 *  See also: \ref REFSYNC
 *
 */
struct occ_rwlock {
    occ_rwlock();
    ~occ_rwlock();
    /// The normal way to acquire a read lock.
    void acquire_read();
    /// The normal way to release a read lock.
    void release_read();
    /// The normal way to acquire a write lock.
    void acquire_write();
    /// The normal way to release a write lock.
    void release_write();

    /**\cond skip */
    /// Exposed for critical_section<>. Do not use directly.
    struct occ_rlock {
        occ_rwlock* _lock;
        void acquire() { _lock->acquire_read(); }
        void release() { _lock->release_read(); }
    };
    /// Exposed for critical_section<>. Do not use directly.
    struct occ_wlock {
        occ_rwlock* _lock;
        void acquire() { _lock->acquire_write(); }
        void release() { _lock->release_write(); }
    };

    /// Exposed for the latch manager.. Do not use directly.
    occ_rlock *read_lock() { return &_read_lock; }
    /// Exposed for the latch manager.. Do not use directly.
    occ_wlock *write_lock() { return &_write_lock; }
    /**\endcond skip */
private:
    enum { WRITER=1, READER=2 };
    unsigned int volatile _active_count;
    occ_rlock _read_lock;
    occ_wlock _write_lock;

    pthread_mutex_t _read_write_mutex; // paired w/ _read_cond, _write_cond
    pthread_cond_t _read_cond; // paired w/ _read_write_mutex
    pthread_cond_t _write_cond; // paired w/ _read_write_mutex
};

typedef w_list_t<sthread_t, queue_based_lock_t>        sthread_list_t;


/**\brief Thread class for all threads that use the Shore Storage Manager.
 *
 *  All threads that perform \b any work on behalf of the storage
 *  manager or call any storage manager API \b must be an sthread_t or
 *  a class derived from sthread_t.
 *
 *  Storage manager threads use block/unblock methods provided by
 *  sthread, and use thread-local storage (data attributes of
 *  sthread_t).
 *
 *  This class also provides an os-independent API for file-system
 *  calls (open, read, write, close, etc.) used by the storage manager.
 *
 *  This class is a fairly thin layer over pthreads.  Client threads
 *  may use pthread synchronization primitives.
 */
class sthread_t : public sthread_named_base_t
{
    friend class sthread_init_t;
    friend class sthread_main_t;
    /* For access to block() and unblock() */
    friend class latch_t;
    /* For access to I/O stats */


public:
    static void  initialize_sthreads_package();

    enum status_t {
        t_defunct,    // thread has terminated
        t_virgin,    // thread hasn't started yet
        t_ready,    // thread is ready to run
        t_running,    // when me() is this thread
        t_blocked,      // thread is blocked on something
        t_boot        // system boot
    };
    static const char *status_strings[];

    enum priority_t {
        t_time_critical = 1,
        t_regular    = 0,
        max_priority    = t_time_critical,
        min_priority    = t_regular
    };
    static const char *priority_strings[];

    /* Default stack size for a thread */
    enum { default_stack = 64*1024 };

    /*
     *  Class member variables
     */
    void*             user;    // user can use this
    const id_t        id;

    // max_os_file_size is used by the sm and set in
    // static initialization of sthreads (sthread_init_t in sthread.cpp)
    static int64_t     max_os_file_size;

private:

    // ASSUMES WE ALREADY LOCKED self->_wait_lock
    static w_error_codes        _block(
                            timeout_in_ms          timeout = WAIT_FOREVER,
                            const char* const      caller = 0,
                            const void *           id = 0);

    static w_error_codes        _block(
                            pthread_mutex_t        *lock,
                            timeout_in_ms          timeout = WAIT_FOREVER,
                            sthread_list_t*        list = 0,
                            const char* const      caller = 0,
                            const void *           id = 0);

    w_rc_t               _unblock(w_error_codes e);

public:
    static void          timeout_to_timespec(timeout_in_ms timeout,
                                             struct timespec &when);
    w_rc_t               unblock(w_error_codes e);
    static w_rc_t        block(
                            pthread_mutex_t        &lock,
                            timeout_in_ms          timeout = WAIT_FOREVER,
                            sthread_list_t*        list = 0,
                            const char* const      caller = 0,
                            const void *           id = 0);
    static w_error_codes       block(int32_t  timeout = WAIT_FOREVER);

    virtual void        _dump(ostream &) const; // to be over-ridden

    // these traverse all threads
    static void       dumpall(const char *, ostream &);
    static void       dumpall(ostream &);

    static void       dump_io(ostream &);
    static void       dump_event(ostream &);

    static void       dump_stats(ostream &);
    static void       reset_stats();

    static void      find_stack(void *address);
    static void      for_each_thread(ThreadFunc& f);

    /* request stack overflow check, die on error. */
    static void      check_all_stacks(const char *file = "",
                             int line = 0);
    bool             isStackOK(const char *file = "", int line = 0) const;

    /* Recursion, etc stack depth estimator */
    bool             isStackFrameOK(size_t size = 0);

    w_rc_t           set_priority(priority_t priority);
    priority_t       priority() const;
    status_t         status() const;

private:

// WITHOUT_MMAP is controlled by configure
#ifdef WITHOUT_MMAP
    static w_rc_t     set_bufsize_memalign(size_t size,
                        char *&buf_start /* in/out*/, long system_page_size);
#endif
#ifdef HAVE_HUGETLBFS
public:
    // Must be called if we are configured with  hugetlbfs
    static w_rc_t     set_hugetlbfs_path(const char *path);
private:
    static w_rc_t     set_bufsize_huge(size_t size,
                        char *&buf_start /* in/out*/, long system_page_size);
#endif
    static w_rc_t     set_bufsize_normal(size_t size,
                        char *&buf_start /* in/out*/, long system_page_size);
    static void       align_bufsize(size_t size, long system_page_size,
                                                long max_page_size);
    static long       get_max_page_size(long system_page_size);
    static void       align_for_sm(size_t requested_size);

public:
    static int          do_unmap();
    /*
     *  Concurrent I/O ops
     */
    static char*        set_bufsize(size_t size);
    static w_rc_t       set_bufsize(size_t size, char *&buf_start /* in/out*/,
                                    bool use_normal_if_huge_fails=false);

    static w_rc_t        open(
                            const char*            path,
                            int                flags,
                            int                mode,
                            int&                fd);
    static w_rc_t        close(int fd);
    static w_rc_t        read(
                            int                 fd,
                            void*                 buf,
                            int                 n);
    static w_rc_t        write(
                            int                 fd,
                            const void*             buf,
                            int                 n);
    static w_rc_t        readv(
                            int                 fd,
                            const iovec_t*             iov,
                            size_t                iovcnt);
    static w_rc_t        writev(
                            int                 fd,
                            const iovec_t*                iov,
                            size_t                 iovcnt);

    static w_rc_t        pread(int fd, void *buf, int n, fileoff_t pos);
    static w_rc_t        pread_short(int fd, void *buf, int n, fileoff_t pos,
                            int& done);
    static w_rc_t        pwrite(int fd, const void *buf, int n,
                           fileoff_t pos);
    static w_rc_t        lseek(
                            int                fd,
                            fileoff_t            offset,
                            int                whence,
                            fileoff_t&            ret);
    /* returns an error if the seek doesn't match its destination */
    static w_rc_t        lseek(
                            int                fd,
                            fileoff_t                offset,
                            int                whence);
    static w_rc_t        fsync(int fd);
    static w_rc_t        ftruncate(int fd, fileoff_t sz);
    static w_rc_t        frename(int fd, const char* o, const char* n);
    static w_rc_t        fstat(int fd, filestat_t &sb);
    static w_rc_t        fisraw(int fd, bool &raw);


    /*
     *  Misc
     */
private:
    // NOTE: this returns a REFERENCE to a pointer
    /* #\fn static sthread_t*& sthread_t::me_lval()
     ** \brief Returns a (writable) reference to the a
     * pointer to the running sthread_t.
     * \ingroup TLS
     */
    inline static sthread_t*& me_lval() {
        /**\var sthread_t* _me;
         * \brief A pointer to the running sthread_t.
         * \ingroup TLS
         */
        static __thread sthread_t* _TLSme(NULL);
        return _TLSme;
    }
public:
    // NOTE: this returns a POINTER
    static sthread_t*    me() { return me_lval(); }
                         // for debugging:
    pthread_t            myself(); // pthread_t associated with this
    static int           rand(); // returns an int in [0, 2**31)
    static double        drand(); // returns a double in [0.0, 1)
    static int           randn(int max); // returns an int in [0, max)

    /* XXX  sleep, fork, and wait exit overlap the unix version. */

    // sleep for timeout milliseconds
    void                 sleep(timeout_in_ms timeout = WAIT_IMMEDIATE,
                         const char *reason = 0);
    void                 wakeup();

    // wait for a thread to finish running
    w_rc_t            join(timeout_in_ms timeout = WAIT_FOREVER);

    // start a thread
    w_rc_t            fork();

    // give up the processor
    static void        yield();
    ostream            &print(ostream &) const;

    // anyone can wait and delete a thread
    virtual            ~sthread_t();

    // function to do runtime up-cast to smthread_t
    // return 0 if the sthread is not derrived from sm_thread_t.
    // should be removed when RTTI is supported
    virtual smthread_t*        dynamic_cast_to_smthread();
    virtual const smthread_t*  dynamic_cast_to_const_smthread() const;

protected:
    sthread_t(
          priority_t    priority = t_regular,
          const char    *name = 0,
          unsigned        stack_size = default_stack);

    virtual void        before_run() { }
    virtual void        run() = 0;
    virtual void        after_run() { }

private:

    /* start offset of sthread FDs, to differentiate from system FDs */
    enum { fd_base = 4000 };
    void *                      _start_frame;
    void *                      _danger;
    size_t                      _stack_size;

    pthread_mutex_t             _wait_lock; // paired with _wait_cond, also
                                // protects _link
    pthread_cond_t              _wait_cond; // posted when thread should unblock

    pthread_mutex_t*            _start_terminate_lock; // _start_cond, _terminate_cond, _forked
    pthread_cond_t *            _start_cond; // paired w/ _start_terminate_lock

    volatile bool               _sleeping;
    volatile bool               _forked;
    bool                        _terminated; // protects against double calls
                                // to sthread_core_exit
    volatile bool               _unblock_flag; // used internally by _block()

    fill4                       _dummy4valgrind;

    sthread_core_t *            _core;        // registers, stack, etc
    volatile status_t           _status;    // thread status
    priority_t                  _priority;     // thread priority
    w_error_codes           _rce;        // used in block/unblock

    w_link_t                    _link;        // protected by _wait_lock

    w_link_t                    _class_link;    // used in _class_list,
                                 // protected by _class_list_lock
    static sthread_list_t*      _class_list;
    static queue_based_lock_t   _class_list_lock; // for protecting _class_list


    /* XXX alignment probs in derived thread classes.  Sigh */
    // fill4                       _ex_fill;

    static sdisk_t* get_disk(int& fd);

    /* I/O subsystem */
    static    std::vector<sdisk_t*> _disks;
    static    unsigned       open_max;
    static    unsigned       open_count;

    /* in-thread startup and shutdown */
    static void            __start(void *arg_thread);
    void                   _start();


    /* system initialization and shutdown */
    static w_rc_t        cold_startup();
    static w_rc_t        shutdown();
    static stime_t        boot_time;
    static sthread_t*    _main_thread;
    static uint32_t        _next_id;    // unique id generator

private:
    static int           _disk_buffer_disalignment;
    static size_t        _disk_buffer_size;
    static char *        _disk_buffer;
public:
    // export so smthread can read it and so latch/srwlock can write it:
    sthread_stats        SthreadStats;
};

extern ostream &operator<<(ostream &o, const sthread_t &t);

void print_timeout(ostream& o, const sthread_base_t::timeout_in_ms timeout);


/**\cond skip */
/**\brief The main thread.
*
* Called from sthread_t::cold_startup(), which is
* called from sthread_init_t::do_init(), which is
* called from sthread_t::initialize_sthreads_package(), which is called
* when the storage manager sets up options, among other places.
*/
class sthread_main_t : public sthread_t  {
    friend class sthread_t;

protected:
    NORET            sthread_main_t();
    virtual void        run();
};
/**\endcond skip */


/**\cond skip */

#define MUTEX_ACQUIRE(mutex)    W_COERCE((mutex).acquire());
#define MUTEX_RELEASE(mutex)    (mutex).release();
#define MUTEX_IS_MINE(mutex)    (mutex).is_mine()

// critical_section.h contains the macros needed for the following
// SPECIALIZE_CS
#include "critical_section.h"

// tatas_lock doesn't have is_mine, but I changed its release()
// to Release and through compiling saw everywhere that uses release,
// and fixed those places
SPECIALIZE_CS(tatas_lock, int _dummy, (_dummy=0),
    _mutex->acquire(), _mutex->release());

// queue_based_lock_t asserts is_mine() in release()
SPECIALIZE_CS(w_pthread_lock_t, w_pthread_lock_t::ext_qnode _me, (_me._held=0),
    _mutex->acquire(&_me), _mutex->release(&_me));
#ifndef USE_PTHREAD_MUTEX
SPECIALIZE_CS(mcs_lock, mcs_lock::ext_qnode _me, (_me._held=0),
    _mutex->acquire(&_me), _mutex->release(&_me));
#endif

SPECIALIZE_CS(occ_rwlock::occ_rlock, int _dummy, (_dummy=0),
    _mutex->acquire(), _mutex->release());

SPECIALIZE_CS(occ_rwlock::occ_wlock, int _dummy, (_dummy=0),
    _mutex->acquire(), _mutex->release());

inline sthread_t::priority_t
sthread_t::priority() const
{
    return _priority;
}

inline sthread_t::status_t
sthread_t::status() const
{
    return _status;
}

#include <w_strstream.h>
// Need string.h to get strerror_r
#include <string.h>
/**\endcond skip */


/*<std-footer incl-file-exclusion='STHREAD_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
