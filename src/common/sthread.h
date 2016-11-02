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
#include "latches.h"

class sthread_t;
class smthread_t;
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
          const char    *name = 0,
          unsigned        stack_size = default_stack);

    virtual void        before_run() { }
    virtual void        run() = 0;
    virtual void        after_run() { }

private:

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
    w_error_codes           _rce;        // used in block/unblock

    w_link_t                    _link;        // protected by _wait_lock

    w_link_t                    _class_link;    // used in _class_list,
                                 // protected by _class_list_lock
    static sthread_list_t*      _class_list;
    static queue_based_lock_t   _class_list_lock; // for protecting _class_list


    /* in-thread startup and shutdown */
    static void            __start(void *arg_thread);
    void                   _start();


    /* system initialization and shutdown */
    static w_rc_t        cold_startup();
    static w_rc_t        shutdown();
    static stime_t        boot_time;
    static sthread_t*    _main_thread;
    static uint32_t        _next_id;    // unique id generator
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
