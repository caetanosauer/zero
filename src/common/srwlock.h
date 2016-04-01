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

 $Id: srwlock.h,v 1.4 2010/11/08 15:07:28 nhall Exp $

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

#ifndef SRWLOCK_H
#define SRWLOCK_H

#include "sthread.h"

class mcs_rwlock; // forward
typedef mcs_rwlock srwlock_t;


/**\brief Shore read-write lock:: many-reader/one-writer spin lock 
 *
 * This read-write lock is implemented around a queue-based lock. It is
 * the basis for latches in the storage manager.
 *
 * Use this to protect data structures that get constantly hammered by
 * short reads, and less frequently (but still often) by short writes.
 *
 * "Short" is the key word here, since this is spin-based.
 */
class mcs_rwlock : protected queue_based_lock_t 
{
    typedef queue_based_lock_t parent_lock;
    
    /* \todo  TODO: Add support for blocking if any of the spins takes too long.
     * 
       There are three spins to worry about: spin_on_writer,
       spin_on_reader, and spin_on_waiting

       The overall idea is that threads which decide to block lose
       their place in line to avoid forming convoys. To make this work
       we need to modify the spin_on_waiting so that it blocks
       eventually; the mcs_lock's preemption resistance will take care
       of booting it from the queue as necessary.

       Whenever the last reader leaves it signals a cond var; when a
       writer leaves it broadcasts.
       END TODO
     */
    unsigned int volatile _holders; // 2*readers + writer

public:
    enum rwmode_t { NONE=0, WRITER=0x1, READER=0x2 };
    mcs_rwlock() : _holders(0) { }
    ~mcs_rwlock() {}

    /// Return the mode in which this lock is held by anyone.
    rwmode_t mode() const { int holders = *&_holders; 
        return (holders == WRITER)? WRITER : (holders > 0) ? READER : NONE; }

    /// True if locked in any mode.
    bool is_locked() const { return (*&_holders)==0?false:true; }

    /// 1 if held in write mode, else it's the number of readers
    int num_holders() const { int holders = *&_holders; 
                              return (holders == WRITER)? 1 : holders/2; }

    /// True iff has one or more readers.
    bool has_reader() const { return *&_holders & ~WRITER; }
    /// True iff has a writer (never more than 1) 
    bool has_writer() const { return *&_holders & WRITER; }

    /// True if success.
    bool attempt_read();
    /// Wait (spin) until acquired.
    void acquire_read();
    /// This thread had better hold the lock in read mode.
    void release_read();

    /// True if success.
    bool attempt_write();
    /// Wait (spin) until acquired.
    void acquire_write();
    /// This thread had better hold the lock in write mode.
    void release_write();
    /// Try to upgrade from READ to WRITE mode. Fail if any other threads are waiting.
    bool attempt_upgrade();
    /// Atomically downgrade the lock from WRITE to READ mode.
    void downgrade();

private:
    // CC mangles this as __1cKmcs_rwlockO_spin_on_writer6M_v_
    int  _spin_on_writer();
    // CC mangles this as __1cKmcs_rwlockP_spin_on_readers6M_v_
    void _spin_on_readers();
    bool _attempt_write(unsigned int expected);
    void _add_when_writer_leaves(int delta);
};

/** Scoped objects to automatically acquire srwlock_t/mcs_rwlock. */
class spinlock_read_critical_section {
public:
    spinlock_read_critical_section(srwlock_t *lock) : _lock(lock) {
        _lock->acquire_read();
    }
    ~spinlock_read_critical_section() {
        _lock->release_read();
    }
private:
    srwlock_t *_lock;
};

class spinlock_write_critical_section {
public:
    spinlock_write_critical_section(srwlock_t *lock) : _lock(lock) {
        _lock->acquire_write();
    }
    ~spinlock_write_critical_section() {
        _lock->release_write();
    }
private:
    srwlock_t *_lock;
};

/*<std-footer incl-file-exclusion='STHREAD_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
