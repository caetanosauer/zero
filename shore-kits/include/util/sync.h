/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
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

#ifndef __UTIL_SYNC_H
#define __UTIL_SYNC_H

#include "util/thread.h"

// exported functions

struct debug_mutex_t {
    pthread_mutex_t _lock;
    pthread_t _last_owner_tid;
    debug_mutex_t(pthread_mutex_t const &lock)
	: _lock(lock), _last_owner_tid(0)
    {
    }
    operator pthread_mutex_t&() {
	return _lock;
    }
};


/**
 *  @brief A critical section manager. Locks and unlocks the specified
 *  mutex upon construction and destruction respectively.
 */

struct critical_section_t {

    pthread_mutex_t* _mutex;

    critical_section_t(pthread_mutex_t &mutex)
        : _mutex(&mutex)
    {
        thread_mutex_lock(*_mutex);
    }

    critical_section_t(debug_mutex_t &mutex)
        : _mutex(&mutex._lock)
    {
        thread_mutex_lock(*_mutex);
	mutex._last_owner_tid = pthread_self();
    }
    

    void enter(pthread_mutex_t &mutex) {
        exit();
        _mutex = &mutex;
        thread_mutex_lock(*_mutex);
    }

    void enter(debug_mutex_t &mutex) {
        exit();
        _mutex = &mutex._lock;
        thread_mutex_lock(*_mutex);
	mutex._last_owner_tid = pthread_self();
    }

    void exit() {
        if(_mutex) {
            thread_mutex_unlock(*_mutex);
            _mutex = NULL;
        }
    }

    ~critical_section_t() {
        exit();
    }

private:
    critical_section_t(critical_section_t const &);
    critical_section_t &operator =(critical_section_t const &);
};



/**
 * @brief A simple "notifier" that allows a thread to block on pending
 * events. Multiple notifications that arrive with no waiting thread
 * will be treated as one event.
 */

struct notify_t {

    volatile bool _notified;
    volatile bool _cancelled;
    pthread_mutex_t _lock;
    pthread_cond_t _notify;

    notify_t()
        : _notified(false),
          _cancelled(false),
          _lock(thread_mutex_create()),
          _notify(thread_cond_create())
    {
    }


    /**
     * @brief Blocks the calling thread until either notify() or
     * cancel() is called. If either method was called before wait()
     * return immediately.
     *
     * @return zero if notified, non-zero if cancelled.
     */

    int wait() {
        critical_section_t cs(_lock);
        return wait_holding_lock();
    }


    /**
     * @brief Similar to wait(), except the caller is assumed to
     * already hold the lock for other reasons.
     */

    int wait_holding_lock() {
        while(!_notified && !_cancelled)
            thread_cond_wait(_notify, _lock);

        bool result = _cancelled;
        _notified = _cancelled = false;
        return result;
    }

    
    /**
     * @brief Wake up a waiting thread. If no thread is waiting the
     * event will be remembered. 
     */

    void notify() {
        critical_section_t cs(_lock);
        notify_holding_lock();
    }


    /**
     * @brief Wake up a waiting thread from within an existing
     * critical section.
     *
     * WARNING: the caller MUST hold (_lock) or the behavior of this
     * function is undefined!
     */

    void notify_holding_lock() {
        signal(_notified);
    }

    void cancel() {
        critical_section_t cs(_lock);
        cancel_holding_lock();
    }

    void cancel_holding_lock() {
        signal(_cancelled);
    }
    
protected:

    void signal(volatile bool &val) {
        val = true;
        thread_cond_signal(_notify);
    }
};



#endif
