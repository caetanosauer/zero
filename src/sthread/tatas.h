#ifndef TATAS_H
#define TATAS_H

#include "atomic_templates.h"
#include "os_interface.h"

/**\brief A test-and-test-and-set spinlock. 
 *
 * This lock is good for short, uncontended critical sections. 
 * If contention is high, use an mcs_lock. 
 * Long critical sections should use pthread_mutex_t.
 *
 * Tradeoffs are:
 *  - test-and-test-and-set locks: low-overhead but not scalable
 *  - queue-based locks: higher overhead but scalable
 *  - pthread mutexes : very high overhead and blocks, but frees up 
 *  cpu for other threads when number of cpus is fewer than number of threads
 *
 *  \sa REFSYNC
 */
struct tatas_lock {
    /**\cond skip */
    enum { NOBODY=0 };
    typedef union  {
        pthread_t         handle;
#undef CASFUNC 
#if SIZEOF_PTHREAD_T==4
#define CASFUNC atomic_cas_32
        unsigned int       bits;
#elif SIZEOF_PTHREAD_T==8
# define CASFUNC atomic_cas_64
        uint64_t           bits;
#elif SIZEOF_PTHREAD_T==0
#error  Configuration could not determine size of pthread_t. Fix configure.ac.
#else 
#error  Configuration determined size of pthread_t is unexpected. Fix sthread.h.
#endif
    } holder_type_t;
    volatile holder_type_t _holder;
    /**\endcond skip */

    tatas_lock() { _holder.bits=NOBODY; }

private:
    // CC mangles this as __1cKtatas_lockEspin6M_v_
    /// spin until lock is free
    void spin() { while(*&(_holder.handle)) ; }

public:
    /// Try to acquire the lock immediately.
    bool try_lock() 
    {
        holder_type_t tid = { pthread_self() };
        bool success = false;
        unsigned int old_holder = 
                        CASFUNC(&_holder.bits, NOBODY, tid.bits);
        if(old_holder == NOBODY) {
            membar_enter();
            success = true;
        }
        
        return success;
    }

    /// Acquire the lock, spinning as long as necessary. 
    void acquire() {
        w_assert1(!is_mine());
        holder_type_t tid = { pthread_self() };
        do {
            spin();
        }
        while(CASFUNC(&_holder.bits, NOBODY, tid.bits));
        membar_enter();
        w_assert1(is_mine());
    }

    /// Release the lock
    void release() {
        membar_exit();
        w_assert1(is_mine()); // moved after the membar
        _holder.bits= NOBODY;
#if W_DEBUG_LEVEL > 0
        {
            membar_enter(); // needed for the assert?
            w_assert1(!is_mine());
        }
#endif
    }

    /// True if this thread is the lock holder
    bool is_mine() const { return 
        pthread_equal(_holder.handle, pthread_self()) ? true : false; }
#undef CASFUNC 
};
/** Scoped objects to automatically acquire tatas_lock. */
class tataslock_critical_section {
public:
    tataslock_critical_section(tatas_lock *lock) : _lock(lock) {
        _lock->acquire();
    }
    ~tataslock_critical_section() {
        _lock->release();
    }
private:
    tatas_lock *_lock;
};

#endif // TATAS_H
