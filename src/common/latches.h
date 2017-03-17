#ifndef LATCHES_H
#define LATCHES_H

#include "w_base.h"
#include "timeout.h"
#include "w_pthread.h"
#include "AtomicCounter.hpp"
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

typedef mcs_rwlock srwlock_t;

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

#endif

