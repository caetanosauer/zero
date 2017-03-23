#include "latches.h"

#include "smthread.h"

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


/************************************************************************************
 * mcs_rwlock implementation; cheaper but problematic when we get os preemptions
 */

// CC mangles this as __1cKmcs_rwlockOspin_on_writer6M_v_
// private
int mcs_rwlock::_spin_on_writer()
{
    int cnt=0;
    while(has_writer()) cnt=1;
    // callers do lintel::atomic_thread_fence(lintel::memory_order_acquire);
    return cnt;
}
// CC mangles this as __1cKmcs_rwlockPspin_on_readers6M_v_
// private
void mcs_rwlock::_spin_on_readers()
{
    while(has_reader()) { };
    // callers do lintel::atomic_thread_fence(lintel::memory_order_acquire);
}

// private
void mcs_rwlock::_add_when_writer_leaves(int delta)
{
    // we always have the parent lock to do this
    int cnt = _spin_on_writer();
    lintel::unsafe::atomic_fetch_add(const_cast<unsigned*>(&_holders), delta);
    // callers do lintel::atomic_thread_fence(lintel::memory_order_acquire);
    if(cnt  && (delta == WRITER)) {
        INC_TSTAT(rwlock_w_wait);
    }
}

bool mcs_rwlock::attempt_read()
{
    unsigned int old_value = *&_holders;
    if(old_value & WRITER ||
       !lintel::unsafe::atomic_compare_exchange_strong(const_cast<unsigned int*>(&_holders), &old_value, old_value+READER))
        return false;

    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    return true;
}

void mcs_rwlock::acquire_read()
{
    /* attempt to CAS first. If no writers around, or no intervening
     * add'l readers, we're done
     */
    if(!attempt_read()) {
        INC_TSTAT(rwlock_r_wait);
        /* There seem to be writers around, or other readers intervened in our
         * attempt_read() above.
         * Join the queue and wait for them to leave
         */
        {
            CRITICAL_SECTION(cs, (parent_lock*) this);
            _add_when_writer_leaves(READER);
        }
        lintel::atomic_thread_fence(lintel::memory_order_acquire);
    }
}

void mcs_rwlock::release_read()
{
    w_assert2(has_reader());
    lintel::atomic_thread_fence(lintel::memory_order_release); // flush protected modified data before releasing lock;
    // update and complete any loads by others before I do this write
    lintel::unsafe::atomic_fetch_sub(const_cast<unsigned*>(&_holders), READER);
}

bool mcs_rwlock::_attempt_write(unsigned int expected)
{
    /* succeeds iff we are the only reader (if expected==READER)
     * or if there are no readers or writers (if expected==0)
     *
     * How do we know if the only reader is us?
     * A:  we rely on these facts:
     * this is called with expected==READER only from attempt_upgrade(),
     *   which is called from latch only in the case
     *   in which we hold the latch in LATCH_SH mode and
     *   are requesting it in LATCH_EX mode.
     *
     * If there is a writer waiting we have to get in line
     * like everyone else.
     * No need for a memfence because we already hold the latch
     */

// USE_PTHREAD_MUTEX is determined by configure option and
// thus defined in config/shore-config.h
#ifdef USE_PTHREAD_MUTEX
    ext_qnode me = QUEUE_EXT_QNODE_INITIALIZER;
#else
    ext_qnode me;
    QUEUE_EXT_QNODE_INITIALIZE(me);
#endif

    if(*&_holders != expected || !attempt(&me))
        return false;
    // at this point, we've called mcs_lock::attempt(&me), and
    // have acquired the parent/mcs lock
    // The following line replaces our reader bit with a writer bit.
    bool result = lintel::unsafe::atomic_compare_exchange_strong(const_cast<unsigned int*>(&_holders), &expected, WRITER);
    release(me); // parent/mcs lock
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    return result;
}

bool mcs_rwlock::attempt_write()
{
    if(!_attempt_write(0))
        return false;

    // moved to end of _attempt_write() lintel::atomic_thread_fence(lintel::memory_order_acquire);
    return true;
}

void mcs_rwlock::acquire_write()
{
    /* always join the queue first.
     *
     * 1. We don't want to race with other writers
     *
     * 2. We don't want to make readers deal with the gap between
     * us updating _holders and actually acquiring the MCS lock.
     */
    CRITICAL_SECTION(cs, (parent_lock*) this);
    _add_when_writer_leaves(WRITER);
    w_assert1(has_writer()); // me!

    // now wait for existing readers to clear out
    if(has_reader()) {
        INC_TSTAT(rwlock_w_wait);
        _spin_on_readers();
    }

    // done!
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
}

void mcs_rwlock::release_write() {
    lintel::atomic_thread_fence(lintel::memory_order_release);
    w_assert1(*&_holders == WRITER);
    *&_holders = 0;
}

bool mcs_rwlock::attempt_upgrade()
{
    w_assert1(has_reader());
    return _attempt_write(READER);
}

void mcs_rwlock::downgrade()
{
    lintel::atomic_thread_fence(lintel::memory_order_release);
    w_assert1(*&_holders == WRITER);
    *&_holders = READER;
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
}

