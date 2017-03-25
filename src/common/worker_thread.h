#ifndef WORKER_THREAD_H
#define WORKER_THREAD_H

#include "thread_wrapper.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <atomic>

class worker_thread_t : public thread_wrapper_t {
public:
    worker_thread_t(int inverval_ms = -1);
    virtual ~worker_thread_t();

    /**
     * Wakes up the worker thread.
     * If wait = true, the call will block until the worker has sent a
     * notification on done_condvar AND the given number of rounds has passed.
     * If rounds_to_wait == -1, then we only wait for a notify.
     * If rounds_to_wait == 0, then we wait for the current round to finish if
     * the worker is busy; if the worker is not busy, then it behaves just like
     * the "-1" case.
     * If rounds_to_wait > 0, then we wait for at least one full round (which
     * means we actually wait for at least two round increments if the worker
     * is busy)
     */
    void wakeup(bool wait = false, int rounds_to_wait = -1);

    /**
     * Request worker to stop on next do_work iteration and wait.
     */
    void stop();

    /**
     * Wait until the round number given has been completed.
     * If round <= rounds_completed, just wait for a notify instead of
     * whole rounds.
     */
    void wait_for_round(long round = 0);

    long get_rounds_completed() const { return rounds_completed; };
    bool is_busy() const { return worker_busy; }

protected:

    /**
     * Actual working method to be implemented by derived classes.
     */
    virtual void do_work() = 0;

    /**
     * Used to check inside work method if thread should exit without
     * waiting for a complete do_work round
     */
    bool should_exit() const { return stop_requested; }

    /**
     * These methods can be called from inside the do_work() implementation to
     * wake up threads waiting at wait_for_notify().
     */
    void notify_one();
    void notify_all();

    void quit();

private:
    virtual void run();

    std::mutex cond_mutex;
    std::condition_variable wakeup_condvar;
    std::condition_variable done_condvar;

    /**
     * Interval at which do_work is invoked, in milliseconds.
     * If < 0: only invoked when explicitly woken up.
     * If = 0: no timeout or wakeup necessary -- run continuously
     * If > 0: wait for this timeout or a wakeup signal, whatever comes first.
     */
    int interval_msec;
    /** whether this thread has been requested to stop. */
    std::atomic<bool> stop_requested;
    /** whether this thread has been requested to wakeup. */
    bool wakeup_requested;
    /** whether this thread is currently busy (and not waiting for wakeup */
    bool worker_busy;
    /** number of do_work() rounds already completed by the worker */
    long rounds_completed;
};

/**
 * Specialization of worker_thread_t for threads that work on LSN ranges
 */
class log_worker_thread_t : public worker_thread_t
{
public:
    log_worker_thread_t(int interval_ms = -1)
        : worker_thread_t(interval_ms), endLSN(lsn_t::null)
    {}

    virtual ~log_worker_thread_t() {}

    void wakeup_until_lsn(lsn_t lsn, bool wait = false, int rounds_to_wait = -1);

    lsn_t getEndLSN() { return endLSN; }

private:
    std::atomic<lsn_t> endLSN;
};

#endif

