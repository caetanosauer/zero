#ifndef WORKER_THREAD_H
#define WORKER_THREAD_H

#include "smthread.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <atomic>

class worker_thread_t : public smthread_t {
public:
    worker_thread_t(int inverval_ms = -1);
    virtual ~worker_thread_t();

    /**
     * Wakes up the worker thread.
     * If wait = true, the call will block until the worker has performed
     * at least a full *do_work* round. This means that if the worker is
     * currently busy, we will wait for the current round to finish, start
     * a new round, and wait for that one too.
     */
    void wakeup(bool wait = false);

    /**
     * Request worker to stop on next do_work iteration and wait.
     */
    void stop();

    /**
     * Wait until the round number given has been completed.
     * If round == 0, wait for the current round to finish.
     */
    void wait_for_round(unsigned long round = 0);

    /**
     * Wait until the worker thread sends a notify (no predicate required).
     * This is useful in cases where we have many threads waiting and the
     * worker_thread executes a certain work in batches. In these cases, we
     * might want to wakeup the waiting threads gradually, not all of them at
     * once when the whole batch processing is done.
     * There might be spurious wake-up calls, and, since no predicate is
     * provided, it is up to the caller of the method to ensure that a certain
     * condition is met. USE WITH CAUTION!
     */
    void wait_for_notify();

    unsigned long get_rounds_completed() const { return rounds_completed; };
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
    unsigned long rounds_completed;
};

#endif


