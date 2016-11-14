#include "worker_thread.h"

worker_thread_t::worker_thread_t(int interval_ms)
    :
    interval_msec(interval_ms),
    stop_requested(false),
    wakeup_requested(false),
    worker_busy(false),
    rounds_completed(0)
{
}

worker_thread_t::~worker_thread_t()
{
}

void worker_thread_t::wakeup(bool wait)
{
    unsigned long this_round = 0;
    {
        unique_lock<mutex> lck(cond_mutex);

        if (wait) {
            // Capture current round number before wakeup
            this_round = rounds_completed + 1;
            if (worker_busy) { this_round++; }
        }

        // Send wake-up signal
        wakeup_requested = true;
        wakeup_condvar.notify_one();
    }

    if (wait) {
        // Wait for worker to finish one round
        wait_for_round(this_round);
    }
}

void worker_thread_t::wait_for_round(unsigned long round)
{
    unique_lock<mutex> lck(cond_mutex);

    if (round == 0) { round = rounds_completed + 1; }

    auto predicate = [this, round]
    {
        return rounds_completed >= round;
    };

    done_condvar.wait(lck, predicate);
}

void worker_thread_t::wait_for_notify()
{
    /* We have a timeout of 10ms. This is pretty high, but it is issued only to
     * avoid starvation in case the notify() signal is lost. In most cases we
     * should be awaken by the notify() call, not by the timeout. */
    unique_lock<mutex> lck(cond_mutex);
    done_condvar.wait_for(lck, std::chrono::milliseconds(10));
}

void worker_thread_t::stop()
{
    stop_requested = true;
    wakeup();
    join();
}

void worker_thread_t::run()
{
    auto predicate = [this] { return wakeup_requested; };
    auto timeout = chrono::milliseconds(interval_msec);

    while (true) {
        if (stop_requested) { break; }

        {
            unique_lock<mutex> lck(cond_mutex);
            if (interval_msec < 0) {
                // Only activate upon recieving a wakeup signal
                wakeup_condvar.wait(lck, predicate);
            }
            else if (interval_msec > 0) {
                // Activate on either signal or interval timeout; whatever
                // comes first
                wakeup_condvar.wait_for(lck, timeout, predicate);
            }

            if (stop_requested) { break; }
            wakeup_requested = false;
            worker_busy = true;
        }

        do_work();

        {
            // Notify waiting threads that we are done with this round
            lock_guard<mutex> lck(cond_mutex);
            rounds_completed++;
            worker_busy = false;
            done_condvar.notify_all();
        }
    }
}

void worker_thread_t::notify_one()
{
    done_condvar.notify_one();
}

void worker_thread_t::notify_all()
{
    done_condvar.notify_all();
}