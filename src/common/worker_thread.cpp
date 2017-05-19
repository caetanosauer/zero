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

void worker_thread_t::wakeup(bool wait, int rounds_to_wait)
{
    long this_round = 0;
    {
        std::unique_lock<std::mutex> lck(cond_mutex);

        if (wait) {
            // Capture current round number before wakeup
            this_round = rounds_completed + rounds_to_wait;
            if (worker_busy) { this_round++; }
        }

        // Send wake-up signal
        wakeup_requested = true;
        wakeup_condvar.notify_one();
    }

    if (wait) {
        wait_for_round(this_round);
    }
}

void worker_thread_t::wait_for_round(long round)
{
    std::unique_lock<std::mutex> lck(cond_mutex);

    if (round == 0) { round = rounds_completed + 1; }

    auto predicate = [this, round]
    {
        return should_exit() || rounds_completed >= round;
    };

    done_condvar.wait(lck, predicate);
}

void worker_thread_t::stop()
{
    stop_requested = true;
    wakeup();
    join();
}

void worker_thread_t::quit()
{
    stop_requested = true;
}

void worker_thread_t::run()
{
    auto predicate = [this] { return wakeup_requested || should_exit(); };
    auto timeout = std::chrono::milliseconds(interval_msec);

    while (true) {
        if (stop_requested) { break; }

        {
            std::unique_lock<std::mutex> lck(cond_mutex);
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
            std::lock_guard<std::mutex> lck(cond_mutex);
            rounds_completed++;
            worker_busy = false;
            done_condvar.notify_all();
        }
    }
}

void worker_thread_t::notify_one()
{
    std::lock_guard<std::mutex> lck(cond_mutex);
    done_condvar.notify_one();
}

void worker_thread_t::notify_all()
{
    std::lock_guard<std::mutex> lck(cond_mutex);
    done_condvar.notify_all();
}

void log_worker_thread_t::wakeup_until_lsn(lsn_t lsn, bool wait, int rounds_to_wait)
{
    // Only change endLSN if it's increasing
    while (true) {
        lsn_t curr = endLSN;
        if (lsn < curr) { break; }
        if (endLSN.compare_exchange_strong(curr, lsn))
        {
            break;
        }
    }

    // Now send wakeup signal. No need to set LSN while holding mutex.
    wakeup(wait, rounds_to_wait);
}
