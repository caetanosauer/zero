#include "page_cleaner.h"

#include "log_core.h"
#include "bf_tree.h"
#include "generic_page.h"

page_cleaner_base::page_cleaner_base(bf_tree_m* bufferpool, const sm_options& _options)
    :
    _bufferpool(bufferpool),
    _clean_lsn(lsn_t(1,0)),
    _stop_requested(false),
    _wakeup_requested(false),
    _cleaner_busy(false),
    _rounds_completed(0)
{
    _interval_msec = _options.get_int_option("sm_cleaner_interval_millisec", 1000);
    _workspace_size = (uint32_t) _options.get_int_option("sm_cleaner_workspace_size", 128);

    _workspace.resize(_workspace_size);
    _workspace_cb_indexes.resize(_workspace_size, 0);
}

page_cleaner_base::~page_cleaner_base()
{
}

void page_cleaner_base::wakeup(bool wait)
{
    unsigned long wait_for_round = 0;
    {
        unique_lock<mutex> lck(_cond_mutex);

        if (wait) {
            // Capture current round number before wakeup
            wait_for_round = _rounds_completed + 1;
            if (_cleaner_busy) { wait_for_round++; }
        }

        // Send wake-up signal
        _wakeup_requested = true;
        _wakeup_condvar.notify_one();
    }

    if (wait) {
        // Wait for cleaner to finish one round
        unique_lock<mutex> lck(_cond_mutex);
        auto predicate = [this, wait_for_round]
        {
            return _rounds_completed >= wait_for_round;
        };

        _done_condvar.wait(lck, predicate);
    }
}

void page_cleaner_base::shutdown()
{
    _stop_requested = true;
    wakeup();
    join();
}

void page_cleaner_base::run()
{
    auto predicate = [this] { return _wakeup_requested; };
    auto timeout = chrono::milliseconds(_interval_msec);

    while (true) {
        {
            unique_lock<mutex> lck(_cond_mutex);
            if (_interval_msec < 0) {
                // Only activate upon recieving a wakeup signal
                _wakeup_condvar.wait(lck, predicate);
            }
            else if (_interval_msec > 0) {
                // Activate on either signal or interval timeout; whatever
                // comes first
                _wakeup_condvar.wait_for(lck, timeout, predicate);
            }

            if (_stop_requested) { break; }
            _wakeup_requested = false;
            _cleaner_busy = true;
        }

        do_work();

        {
            // Notify waiting threads that we are done with this round
            lock_guard<mutex> lck(_cond_mutex);
            _rounds_completed++;
            _cleaner_busy = false;
            _done_condvar.notify_all();
        }
    }
}

void page_cleaner_base::flush_workspace(size_t from, size_t to)
{
    if (from - to == 0) {
        return;
    }

    // Flush log to guarantee WAL property
    W_COERCE(smlevel_0::log->flush(_clean_lsn));

    W_COERCE(smlevel_0::vol->write_many_pages(
                _workspace[from].pid, &(_workspace[from]), to - from));

    for (size_t i = from; i < to; ++i) {
        bf_idx idx = _workspace_cb_indexes[i];
        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);

        // Assertion below may fail for decoupled cleaner, and it's OK
        // w_assert1(i == from || _workspace[i].pid == _workspace[i - 1].pid + 1);

        cb.pin();
        if (cb._pid == _workspace[i].pid && cb.get_clean_lsn() < _clean_lsn) {
            cb.set_clean_lsn(_clean_lsn);
        }
        cb.unpin();
    }
}
