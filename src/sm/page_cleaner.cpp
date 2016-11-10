#include "page_cleaner.h"

#include "log_core.h"
#include "bf_tree.h"
#include "generic_page.h"

page_cleaner_base::page_cleaner_base(bf_tree_m* bufferpool, const sm_options& _options)
    :
    worker_thread_t(_options.get_int_option("sm_cleaner_interval_millisec", 1000)),
    _bufferpool(bufferpool),
    _clean_lsn(lsn_t(1,0))
{
    _workspace_size = (uint32_t) _options.get_int_option("sm_cleaner_workspace_size", 128);

    _workspace.resize(_workspace_size);
    _workspace_cb_indexes.resize(_workspace_size, 0);
}

page_cleaner_base::~page_cleaner_base()
{
}

void page_cleaner_base::flush_workspace(size_t from, size_t to)
{
    if (from - to == 0) {
        return;
    }

    // Flush log to guarantee WAL property
    W_COERCE(smlevel_0::log->flush(_clean_lsn));

    W_COERCE(smlevel_0::vol->write_many_pages(
                _workspace[from].pid, &(_workspace[from]), to - from,
                true /* ignore restore */));

    for (size_t i = from; i < to; ++i) {
        bf_idx idx = _workspace_cb_indexes[i];
        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);

        // Assertion below may fail for decoupled cleaner, and it's OK
        // w_assert1(i == from || _workspace[i].pid == _workspace[i - 1].pid + 1);

        rc_t rc = cb.latch().latch_acquire(LATCH_EX, sthread_t::WAIT_IMMEDIATE);
        if (rc.is_error()) {
            continue;   // Could not latch page in EX mode -- just skip it
        }

        cb.pin();
        if (cb._pid == _workspace[i].pid && cb.get_clean_lsn() < _clean_lsn) {
            cb.set_clean_lsn(_clean_lsn);
        }
        cb.unpin();

        cb.latch().latch_release();
    }
}
