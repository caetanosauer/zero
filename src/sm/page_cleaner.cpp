#include "page_cleaner.h"

#include "log_core.h"
#include "bf_tree.h"
#include "generic_page.h"

page_cleaner_base::page_cleaner_base(const sm_options& _options)
    :
    worker_thread_t(_options.get_int_option("sm_cleaner_interval", -1)),
    _clean_lsn(lsn_t(1,0))
{
    _bufferpool = smlevel_0::bf;

    _workspace_size = _options.get_int_option("sm_cleaner_workspace_size", 0);
    if (_workspace_size == 0) {
        // if 0 given, set workspace size to 1/128 of the buffer pool size
        auto bufpoolsize = _bufferpool->get_block_cnt();
        _workspace_size = bufpoolsize >> 7;
        if (_workspace_size == 0) { _workspace_size = 1; }
    }

    _write_elision = _options.get_bool_option("sm_write_elision", false);

    _workspace.resize(_workspace_size);
    _workspace_cb_indexes.resize(_workspace_size, 0);
}

page_cleaner_base::~page_cleaner_base()
{
}

void page_cleaner_base::write_pages(size_t from, size_t to)
{
    W_COERCE(smlevel_0::vol->write_many_pages(
                _workspace[from].pid, &(_workspace[from]), to - from));
    ADD_TSTAT(cleaned_pages, to - from);
}

void page_cleaner_base::mark_pages_clean(size_t from, size_t to)
{
    for (size_t i = from; i < to; ++i) {
        bf_idx idx = _workspace_cb_indexes[i];
        if (idx == 0) { continue; }

        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
        if (!cb.pin()) { continue; }

        if (cb._pid == _workspace[i].pid) {
            w_assert1(cb.is_in_use());
            cb.notify_write();
            // CS TODO: should do this only if policy == highest_refcount
            cb.reset_ref_count_ex();
        }

        cb.unpin();
    }
}
