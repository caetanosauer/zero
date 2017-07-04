#include "page_cleaner_decoupled.h"

#include "logrec.h"
#include "fixable_page_h.h"
#include "bf_tree_cb.h"
#include "log_core.h"
#include "xct_logger.h"
#include "logarchive_scanner.h"

page_cleaner_decoupled::page_cleaner_decoupled(const sm_options& options)
    : page_cleaner_base(options)
{
    // CS TODO: clean_lsn must be recovered from checkpoint
    _clean_lsn = smlevel_0::log->durable_lsn();
    _write_elision = options.get_bool_option("sm_write_elision", false);
    _segment_size = options.get_int_option("sm_batch_segment_size", 64);

    if (_workspace_size % _segment_size != 0) {
        _workspace_size -= _workspace_size % _segment_size;
    }
    w_assert0(_workspace_size >= _segment_size);
}

page_cleaner_decoupled::~page_cleaner_decoupled()
{
}

void page_cleaner_decoupled::notify_archived_lsn(lsn_t)
{
    wakeup();
}

void page_cleaner_decoupled::do_work()
{
    auto arch_index = smlevel_0::logArchiver->getIndex();
    _last_lsn = arch_index->getLastLSN();
    if(_last_lsn <= _clean_lsn) { return; }

    ERROUT(<< "Decoupled cleaner thread activated from " << _clean_lsn
            << " to " << _last_lsn);
    static thread_local ArchiveScan archive_scan{arch_index};
    archive_scan.open(0, 0, _clean_lsn);
    auto merger = &archive_scan;

    segments.clear();
    generic_page* page = nullptr;
    PageID curr_pid = 0, segment_pid = 0;
    size_t w_index = 0;
    fixable_page_h fixable;
    logrec_t* lr;
    PageID lrpid;

    while (merger->next(lr)) {
        lrpid = lr->pid();

        if (!page || lrpid - segment_pid >= _segment_size) {
            // Time to read a new segment
            if (page && w_index == _workspace_size) {
                // And also time to write current workspace
                flush_segments();
                w_index = 0;
            }
            segment_pid = (lrpid / _segment_size) * _segment_size;
            segments.push_back(segment_pid);

            page = &_workspace[w_index];
            curr_pid = segment_pid;
            W_COERCE(smlevel_0::vol->read_many_pages(segment_pid, page, _segment_size));

            w_index += _segment_size;
            page->pid = segment_pid;
        }

        while (lrpid > curr_pid) {
            // move to next page
            w_assert1(page);
            page++;
            curr_pid++;
            page->pid = curr_pid;
        }

        if(page->lsn >= lr->lsn()) { continue; }

        w_assert0(lr->page_prev_lsn() == lsn_t::null ||
                lr->page_prev_lsn() == page->lsn || lr->has_page_img(lrpid));

        fixable.setup_for_restore(page);
        lr->redo(&fixable);
    }

    if(!segments.empty()) { flush_segments(); }

    // cleans up dirty page table and updates rec_lsn
    if (_write_elision) {
        // smlevel_0::vol->sync();
        w_assert0(smlevel_0::recovery);
        smlevel_0::recovery->notify_cleaned_lsn(_last_lsn);
    }

    DBGTHRD(<< "Cleaner thread deactivating. Cleaned until " << _clean_lsn);
    _clean_lsn = _last_lsn;
}

void page_cleaner_decoupled::flush_segments()
{
    if (segments.empty()) { return; }

    size_t w_index = 0;
    size_t adjacent = 0;
    for (size_t i = 0; i < segments.size(); i++) {
       if (i < segments.size() - 1 &&
               segments[i+1] == segments[i] + _segment_size)
       {
           adjacent++;
       }
       else {
           size_t flush_size = (adjacent+1) * _segment_size;
           W_COERCE(smlevel_0::vol->write_many_pages(segments[i-adjacent],
                       &_workspace[w_index], flush_size));
           ADD_TSTAT(cleaned_pages, flush_size);
           w_index += flush_size;
           adjacent = 0;
       }
    }

    if (!_write_elision) {
        smlevel_0::vol->sync();
        // mark clean
    }

    w_index = 0;
    adjacent = 0;
    for (size_t i = 0; i < segments.size(); i++) {
       if (i < segments.size() - 1 &&
               segments[i+1] == segments[i] + _segment_size)
       {
           adjacent++;
       }
       else {
           size_t flush_size = (adjacent+1) * _segment_size;
           Logger::log_sys<page_write_log>(segments[i-adjacent], _last_lsn,
                   flush_size);
           if (!_write_elision) {
               update_cb_clean(w_index, w_index + flush_size);
           }
           w_index += flush_size;
           adjacent = 0;
       }
    }

    segments.clear();
}

void page_cleaner_decoupled::update_cb_clean(size_t from, size_t to)
{
    for (size_t i = from; i < to; ++i) {
        bf_idx idx = _bufferpool->lookup(_workspace[i].pid);

        if (idx == 0) { continue; }

        bf_tree_cb_t &cb = _bufferpool->get_cb(idx);
        if (!cb.pin()) { continue; }

        if (cb._pid == _workspace[i].pid) {
            w_assert1(cb.is_in_use());
            cb.notify_write_logbased(_last_lsn);

            // CS TODO: should do this only if policy == highest_refcount
            cb.reset_ref_count_ex();
        }

        cb.unpin();
    }
}
