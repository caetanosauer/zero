#include "page_cleaner_decoupled.h"

#include "logrec.h"
#include "fixable_page_h.h"
#include "bf_tree_cb.h"
#include "log_core.h"
#include "eventlog.h"

page_cleaner_decoupled::page_cleaner_decoupled(
        bf_tree_m* _bufferpool, const sm_options& _options)
    : page_cleaner_base(_bufferpool, _options)
{
    // CS TODO: clean_lsn must be recovered from checkpoint
    _clean_lsn = smlevel_0::log->durable_lsn();
}

page_cleaner_decoupled::~page_cleaner_decoupled()
{
}

void page_cleaner_decoupled::do_work()
{
    lsn_t last_lsn = smlevel_0::logArchiver->getDirectory()->getLastLSN();
    if(last_lsn <= _clean_lsn) {
        ERROUT(<< "Nothing archived to clean.");
        return;
    }

    ERROUT(<< "Cleaner thread activated from " << _clean_lsn);

    LogArchiver::ArchiveScanner logScan(smlevel_0::logArchiver->getDirectory());
    // CS TODO block size
    LogArchiver::ArchiveScanner::RunMerger* merger = logScan.open(0, 0,
            _clean_lsn, 1048576);

    generic_page* page = nullptr;
    PageID currentPid = 0, firstPid = 0;
    logrec_t* lr;
    while (merger && merger->next(lr)) {
        if (!lr->is_redo()) {
            continue;
        }

        PageID lrpid = lr->pid();
        if (!page || lrpid - firstPid >= _workspace_size) {
            // first iteration of the loop or time to read new buffer
            if (page) {
                fill_cb_indexes();
                flush_workspace(0, _workspace_size);
                sysevent::log_page_write(firstPid, last_lsn, _workspace_size);
            }
            currentPid = lrpid;
            firstPid = (lrpid / _workspace_size) * _workspace_size;
            W_COERCE(smlevel_0::vol->read_many_pages(firstPid, &(_workspace[0]), _workspace_size));
            page = &_workspace[lrpid - firstPid];
        }

        if (lrpid > currentPid) {
            // move to next page
            w_assert1(page);
            page->checksum = page->calculate_checksum();
            page += lrpid - currentPid;
            currentPid = lrpid;
        }

        if(page->lsn >= lr->lsn()) {
            DBGOUT(<<"Not replaying log record " << lr->lsn()
                    << ". Page " << page->pid << " is up-to-date.");
            continue;
        }

        // CS TODO setting the pid is required to redo a split on the new foster child
        page->pid = lrpid;
        fixable_page_h fixable;
        fixable.setup_for_restore(page);
        lr->redo(&fixable);

        DBGOUT(<<"Replayed log record " << lr->lsn_ck() << " for page " << page->pid);
    }

    if (merger) { delete merger; }

    if(page && currentPid - firstPid > 0) {
        page->checksum = page->calculate_checksum();
        fill_cb_indexes();
        flush_workspace(0, _workspace_size);
        sysevent::log_page_write(firstPid, last_lsn, _workspace_size);
    }

    DBGTHRD(<< "Cleaner thread deactivating. Cleaned until " << _clean_lsn);
    _clean_lsn = last_lsn;
}

void page_cleaner_decoupled::fill_cb_indexes()
{
    for(size_t i = 0; i < _workspace_size; i++)
    {
        bf_idx idx = _bufferpool->lookup(_workspace[i].pid);
        _workspace_cb_indexes[i] = idx;
    }
}
