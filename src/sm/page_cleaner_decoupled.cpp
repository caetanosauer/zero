#include "page_cleaner_decoupled.h"

#include "logrec.h"
#include "fixable_page_h.h"
#include "bf_tree_cb.h"
#include "log_core.h"

page_cleaner_decoupled::page_cleaner_decoupled(
        bf_tree_m* _bufferpool,
        const sm_options& _options)
    :
    page_cleaner_base(_bufferpool, _options),
    workspace_empty(true)
{
}

page_cleaner_decoupled::~page_cleaner_decoupled()
{
}

void page_cleaner_decoupled::do_work()
{
    lsn_t last_lsn = smlevel_0::logArchiver->getDirectory()->getLastLSN();
    if(last_lsn <= _clean_lsn) {
        DBGTHRD(<< "Nothing archived to clean.");
        return;
    }

    DBGTHRD(<< "Cleaner thread activated from " << _clean_lsn);

    LogArchiver::ArchiveScanner logScan(smlevel_0::logArchiver->getDirectory());
    // CS TODO block size
    LogArchiver::ArchiveScanner::RunMerger* merger = logScan.open(0, 0,
            _clean_lsn, 1048576);

    generic_page* page = NULL;
    logrec_t* lr;
    while (merger != NULL && merger->next(lr)) {

        PageID lrpid = lr->pid();

        if (!lr->is_redo()) {
            continue;
        }

        if(page != NULL && page->pid != lrpid) {
            page->checksum = page->calculate_checksum();
        }

        if(!workspace_empty
                && _workspace[_workspace_size-1].pid != 0
                && _workspace[_workspace_size-1].pid < lrpid) {
            w_assert0(_workspace[_workspace_size-1].pid >= page->pid);
            fill_cb_indexes();
            flush_workspace(0, _workspace_size);

            memset(&_workspace[0], '\0', _workspace_size * sizeof(generic_page));
            workspace_empty = true;
        }

        if(workspace_empty) {
            /* true for ignoreRestore */
            w_rc_t err = smlevel_0::vol->read_many_pages(lrpid, &(_workspace[0]),
                    _workspace_size, true);
            if(err.err_num() == eVOLFAILED) {
                DBGOUT(<<"Trying to clean pages, but device is failed. Cleaner deactivating.");
                break;
            }
            else if(err.err_num() != stSHORTIO) {
                W_COERCE(err);
            }
            workspace_empty = false;

            page = &_workspace[0];
        }
        else {
            PageID base_pid = _workspace[0].pid;
            page = &_workspace[lrpid - base_pid];
        }

        if(page->lsn >= lr->lsn_ck()) {
            DBGOUT(<<"Not replaying log record " << lr->lsn_ck() << ". Page " << page->pid << " is up-to-date.");
            continue;
        }

        page->pid = lrpid;

        fixable_page_h fixable;
        fixable.setup_for_restore(page);
        lr->redo(&fixable);

        DBGOUT(<<"Replayed log record " << lr->lsn_ck() << " for page " << page->pid);
    }

    if(!workspace_empty) {
        page->checksum = page->calculate_checksum();
        fill_cb_indexes();
        flush_workspace(0, _workspace_size);

        memset(&_workspace[0], '\0', _workspace_size * sizeof(generic_page));
        workspace_empty = true;
    }
    _clean_lsn = last_lsn;
    DBGTHRD(<< "Cleaner thread deactivating. Cleaned until " << _clean_lsn);
}

void page_cleaner_decoupled::fill_cb_indexes()
{
    for(size_t i = 0; i < _workspace_size; i++)
    {
        bf_idx idx = _bufferpool->lookup(_workspace[i].pid);
        _workspace_cb_indexes[i] = idx;
    }
}
