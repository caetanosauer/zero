#include "page_cleaner.h"

page_cleaner::page_cleaner(vol_t* _volume, LogArchiver::ArchiveDirectory* _archive)
: volume(_volume), archive(_archive) {

}

page_cleaner::~page_cleaner() {

}

void page_cleaner::run() {
    lpid_t first_page = lpid_t(volume->vid(), volume->first_data_pageid());

    LogArchiver::ArchiveScanner logScan(archive);
    LogArchiver::ArchiveScanner::RunMerger* merger = logScan.open(first_page, lpid_t::null, lsn_t::null);

    generic_page page;
    fixable_page_h fixable;
    logrec_t* lr;
    while (merger->next(lr)) {
        lpid_t lrpid = lr->construct_pid();
        if(page.pid != lrpid) { // getting log records for a new page
            if(page.pid != lpid_t::null) {
                write_buffer.push_back(page);
            }

            if(lrpid.page != (page.pid.page+1)) {  // new page is not contiguous
                flush_write_buffer();
            }

            volume->read_page(lr->shpid(), page);
        }
        w_assert0(page.pid == lrpid);
        DBGOUT(<<"Replaying log record for page " << page.pid);

        if (!fixable.is_fixed() || fixable.pid().page != lrpid.page) {
            fixable.setup_for_restore(&page);
        }

        if (lr->lsn_ck() <= page.lsn) {
            // update may already be reflected on page
            continue;
        }

        lr->redo(&fixable);
        fixable.update_initial_and_last_lsn(lr->lsn_ck());
        fixable.update_clsn(lr->lsn_ck());
    }
}

void page_cleaner::flush_write_buffer() {
    shpid_t first_pid = write_buffer.front().pid.page;
    DBGOUT1(<<"Flushing write buffer from page "<<first_pid << " to page " << first_pid + write_buffer.size() - 1);
    W_COERCE(volume->write_many_pages(first_pid, &write_buffer[0], write_buffer.size()));
    write_buffer.clear();
}