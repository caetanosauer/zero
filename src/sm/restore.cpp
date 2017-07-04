#include "w_defines.h"

#define SM_SOURCE

#include "restore.h"
#include "logarchiver.h"
#include "log_core.h"
#include "bf_tree.h"
#include "vol.h"
#include "sm_options.h"
#include "xct_logger.h"

#include <algorithm>
#include <random>

#include "stopwatch.h"

void SegmentRestorer::bf_restore(unsigned segment_begin, unsigned segment_end,
        size_t segment_size, bool virgin_pages, lsn_t begin_lsn, lsn_t end_lsn)
{
    PageID first_pid = segment_begin * segment_size;
    PageID total_pages = (segment_end - segment_begin) * segment_size;
    if (smlevel_0::bf->is_media_failure(first_pid)) {
        auto count = std::min(total_pages,
                smlevel_0::bf->get_media_failure_pid() - first_pid);
        smlevel_0::bf->prefetch_pages(first_pid, count);
    }

    for (unsigned s = segment_begin; s < segment_end; s++) {
        first_pid = s * segment_size;
        GenericPageIterator pbegin {first_pid, segment_size, virgin_pages};
        GenericPageIterator pend;

        // CS TODO there seems to be a weird memory leak in ArchiveScan
        static thread_local ArchiveScan archive_scan{smlevel_0::logArchiver->getIndex()};
        // ArchiveScan archive_scan{smlevel_0::logArchiver->getIndex()};
        archive_scan.open(first_pid, 0, begin_lsn, end_lsn);
        auto logiter = &archive_scan;

        if (logiter) {
            LogReplayer::replay(logiter, pbegin, pend);
        }

        // CS TODO:  use boolean template parameter to tell whether to log or not
        Logger::log_sys<restore_segment_log>(s);
    }
}

template <class LogScan, class PageIter>
void LogReplayer::replay(LogScan logs, PageIter& pagesBegin, PageIter pagesEnd)
{
    fixable_page_h fixable;
    auto page = pagesBegin;
    logrec_t* lr;

    lsn_t prev_lsn = lsn_t::null;
    PageID prev_pid = 0;
    unsigned replayed = 0;

    w_assert1(page != pagesEnd);

    while (logs->next(lr)) {
        auto pid = lr->pid();
        w_assert0(pid > prev_pid || (pid == prev_pid && lr->lsn() > prev_lsn));

        while (page != pagesEnd && page.current_pid() < pid) {
            ++page;
        }
        if (page == pagesEnd) { break; }

        if (page.current_pid() > pid) {
            /*
             * This icky situation occurs because of how our restore mechanism
             * interacts with page coupling (see comments in
             * GenericPageIterator::fix_current).  It can only happen if the
             * parent page has been fixed with a hit in SH mode (with latch
             * coupling) and the fix  of the child is a miss which incurs
             * restoration of its segment, which happens to be the same segment
             * of the parent.  In that case, we are not able to fix the parent
             * page for restore, but because it is already fixed, it does not
             * require any recovery (i.e., it already is in its most recent
             * consistent state). Thus, we just skip to the next page here.
             */
            continue;
        }

        auto p = *page;

        fixable.setup_for_restore(p);
        if (p->pid != pid) {
            p->pid = pid;
        }

        if (lr->lsn() > fixable.lsn()) {
            w_assert0(lr->page_prev_lsn() == lsn_t::null ||
                    lr->page_prev_lsn() == p->lsn || lr->has_page_img(pid));

            lr->redo(&fixable);
        }

        w_assert0(p->pid == pid);
        w_assert0(p->lsn != lsn_t::null);
        prev_pid = pid;
        prev_lsn = lr->lsn();

        ADD_TSTAT(restore_log_volume, lr->length());
        replayed++;
    }

    while (page != pagesEnd) {
        // make sure every page is unpinned
        ++page;
    }
}
