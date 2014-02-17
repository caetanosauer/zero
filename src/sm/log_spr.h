#ifndef SM_LOG_SPR_H
#define SM_LOG_SPR_H
/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#include "basics.h"
#include "lsn.h"

/**
 * \defgroup SPR  Single-Page Recovery
 * \brief \b Single-Page-Recovery (\b SPR) is a novel feature to recover a single page
 * without going through the entire restart procedure.
 * \ingroup SSMLOG
 * \details
 * \section Benefits Key Benefits
 * Suppose the situation where we have 999,999 pages that are intact and flushed
 * since the previous checkpoint. Now, while running our database, we updated one page,
 * flushed it to disk, evicted from bufferpool, then later re-read it from disk, finding that
 * the page gets corrupted.
 *
 * The time to recover the single page in traditional database would be hours, analyzing the
 * logs, applying REDOs and UNDOs.
 * SPR, on the other hand, finishes in milliseconds. It fetches a single page from the backup,
 * collects only relevant transactional logs for the page, and applies them to the page.
 *
 * \section Terminology Terminology
 * \li A \b page here refers to a B-tree page. It does NOT refer to
 * other types of pages (e.g., page-allocation bitmap pages, store-node pages).
 *
 * \li \b Page-LSN of page X, or PageLSN(X), refers to the log sequence number
 * of the latest update to the logical page X, regardless of whether the page is currently
 * in the buffer pool or its cleanliness.
 *
 * \li \b Recorded-Page-LSN of page X in data source Y, or RecordedPageLSN(X, Y), refers to
 * the log sequence number of the latest update to a particular image of page X stored in
 * data source Y (e.g., bufferpool, media, backup as of Wed, backup as of Thr, etc)
 *
 * \li \b Expected-Minimum-LSN of page X, or EMLSN(X), refers to the Page-LSN of X at the
 * time of its latest eviction from the buffer pool when the page X was dirty. EM-LSN of X
 * is stored in X's parent page, except in the case of the root page.
 *
 * \section Invariantes Invariantes
 * \li EMLSN(X) <= PageLSN(X)
 * \li EMLSN(X) = PageLSN(X) only when X had no updates since previous write-out.
 * \li EMLSN(X) > 0
 *
 * \section Algorithm Algorithm Overview
 * Single page recovery is invoked only when a page is first brought into the buffer pool.
 * When we bring a page, X, into the buffer pool, we compare RecordedPageLSN(X, media)
 * to EMLSN(X). If we see that that EMLSN(X) > RecordedPageLSN(X, media),
 * then we will invoke single page recovery.
 *
 * Given a parent page P with a child page C, EMLSN(C) is updated only when the child page C is
 * evicted from the buffer pool. This is in keeping with the invariant EMLSN(C) <= PageLSN(C).
 * Given a parent page P with a child page C, updating EMLSN(C) is a logged system transaction
 * that does change PageLSN(P).
 *
 * The buffer pool will evict only complete subtrees. Evicting a page from the buffer pool
 * means that all of that page's children must have already been evicted.
 * \li The buffer pool evicts pages bottom-up (leaf pages first, then parents,
 * then grandparents, etc).
 * \li Single page recovery works top-down --- a page cannot be recovered until all of its
 * ancestors have been recovered.
 * \li Conversely, bringing a page into the buffer pool requires that all of that page's
 * ancestors must also be in the buffer pool.
 *
 * Initially, the log records to be recovered will fit in memory. Ultimately, we intend to
 * create a recovery index (so that the log records won't have to fit into memory).
 *
 * \section LSN-CHAIN Per-Page LSN Chain
 * To maintain the per-page LSN chain mentioned above, we have baseLogHeader#_page_prv
 * in logrec_t and multi_page_log_t#_page2_prv.
 * These properties are populated in log manager when we call
 * xct_t#give_logbuf(). We use these
 * fields to collect relevant logs in log_core#_collect_single_page_recovery_logs().
 *
 * \section EMLSN-BTREE EMLSN in B-tree pages
 * We also have EMLSN fields for all page pointers in B-tree pages, which is required
 * to tell "from where we should start collecting relevant per-page logs" in case of SPR.
 * See the setters/getters of btree_page_h linked to this HTML.
 * We also run a system transaction to update the EMLSN whenever we evict the child page
 * from bufferpool. See page_evict_t and log_page_evict().
 *
 * \section References References
 * More details are given in the following papers.
 * \li G. Graefe et al., "Definition, Detection, and Recovery of Single-page Failures,
 *  a Fourth Class of Database Failures". VLDB'12.
 * \li G. Graefe et al., "Self-diagnosing and self-healing indexes". DBTest'12.
 * \li G. Graefe et al., "Foster B-trees". TODS'12.
 */

/**
 * \brief Log content of page_evict to maintain EMLSN in parent page.
 * \ingroup SPR
 * \details
 * This is the log of the system transaction to maintain EMLSN.
 * The log is generated whenever we evict a page from bufferpool to maintain EMLSN
 * in the parent page.
 * @see log_page_evict()
 */
struct page_evict_t {
    lsn_t                   _child_lsn;
    general_recordid_t      _child_slot;
    page_evict_t(const lsn_t &child_lsn, general_recordid_t child_slot)
        : _child_lsn (child_lsn), _child_slot(child_slot) {}
};
// trivial change
#endif // SM_LOG_SPR_H
