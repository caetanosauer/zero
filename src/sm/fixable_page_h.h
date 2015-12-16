/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef FIXABLE_PAGE_H_H
#define FIXABLE_PAGE_H_H

#include "bf_idx.h"
#include "generic_page.h"
#include "latch.h"
#include "sm_base.h"


/**
 * \brief Handle class for pages that may be fixed (i.e., paged in by the main buffer
 * manager, bf_tree_m)
 *
 * \details
 * Currently, only B-tree pages are fixable.
 *
 * Pages fixed using latching mode Q for the page being fixed are still subject to
 * modification by other threads at any time, including replacement by a different page;
 * the methods of this class are safe to use in this case (but may return errors such as
 * eLATCHQFAIL) but super or subclass methods may not be safe to use in this case unless
 * their description specifically says so or they are labeled "robust".
 */
class fixable_page_h : public generic_page_h {
public:
    static int force_Q_fixing; // converts S mode to Q when: 0=no (default), 1=root only, 2=all <<<>>>

    // ======================================================================
    //   BEGIN: Construction/destruction/assignment
    // ======================================================================

    /// Create handle not yet fixed to a page
    fixable_page_h() : generic_page_h(NULL), _bufferpool_managed(false), _mode(LATCH_NL) {}
    ~fixable_page_h() { unfix(); }

    /// assignment; steals the ownership of the page/latch p and unfixes old associated
    /// page if exists and is different
    fixable_page_h& operator=(fixable_page_h& p) {
        if (&p != this) {
            unfix();
            _pp                 = p._pp;
            _bufferpool_managed = p._bufferpool_managed;
            _mode               = p._mode;
            _Q_ticket           = p._Q_ticket;
            p._pp               = NULL;
            p._mode             = LATCH_NL;
        }
        return *this;
    }


    // ======================================================================
    //   BEGIN: [Un]fixing pages
    // ======================================================================

    /// Do we have an associated page?
    bool is_fixed() const { return _pp != 0; }

    /// Unassociate us with any page; releases latch we may have held on previously
    /// associated page.
    void unfix();

    /// Is this page really fixed in bufferpool or a psuedo-fix?
    bool is_bufferpool_managed() const { return _bufferpool_managed; }

    /**
     * Fixes a non-root page in the bufferpool.  This method receives the parent page and
     * efficiently fixes the page if the shpid (pointer) is already swizzled by the parent
     * page.  The optimization is transparent for most of the code because the shpid
     * stored in the parent page is automatically (and atomically) changed to a swizzled
     * pointer when permited by the latch mode.
     *
     * @param[in] parent       parent of the page to be fixed.  If you can't provide this, use
     *                         fix_direct() though it can't exploit pointer swizzling and
     *                         thus will be slower.
     * @param[in] vol          volume ID.
     * @param[in] shpid        ID of the page to fix (or bufferpool index when swizzled)
     * @param[in] mode         latch mode.  Can be Q, SH, or EX.
     * @param[in] conditional  whether the fix is conditional (returns immediately even if
     *                         failed).
     * @param[in] virgin_page  whether the page is a new page and thus doesn't have to be
     *                         read from disk.
     *
     * If parent.latch_mode() or mode is LATCH_Q, can return eLATCHQFAIL,
     * ePARENTLATCHQFAIL, or eNEEDREALLATCH.  The later occurs only when virgin_page is
     * true or shpid is not swizzled.
     */
    w_rc_t fix_nonroot(const fixable_page_h &parent,
                       PageID pid, latch_mode_t mode, bool conditional=false,
                       bool virgin_page=false);

    /**
     * Only used in the REDO phase of Recovery process
     * The physical page has been loaded into buffer pooland the idx is known
     * when calling this function.
     * We associate the page in buffer pool with fixable_page.
     * In this case we need to fix the page without fixing the parent.
     * This method can be used only when pointer swizzling is off.
     *
     * @param[in] idx          index into buffer pool
     */
    w_rc_t fix_recovery_redo(bf_idx idx, PageID page_updated, const bool managed = true);


    /**
     * Only used in the REDO phase of Recovery process
     * with page driven REDO (Single-Page-Recovery) with minimal logging
     * mark the page as a buffer pool managed page
     */
    w_rc_t fix_recovery_redo_managed();

    /**
     * Adds an additional pin count for the given page.  This is used to re-fix the page
     * later without parent pointer.  See fix_direct() why we need this feature.  Never
     * forget to call a corresponding unpin_for_refix() for this page.  Otherwise, the
     * page will be in the bufferpool forever.
     *
     * @pre We hold our associated page's latch in SH or EX mode, it is managed by the buffer pool
     * @return slot index of the page in this bufferpool.  Pass this value to the
     * subsequent refix_direct() and unpin_for_refix() call.
     */
    bf_idx pin_for_refix();

    /**
     * Fixes a page with the already known slot index, assuming the slot has at least one
     * pin count.  Used with pin_for_refix() and unpin_for_refix().
     *
     * Currently returns eNEEDREALLATCH if mode is Q
     */
    w_rc_t refix_direct(bf_idx idx, latch_mode_t mode, bool conditional=false);

    /**
     * Fixes a new (virgin) root page for a new store with the specified page ID.
     * Implicitly, the latch will be EX and non-conditional.
     */
    w_rc_t fix_virgin_root(StoreID store, PageID pid);

    /**
     * Fixes an existing (not virgin) root page for the given store.  This method doesn't
     * receive page ID because it's already known by bufferpool.
     */
    w_rc_t fix_root(StoreID store, latch_mode_t mode,
                    bool conditional=false);

    /**
     * Imaginery 'fix' for a non-bufferpool-managed page.
     *
     * The resulting page is considered to be latched in EX mode.
     */
    void fix_nonbufferpool_page(generic_page* s);


    // ======================================================================
    //   BEGIN: Other page operations
    // ======================================================================

    /**
     * Mark this page in the bufferpool dirty.  If this page is not a bufferpool-managed
     * page, does nothing.
     *
     * @pre We do not hold current page's latch in Q mode
     */
    void         set_dirty() const;
    /**
     * Return true if this page in the bufferpool is marked dirty.  If this page is not a
     * bufferpool-managed page, returns false.
     *
     * @pre We do not hold current page's latch in Q mode
     */
    bool         is_dirty()  const;

    // Update both initial dirty lsn (if needed) and last write lsn on page
    void update_initial_and_last_lsn(const lsn_t & lsn) const;

    // Update initial dirty lsn (if needed) on page
    void update_initial_dirty_lsn(const lsn_t & lsn) const;

    void update_clsn(const lsn_t& lsn);

    /// Return flag for if this page to be deleted when bufferpool evicts it.
    /// @pre We do not hold current page's latch in Q mode
    bool         is_to_be_deleted();
    /// Flag this page to be deleted when bufferpool evicts it.
    /// @pre We hold our associated page's latch in SH or EX mode
    w_rc_t         set_to_be_deleted(bool log_it);
    /// Unset the to be deleted flag.  This is only used by UNDO, so no logging and no
    /// failure possible.
    /// @pre We hold our associated page's latch in SH or EX mode
    void         unset_to_be_deleted();


    latch_mode_t latch_mode() const { return _mode; }
    /// Do we hold our page's latch in SH or EX mode?
    bool         is_latched() const { return _mode == LATCH_SH || _mode == LATCH_EX; }

    /**
     * Could someone else have changed our page via a EX latch since we last fixed it?
     *
     * Returns false for non-bufferpool managed pages as they are assumed to be private
     * pages not in the buffer pool.
     *
     * @pre is_fixed()
     */
    bool         change_possible_after_fix() const;

    /**
     * Attempt to upgrade our latch to at least mode, which must be either LATCH_EX (the
     * default) or LATCH_SH.  Returns true iff our latch mode afterwards is >= mode.  Our
     * latch mode is unchanged if we return false.
     *
     * WARNING: this operation can spuriously fail once in a while (e.g., S -> X may fail
     * even without any other latch holders).
     */
    bool         upgrade_latch_conditional(latch_mode_t mode=LATCH_EX);


    // ======================================================================
    //   BEGIN: Interface for use only by buffer manager to perform swizzling
    // ======================================================================

    /// @pre We do not hold current page's latch in Q mode
    bool         has_children()   const;
    /// @pre We do not hold current page's latch in Q mode
    int          max_child_slot() const;
    /// valid slots are [-1 .. max_child_slot()], where -1 is foster pointer and 0 is pid0
    /// @pre We do not hold current page's latch in Q mode
    PageID*     child_slot_address(int child_slot) const;

    /**
     * Used by restore to perform REDO on a page allocated ouside the buffer
     * pool. Ideally, this would not be necessary, but unfortunately, the
     * log record interface requires a fixable_page_h to perform REDO.
     */
     void setup_for_restore(generic_page* pp);


protected:
    friend class borrowed_btree_page_h;

    bool          _bufferpool_managed; ///< is our associated page managed by the buffer pool?
    latch_mode_t  _mode;
    q_ticket_t    _Q_ticket;
};

#endif
