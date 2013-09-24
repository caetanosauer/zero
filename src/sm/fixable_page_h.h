/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef FIXABLE_PAGE_H_H
#define FIXABLE_PAGE_H_H

#include "bf_idx.h"
#include "generic_page.h"
#include "latch.h"



/**
 * \brief Handle class for pages that may be fixed (i.e., paged in by
 * the main buffer manager, bf_tree_m)
 *
 * \details
 * Currently, only B-tree pages are fixable.
 */
class fixable_page_h : public generic_page_h {
public:
    // ======================================================================
    //   BEGIN: Construction/destruction/assignment 
    // ======================================================================

    /// Create handle not yet fixed to a page
    fixable_page_h() : generic_page_h(NULL), _mode(LATCH_NL) {}
    /// Imaginery 'fix' for a non-bufferpool-managed page.
    fixable_page_h(generic_page* s) : generic_page_h(s), _mode(LATCH_NL) {
        w_assert1(s != NULL);
        w_assert1(s->tag == t_btree_p);  // <<<>>>
    }
    ~fixable_page_h() { unfix(); }

    /// assignment; steals the ownership of the page/latch p and
    /// unfixes old associated page if exists and is different
    fixable_page_h& operator=(fixable_page_h& p) {
        if (&p != this) {
            unfix();
            _pp     = p._pp;
            _mode   = p._mode;
            p._pp   = NULL;
            p._mode = LATCH_NL;
        }
        return *this;
    }


    // ======================================================================
    //   BEGIN: [Un]fixing pages
    // ======================================================================
    
    bool is_fixed() const { return _pp != 0; }
    /// Unassociate us with any page; releases latch we may have held
    /// on previously associated page.
    void unfix();

    /**
     * Fixes a non-root page in the bufferpool. This method receives
     * the parent page and efficiently fixes the page if the shpid
     * (pointer) is already swizzled by the parent page.  The
     * optimization is transparent for most of the code because the
     * shpid stored in the parent page is automatically (and
     * atomically) changed to a swizzled pointer by the bufferpool.
     *
     * @param[in] parent parent of the page to be fixed.  has to be
     * already latched.  if you can't provide this, use fix_direct()
     * though it can't exploit pointer swizzling and thus will be
     * slower.
     * @param[in] vol volume ID.
     * @param[in] shpid ID of the page to fix (or bufferpool index
     * when swizzled)
     * @param[in] mode latch mode.  has to be SH or EX.
     * @param[in] conditional whether the fix is conditional (returns
     * immediately even if failed).
     * @param[in] virgin_page whether the page is a new page thus
     * doesn't have to be read from disk.
     */
    w_rc_t fix_nonroot(const fixable_page_h &parent, volid_t vol,
                       shpid_t shpid, latch_mode_t mode, bool conditional=false, 
                       bool virgin_page=false);

    /**
     * Fixes any page (root or non-root) in the bufferpool without
     * pointer swizzling.  In some places, we need to fix a page
     * without fixing the parent, e.g., recovery or re-fix in cursor.
     * For such code, this method allows fixing without
     * parent. However, this method can be used only when pointer
     * swizzling is off.
     * @see bf_tree_m::fix_direct()
     *
     * @param[in] vol volume ID.
     * @param[in] shpid ID of the page to fix. If the shpid looks like
     * a swizzled pointer, this method returns an error (see above).
     * @param[in] mode latch mode. has to be SH or EX.
     * @param[in] conditional whether the fix is conditional (returns
     * immediately even if failed).
     * @param[in] virgin_page whether the page is a new page thus
     * doesn't have to be read from disk.
     */
    w_rc_t fix_direct(volid_t vol, shpid_t shpid, latch_mode_t mode,
                      bool conditional=false, bool virgin_page=false);

    /**
     * Adds an additional pin count for the given page (which must be
     * already latched).  This is used to re-fix the page later
     * without parent pointer.  See fix_direct() why we need this
     * feature.  Never forget to call a corresponding
     * unpin_for_refix() for this page.  Otherwise, the page will be in
     * the bufferpool forever.  
     * @return slot index of the page in this bufferpool.  Use this
     * value to the subsequent refix_direct() and unpin_for_refix()
     * call.
     */
    bf_idx pin_for_refix();

    /**
     * Fixes a page with the already known slot index, assuming the
     * slot has at least one pin count.  Used with pin_for_refix() and
     * unpin_for_refix().
     */
    w_rc_t refix_direct(bf_idx idx, latch_mode_t mode, 
                        bool conditional=false);

    /**
     * Fixes a new (virgin) root page for a new store with the
     * specified page ID.  Implicitly, the latch will be EX and
     * non-conditional.
     */
    w_rc_t fix_virgin_root(volid_t vol, snum_t store, shpid_t shpid);

    /**
     * Fixes an existing (not virgin) root page for the given store.
     * This method doesn't receive page ID because it's already known
     * by bufferpool.
     */
    w_rc_t fix_root(volid_t vol, snum_t store, latch_mode_t mode,
                    bool conditional=false);


    // ======================================================================
    //   BEGIN: 
    // ======================================================================

    /// Mark this page in the bufferpool dirty.  If this page is not a
    /// bufferpool-managed page, does nothing.
    void         set_dirty() const;
    /// Return true if this page in the bufferpool is marked dirty.
    /// If this page is not a bufferpool-managed page, returns false.
    bool         is_dirty()  const;


    /// Flag this page to be deleted when bufferpool evicts it.
    rc_t         set_to_be_deleted(bool log_it);
    /// Unset the to be deleted flag.  This is only used by UNDO, so
    /// no logging and no failure possible.
    void         unset_to_be_deleted();
    bool         is_to_be_deleted() { return (_pp->page_flags&t_to_be_deleted) != 0; }

    latch_mode_t latch_mode() const { return _mode; }
    bool         is_latched() const { return _mode != LATCH_NL; }
    /// Conditionally upgrade the latch to EX.  Returns true if successfully upgraded.
    bool         upgrade_latch_conditional();


    // ======================================================================
    //   BEGIN: Interface for use only by buffer manager to perform swizzling
    // ======================================================================

    bool         has_children()   const;
    int          max_child_slot() const;
    /// valid slots are [-1 .. max_child_slot()], where -1 is foster pointer and 0 is pid0
    shpid_t*     child_slot_address(int child_slot) const;

    
protected:
    friend class borrowed_btree_page_h;

    latch_mode_t  _mode;
};

#endif
