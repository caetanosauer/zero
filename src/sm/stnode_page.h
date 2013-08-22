/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef STNODE_PAGE_H
#define STNODE_PAGE_H

#include "w_defines.h"
#include "generic_page.h"
#include "srwlock.h"
#include <vector>

class bf_fixed_m;



/**
 * \brief Persistent structure representing the head of a store's extent list.
 *
 * \details
 * These structures sit on stnode_page pages and point to the start of
 * the extent list.  The stnode_t structures are indexed by store
 * number.
 */
struct stnode_t {
    stnode_t() {
      root     = 0;
      flags    = 0;
      deleting = 0;
    }

    /// First extent of the store
    shpid_t         root;      // +4 -> 4
    /// store flags 
    uint16_t        flags;     // +2 -> 6; holds a smlevel_0::store_flag_t
    /// non-zero if deleting or deleted
    uint16_t        deleting;  // +2 -> 8
};


/**
 * \brief Extent map page that contains store nodes (stnode_t's).
 *
 * \details
 * These are the pages that contain the starting points of a store's
 * root page.
 */
class stnode_page : public generic_page_header {
    friend class stnode_page_h;

    /// max # stnode's on a page
    static const size_t max = data_sz / sizeof(stnode_t);

    stnode_t stnode[max];

    /// unused space (ideally of zero size)
    char*    padding[data_sz - sizeof(stnode)];
};




/**
 * \brief Handle for an extent map page (stnode_page).
 */
class stnode_page_h {
    stnode_page *_page;

public:
    /// format given page with page-ID pid as an stnode_page page then
    /// return a handle to it
    stnode_page_h(generic_page* s, const lpid_t& pid);
    /// construct handle from an existing stnode_page page
    stnode_page_h(generic_page* s) : _page(reinterpret_cast<stnode_page*>(s)) {
        w_assert1(s->tag == t_stnode_p);
    }
    ~stnode_page_h() {}

    /// return pointer to underlying page
    generic_page* to_generic_page() const { return reinterpret_cast<generic_page*>(_page); }


    /// max # store nodes on a page
    static const size_t max = stnode_page::max;

    stnode_t& get(size_t index) {
        w_assert1(index < max);
        return _page->stnode[index];
    }
    const stnode_t& get(size_t index) const {
        w_assert1(index < max);
        return _page->stnode[index];
    }
};



/**
 * \brief Store creation/destroy/query interface.
 *
 * \details
 * This object handles store create/destroy/query requests for one
 * volume.  99.99% of the requests are of course querying the root
 * page ID of indexes.  This object does a lightweight synchronization
 * (latch) to protect them from MT accesses.  However, this doesn't
 * use locks because we don't need them.  If the store is being
 * destroyed, ss_m will check intent locks before calling this object,
 * so we are safe.  This object and vol_t replace the "directory"
 * thingies in original Shore-MT with more efficiency and simplicity.
 * @See stnode_page_h
 */
class stnode_cache_t {
public:
    stnode_cache_t(vid_t vid, bf_fixed_m* fixed_pages);
    
    /**
     * Returns the root page ID of the store.
     * If the store isn't created yet, returns 0.
     * @param[in] store Store ID.
     */
    shpid_t get_root_pid(snum_t store) const;
    
    /// Returns the entire stnode_t of the given store.
    void get_stnode(snum_t store, stnode_t &stnode) const;

    /// Returns the first snum_t that can be used for a new store.
    snum_t get_min_unused_store_ID() const;

    /// Returns the snum_t of all stores that exist in the volume.
    std::vector<snum_t> get_all_used_store_ID() const;


    /**
     *  Fix the stnode_page and perform the store operation 
     *     AND 
     *  log it.
     *
     *  param type is in sm_io.h.
     *
     *  It contains:
     *   typedef smlevel_0::store_operation_t        store_operation_t;
     *   in sm_base.h
     *   Operations:
     *       t_delete_store, <---- when really deleted after space freed
     *       t_create_store, <---- store is allocated (snum_t is in use)
     *       t_set_deleting, <---- when transaction deletes store (t_deleting_store)
     *                       <---- end of xct (t_store_freeing_exts)
     *       t_set_store_flags, 
     *
     *   typedef smlevel_0::store_flag_t             store_flag_t;
     *       in sm_base.h:
     *       logging attribute: regular, tmp, load, insert
     *
     *   typedef smlevel_0::store_deleting_t         store_deleting_t;
     *           t_not_deleting_store = 0,  // must be 0: code assumes it
     *           t_deleting_store, 
     *           t_unknown_deleting         // for error handling
     */
    rc_t  store_operation(const store_operation_param &op);

private:
    /// all operations in this object are protected by this lock
    mutable queue_based_lock_t _spin_lock;

    vid_t         _vid;
    bf_fixed_m*   _fixed_pages;
    stnode_page_h _stnode_page;        // The stnode_page of _fixed_pages
};

#endif // STNODE_PAGE_H
