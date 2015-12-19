/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef ALLOC_CACHE_H
#define ALLOC_CACHE_H

#include "w_defines.h"
#include "alloc_page.h"
#include "latch.h"
#include <vector>

class bf_fixed_m;

/**
 * \brief Free-Page allocation/deallocation interface.
 *
 * \details
 * This object handles allocation/deallocation requests for one volume.
 * All allocation/deallocation are logged and done in a critical section.
 * To make it scalable, this object is designed to be as fast as possible.
 * @See alloc_page_h
 */
class alloc_cache_t {
public:
    alloc_cache_t(stnode_cache_t& stcache, bool virgin);

    /**
     * Allocates one page. (System transaction)
     * @param[out] pid allocated page ID.
     * @param[in] store Store to which new page belongs (0 to allocate on
     *                  store-independent area)
     * @param[in] redo If redoing the operation (no log generated)
     */
    rc_t sx_allocate_page(PageID &pid, StoreID store = 0, bool redo = false);

    /**
     * Deallocates one page. (System transaction)
     * @param[in] pid page ID to deallocate.
     */
    rc_t sx_deallocate_page(PageID pid, StoreID store = 0, bool redo = false);

    bool is_allocated (PageID pid);

    rc_t force_pages();

    PageID get_last_allocated_pid() const;

private:

    /**
     * These vectors keep track of the free space for each store. Index 0
     * refers to space not assigned to any store. The vector last_alloc_page
     * keeps track of the last page id allocated for each store, which also
     * allows determining the extent id. This allows quick allocation by simply
     * incrementing the counter. When it reaches a page id divisible by the
     * extent size, a new extent must be allocated.
     *
     * Pages which are freed end up in the list of freed pages, again
     * one for each store. Currently, these lists are only used to determine
     * whether a certain page is allocated or not. To avoid fragmentation in
     * a workload with many deletions, items should be removed from these lists
     * when allocating a page to avoid fragmentation. One extreme policy would
     * be to only use the contiguous space when the corresponding list is
     * empty, i.e., when the non-contiguous space has been fully utilized.
     * Policies that trade off allocation performance for fragmentation by
     * managing allocations from both contiguous and non-contiguous space would
     * be the more flexible and robust option.
     */
    vector<PageID> last_alloc_page;
    vector<list<PageID>> freed_pages;

    /**
     * Bitmap of extents whose allocation pages were already loaded. This is
     * needed because the allocation information of each extent is loaded on
     * demand.
     */
    vector<bool> loaded_extents;

    /**
     * CS TODO
     * This map is needed fore the sole purpose of maintaining per-page
     * log-record chains. It keeps track of the current pageLSN of each
     * alloc page. Since we don't apply allocation operations on the pages
     * directly (i.e., no page fix is performed), this is required to
     * generate a correct prev_page pointer when logging allocations.
     */
    map<PageID, lsn_t> page_lsns;

    stnode_cache_t& stcache;

    /** all operations in this object are protected by this lock. */
    mutable srwlock_t _latch;

    static const size_t extent_size;

    rc_t load_alloc_page(extent_id_t ext, bool is_last_ext);
};

#endif // ALLOC_CACHE_H
