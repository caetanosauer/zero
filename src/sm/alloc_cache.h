#ifndef ALLOC_CACHE_H
#define ALLOC_CACHE_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

#include "alloc_p.h"
#include "latch.h"
#include <vector>

class bf_fixed_m;

/**
 * \brief Free-Page allocation/deallocation interface.
 * \details
 * This object handles allocation/deallocation requests for one volume.
 * All allocation/deallocation are logged and done in a critical section.
 * To make it scalable, this object is designed to be as fast as possible.
 * Actually, in original Shore-MT, page allocation was a huge bottleneck in high contention.
 * See ticket:74 for more details.
 * @See alloc_p
 */
class alloc_cache_t {
public:
    /** Creates an empty cache.*/
    alloc_cache_t (vid_t vid, bf_fixed_m* fixed_pages) :
        _vid(vid), _fixed_pages(fixed_pages), _contiguous_free_pages_begin(0), _contiguous_free_pages_end(0) {}

    /** Initialize this object by scanning alloc_p pages of the volume. */
    rc_t load_by_scan (shpid_t max_pid);
    
    /**
     * Allocates one new page from the free-page pool.
     * This method logs the allocation.
     * @param[out] pid allocated page ID.
     */
    rc_t allocate_one_page (shpid_t &pid);
    
    /**
     * Allocates the spefified count of consecutive pages from the free-page pool.
     * This method logs the allocation.
     * @param[out] pid_begin the beginning of allocated page IDs.
     * @param[in] page_count number of pages to allocate.
     */
    rc_t allocate_consecutive_pages (shpid_t &pid_begin, size_t page_count);
    
    /**
     * Deallocates one page in the free-page pool.
     * This method logs the deallocation.
     * @param[in] pid page ID to deallocate.
     */
    rc_t deallocate_one_page (shpid_t pid);

    // for REDOs.
    rc_t redo_allocate_one_page (shpid_t pid);
    rc_t redo_allocate_consecutive_pages (shpid_t pid_begin, size_t page_count);
    rc_t redo_deallocate_one_page (shpid_t pid);
    
    /** Returns the count of all free pages. */
    size_t get_total_free_page_count () const;
    /** Returns the count of consecutive free pages. */
    size_t get_consecutive_free_page_count () const;
    
    /** Returns if the page is already allocated. not quite fast. don't call this so often!. */
    bool is_allocated_page (shpid_t pid) const;

private:
    vid_t _vid;
    bf_fixed_m* _fixed_pages;
    
    /** the first page in this volume from which all pages are unallocated. */
    shpid_t _contiguous_free_pages_begin;
    /** usually just the number of pages in this volume. */
    shpid_t _contiguous_free_pages_end;
    
    /** ID list of unallocated pages. */
    std::vector<shpid_t> _non_contiguous_free_pages;

    /** all operations in this object are protected by this lock. */
    mutable srwlock_t _queue_lock;

    // these do logging (if logit=true) and applying to alloc_p.
    // they assume _contiguous_free_pages_begin/_non_contiguous_free_pages are already modified
    rc_t apply_allocate_one_page (shpid_t pid, bool logit = true);
    rc_t apply_allocate_consecutive_pages (shpid_t pid_begin, size_t page_count, bool logit = true);
    rc_t apply_deallocate_one_page (shpid_t pid, bool logit = true);
};

#endif // ALLOC_CACHE_H
