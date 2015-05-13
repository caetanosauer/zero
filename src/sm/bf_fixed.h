/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BF_FIXED_H
#define BF_FIXED_H

#include "w_defines.h"

#include "w_rc.h"
#include "lsn.h"

class generic_page;
class vol_t;

/**
 * \brief Buffer manager for a small number of special pages in each volume.
 * \ingroup SSMBUFPOOL
 *
 * \details
 * This buffer manager deals with only allocation pages (alloc_page) and
 * store node pages (stnode_page). All pages are always pinned as there are
 * only a fixed and small number of such pages. Therefore, this buffer manager
 * is much simpler and more efficient than the main buffer manager.
 *
 * How simpler it is?  MUCH.
 * No pinning, no synchronization (alloc_cache does it on behalf),
 * no hash table, no background cleaner, no eviction, no write-order-dependency.
 *
 * Also, this buffer manager is the only one that can handle non-hierarchical
 * pages unlike the main one which only deals with btree pages.
 */
class bf_fixed_m {
public:
    bf_fixed_m(vol_t* parent, int unix_fd, shpid_t max_pid);
    virtual ~bf_fixed_m();

    /**
     * This is called after volume is available in order to read the actual
     * pages from disk, whereas the constructor simply allocates the buffer
     * memory. This is important for restore, since the bf_fixed is first
     * constructed and only initialized later, after the metadata pages have
     * been restored.
     */
    w_rc_t init();

    /**
     * Flush (write out) all pages. Used while shutdown and checkpoint.
     */
    w_rc_t flush ();

    /** returns the pointer to page data maintained in this bufferpool. */
    generic_page* get_pages ();
    bool* get_dirty_flags ();

    /** returns number of pages maintained in this bufferpool. */
    uint32_t get_page_cnt() const;

    srwlock_t& get_checkpoint_lock();

private:
    /** the volume. */
    vol_t*      _parent;
    /** file pointer. */
    int         _unix_fd;

    /**
     * Spinlock to pretect against checkpoint (flushing the content of the buffer).
     * The Read/Write semantics is a bit special:
     * Read: each thread that _modifies_ the content takes a read lock. If the
     *   thread is just reading the content, it doesn't take a lock because it's safe against checkpoint.
     * Write: the thread doing checkpoint takes a write lock.
     */
    srwlock_t   _checkpoint_lock;

    /** number of pages maintained in this bufferpool. */
    uint32_t    _page_cnt;
    /** page data maintained in this bufferpool. */
    generic_page*     _pages;
    /** dirty flags for _pages. */
    bool*       _dirty_flags;
};
inline uint32_t bf_fixed_m::get_page_cnt() const {
    return _page_cnt;
}
inline generic_page* bf_fixed_m::get_pages () {
    return _pages;
}
inline bool* bf_fixed_m::get_dirty_flags () {
    return _dirty_flags;
}
inline srwlock_t& bf_fixed_m::get_checkpoint_lock() {
    return _checkpoint_lock;
}

#endif //BF_FIXED_H
