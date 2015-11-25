/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef STNODE_PAGE_H
#define STNODE_PAGE_H

#include <vector>

#include "generic_page.h"
#include "srwlock.h"
#include "w_defines.h"
#include "sm_base.h"
#include "alloc_page.h"

/**
 * \brief Persistent structure representing metadata for a store.
 *
 * \details
  *
 * These are contained in \ref stnode_page's.
 */
struct stnode_t {
    /// also okay to initialize via memset
    stnode_t() : root(0), last_extent(0)
    { }

    /**
     * Root page ID of the store; 0 if the store is not allocated and 1 for
     * the special store number 0, which is used to keep track of the last
     * extent allocated which was not assigned to a specific store.
     */
    shpid_t         root;      // +4 -> 4

    /**
     * Last extend occupied by this store; 0 if store does not use exclusive
     * extents.
     */
    extent_id_t     last_extent; // +4 -> 8

    bool is_used() const  { return root != 0; }
};


/**
 * \brief Store-node page that contains one stnode_t for each
 * (possibly deleted or uncreated) store belonging to a given volume.
 */
class stnode_page : public generic_page_header {
public:

    /// max # \ref stnode_t's on a single stnode_page; thus, the
    /// maximum number of stores per volume
    static const size_t max = (page_sz - sizeof(generic_page_header) - sizeof(lsn_t))
        / sizeof(stnode_t);

    // Page ID used by the stnode page
    static const shpid_t stpid = shpid_t(1);

    stnode_t get(size_t index) const {
        w_assert1(index < max);
        return stnode[index];
    }

    void set_root(size_t index, shpid_t root) {
        w_assert1(index < max);
        stnode[index].root = root;
    }

    void update_last_extent(size_t index, extent_id_t ext)
    {
        w_assert1(index < max);
        stnode[index].last_extent = ext;
    }

    lsn_t getBackupLSN() { return backupLSN; }

    void format_empty(vid_t vid) {
        memset(this, 0, sizeof(generic_page_header));
        pid = lpid_t(vid, stnode_page::stpid);

        backupLSN = lsn_t(0,0);
        memset(&stnode, 0, sizeof(stnode_t) * max);
        update_last_extent(0, 0);
    }


private:
    // Used for backup files
    lsn_t backupLSN;

    /// stnode[i] is the stnode_t for store # i of this volume
    stnode_t stnode[max];
};
BOOST_STATIC_ASSERT(sizeof(stnode_page) == generic_page_header::page_sz);

/**
 * \brief Store creation/destroy/query interface.
 *
 * \details
 * This object handles store create/destroy/query requests for one
 * volume.  99.99% of the requests are, of course, querying the root
 * page ID of indexes.  This object does a lightweight synchronization
 * (latch) to protect them from MT accesses.  However, this object
 * doesn't use locks because we don't need them.  If the store is
 * being destroyed, ss_m will check intent locks before calling this
 * object, so we are safe.
 *
 * This object and vol_t replace the "directory" thingies in original
 * Shore-MT with more efficiency and simplicity.
 */
class stnode_cache_t {
public:
    /// special_pages here holds the special pages for volume vid, the
    /// last of which should be the stnode_page for that volume
    stnode_cache_t(stnode_page& stpage);

    /**
     * Returns the root page ID of the given store.
     * If that store isn't allocated, returns 0.
     * @param[in] store Store ID.
     */
    shpid_t get_root_pid(snum_t store) const;

    bool is_allocated(snum_t store) const;

    /// Make a copy of the entire stnode_t of the given store.
    stnode_t get_stnode(snum_t store) const;

    /// Returns the snum_t of all allocated stores in the volume.
    void get_used_stores(std::vector<snum_t>&) const;

    rc_t sx_create_store(shpid_t root_pid, snum_t& snum, bool redo = false);

    rc_t sx_append_extent(snum_t snum, extent_id_t ext, bool redo = false);

    vid_t get_vid() const { return _vid; }

private:
    /// all operations in this object except get_root_pid are protected by this latch
    mutable queue_based_lock_t _latch;

    // CS TODO: not needed with decoupled propagation (Merge Lucas' branch)
    stnode_page _stnode_page;        /// The stnode_page of the volume we are caching

    vid_t _vid;

    /// Returns the first snum_t that can be used for a new store in
    /// this volume or stnode_page::max if all available stores of
    /// this volume are already allocated.
    snum_t get_min_unused_store_ID() const;

    // CS TODO: not needed with decoupled propagation (Merge Lucas' branch)
    rc_t write_stnode_page();
};

#endif // STNODE_PAGE_H
