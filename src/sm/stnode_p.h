/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef STNODE_P_H
#define STNODE_P_H

#include "w_defines.h"

#include "page_s.h"
#include "srwlock.h"
#include "sthread.h"
#include <vector>

/**
 * \brief Persistent structure representing the head of a store's extent list.
 * \details These structures sit on stnode_p pages and point to the
 * start of the extent list.
 * The stnode_t structures are indexed by store number.
 */
struct stnode_t {
    stnode_t() {
      root = 0;
      flags = 0;
      deleting = 0;
    }
    /**\brief First extent of the store */
    shpid_t         root; // +4 -> 4
    /**\brief store flags  */
    uint16_t        flags; // +2 -> 6
    /**\brief non-zero if deleting or deleted */
    uint16_t        deleting;  // +2 -> 8
};

class stnode_cache_t;
class bf_fixed_m;

/**
 * \brief Extent map page that contains store nodes (stnode_t).
 * \details These are the pages that contain the starting points of 
 * a store's root page.
 */
class stnode_p {
    friend class stnode_cache_t;
public:
    stnode_p(page_s* s) : _pp (s) {}
    ~stnode_p()  {}

    rc_t format(const lpid_t& pid);

    // max # store nodes on a page
    enum { max = page_s::data_sz / sizeof(stnode_t) };

    stnode_t&       get(size_t idx);

    page_s *_pp;
};

/**
 * \brief Store creation/destroy/query interface.
 * \details
 * This object handles store create/destroy/query requests for one volume.
 * 99.99% of the requests are of course querying the root page id of indexes.
 * This object does a light weight synchronization (latch) to protect
 * them from MT accesses. However, this doesn't use locks because
 * we don't need them. If the store is being destroyed, ss_m will check
 * intent locks before calling this object, so we are safe.
 * This object and vol_t replace the "directory" thingies in original Shore-MT
 * with more efficiency and simplicity.
 * @See stnode_p
 */
class stnode_cache_t {
public:
    /** Creates the cache.*/
    stnode_cache_t (vid_t vid, bf_fixed_m* fixed_pages);
    
    /**
     * Returns the root page Id of the store.
     * If the store isn't created yet, returns 0.
     * @param[in] store Store ID.
     */
    shpid_t get_root_pid (snum_t store) const;
    
    /**
     * Returns the entire stnode_t of the store.
     */
    void get_stnode (snum_t store, stnode_t &stnode) const;

    /**
    *  Fix the storenode page and perform the store operation 
    *     AND 
    *  log it.
    *
    *  param type is in sm_io.h.
    *
    *  It contains:
        typedef smlevel_0::store_operation_t        store_operation_t;
        in sm_base.h
        Operations:
            t_delete_store, <---- when really deleted after space freed
            t_create_store,  <--- store is allocated (snum_t is in use)
            t_set_deleting,  <---- when transaction deletes store (t_deleting_store)
                            <---- end of xct (t_store_freeing_exts)
            t_set_store_flags, 

        typedef smlevel_0::store_flag_t             store_flag_t;
            in sm_base.h:
            logging attribute: regular, tmp, load, insert

        typedef smlevel_0::store_deleting_t         store_deleting_t;
                t_not_deleting_store = 0,  // must be 0: code assumes it
                t_deleting_store, 
                t_unknown_deleting // for error handling
    *
    */
    rc_t  store_operation(const store_operation_param & op);

    /** Returns the first snum_t that can be used for a new store. */
    snum_t get_min_unused_store_id () const;

    /** Returns the snum_t of all stores that exist in the volume. */
    std::vector<snum_t> get_all_used_store_id () const;

private:
    vid_t _vid;
    bf_fixed_m* _fixed_pages;

    /** this merely points to the stnode_p data in bf_fixed_m. */
    stnode_t *_stnodes;

    /** all operations in this object are protected by this lock. */
    mutable queue_based_lock_t _spin_lock;
};

#endif //STNODE_P_H
