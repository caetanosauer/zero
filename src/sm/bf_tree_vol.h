/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BF_TREE_VOL_T
#define BF_TREE_VOL_T

#include "w_defines.h"
#include "bf_idx.h"
#include <string.h>

class vol_t;

/**
 * \Brief Volume descriptor in bufferpool.
 * \ingroup SSMBUFPOOL
 * \Details
 * This object is instantiated when one volume is mounted and
 * revoked when the volume is unmounted.
 */
struct bf_tree_vol_t {
    bf_tree_vol_t (vol_t* volume) : _volume (volume) {
        ::memset(_root_pages, 0, sizeof(bf_idx) * stnode_page::max);
    }
    /**
     * Array of pointers to control block for root pages in this volume.
     * The array index is stnum.
     *
     * When the volume is mounted, existing root pages are loaded
     * and set to this array. Other root pages (non existing stores)
     * are NULL. When a new store is created, its root page is loaded
     * and set to this array.
     *
     * Because such root pages are forever kept in this array and
     * there is no race condition in loading (none will use non-existing stnum),
     * this array does not have to be protected by mutex or spinlocks.
     *
     * CS TODO But we still have to ensure cache coherency, because one thread
     * may create a store and another one access it imediately after. In that
     * case, the second thread may see a null root page in the array.
     */
    bf_idx _root_pages[stnode_page::max];

    /**
     * Pointer to the volume object. Used to read and write from this volume.
     */
    vol_t* _volume;

    /**
     * CS: Method used for on-demand loading of root pages, eliminating the
     * need to call install_volume.
     */
    void add_root_page(StoreID store, bf_idx idx)
    {
        _root_pages[store] = idx;
    }
};

#endif // BF_TREE_VOL_T
