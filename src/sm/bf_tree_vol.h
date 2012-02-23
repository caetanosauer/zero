#ifndef BF_TREE_VOL_T
#define BF_TREE_VOL_T

#include "w_defines.h"
#include "bf_idx.h"
#include "stid_t.h"
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
    bf_tree_vol_t () {
        ::memset(this, 0, sizeof(bf_tree_vol_t));
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
     */
    bf_idx _root_pages[MAX_STORE_COUNT];
    
    /**
     * Pointer to the volume object. Used to read and write from this volume.
     */
    vol_t* _volume;
};

#endif // BF_TREE_VOL_T
