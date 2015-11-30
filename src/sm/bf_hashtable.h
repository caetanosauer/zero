/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BF_HASHTABLE_H
#define BF_HASHTABLE_H

#include "basics.h"
#include "w_defines.h"
#include "bf_idx.h"
#include <utility>

template<class T>
class bf_hashbucket;

typedef pair<bf_idx, bf_idx> bf_idx_pair;

/**
 * \Brief Hash table for the buffer pool manager.
 * \ingroup SSMBUFPOOL
 * \Details
 * \Section{Difference from original Shore-MT's hash table}
 * We changed the protocol of page-pinning in bufferpool,
 * so several things have been changed in this hashtable too.
 *
 * The first change is that now this hash table only stores and returns bf_idx,
 * the index in buffer pool, not the actual pointer to the corresponding block.
 * This significantly speeds up the hashtable.
 *
 * The second change is that now we use a simple hashing, not cuckoo-hashing.
 * The main reason is that kicking out the existing entry requires additional
 * latching on it to change its hash function.
 * As the cost of hash collision is relatively cheap, we avoid cuckoo-hashing.
 * (This design might be revisited later, though).
 *
 * Another notable difference is that now this hashtable is totally separated
 * from the bufferpool itself. In other words, it's merely an unoredered_map<pageid, bf_idx>
 * with spinlocks. This means that we do not pin the page in bufferpool
 * while probing this hash table. It's the caller's responsibility to
 * pin the corresponding page in bufferpool.
 *
 * Because of this separation, there IS a slight chance that the page returned by
 * this hashtable is evicted and no longer available in bufferpool
 * when the client subsequently tries to pin the page. If that happens, the client
 * must retry from looking up this hashtable.
 */
template<class T>
class bf_hashtable {
public:
    bf_hashtable(uint32_t size);
    ~bf_hashtable();

    /**
     * Returns the bf_idx linked to the given key (volume and page ID, see bf_key() in bf_tree.cpp).
     * If the key doesn't exist in this bufferpool, returns 0 (invalid bf_idx).
     */
    bool      lookup(PageID key, T& value) const;

    /**
     * Imprecise-but-fast version of lookup().
     * This method doesn't take latch, so it's much faster. However false-positives/negatives
     * are possible. The caller must make sure false-positives/negatives won't cause an issue.
     * This is so far used from eviction routine, which doesn't have to be precise.
     */
    bool      lookup_imprecise(PageID key, T& value) const;

    /**
    * Insert the key in the _table and link it with the given bf_idx.
    * if the given key already exists, this method doesn't change anything and returns false.
    */
    bool        insert_if_not_exists(PageID key, T value);

    /**
     * Updates the value associated with the given key. Returns false if key
     * is not found.
     */
    bool        update(PageID key, T value);

    /**
     * Removes the key from the _table.
     * Returns if the pageID existed or not.
     */
    bool        remove(PageID key);

private:
    uint32_t            _size;
    bf_hashbucket<T>*      _table;
};

#endif // BF_HASHTABLE_H
