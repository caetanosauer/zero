/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef LOGBUF_HASHTABLE_H
#define LOGBUF_HASHTABLE_H

#include "w_defines.h"

//#include "lsn.h"
#include "logbuf_seg.h"

class logbuf_hashbucket;

/**
  * Hash table for the log buffer
  * Modified from the hash table for the buffer pool manager (bf_hashtable)
  */

class logbuf_hashtable {
public:
    logbuf_hashtable(uint32_t size);
    ~logbuf_hashtable();

    /**
     * Return the pointer to the logbuf_seg containing the given key (lsn).
     * If the key doesn't exist, return NULL.
     */
    logbuf_seg *lookup(uint64_t key) const;

    // not needed
    // /**
    //  * Imprecise-but-fast version of lookup().
    //  * This method doesn't take latch, so it's much faster. 
    //  * However false-positives/negatives are possible. 
    //  * The caller must make sure false-positives/negatives won't cause an issue. 
    //  */
    // logbuf_seg *lookup_imprecise(uint64_t key) const;

    /**
    * Insert the key in the _table and link it with the given key.
    * if the given key already exists, 
    * this method doesn't change anything and returns false.
    * This method uses latches, and it is for read
    */
    bool        insert_if_not_exists(uint64_t key, logbuf_seg *value);
    
    /**
     * Removes the key from the _table.
     * Returns if the key existed or not.
     */
    bool        remove(uint64_t key);


private:
    uint32_t            _size;
    logbuf_hashbucket*      _table;
};

#endif // LOGBUF_HASHTABLE_H
