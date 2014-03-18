/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#ifndef LOG_LSN_TRACKER_H
#define LOG_LSN_TRACKER_H

#include <stdint.h>
#include "lsn.h"

/**
 * \brief This class is a strawman implementation of tracking the oldest active transaction
 * in the system.
 * \ingroup SSMLOG
 * \details
 * It simply maintains an array of lsndata_t, hash-indexed by _pointer value_ of xct_t so that
 * we can put and remove the _curr_lsn as of the transaction enters.
 * This is a so simple hashmap. We don't even have next pointers in the bucket to avoid
 * synchronization. If the bucket is unluckily already taken, we just spin until the
 * other transaction finishes.
 * In most cases, this causes only one atomic operation for each transaction.
 * Retrieving the low water mark takes time because it has to read all entries, but
 * this class can be used as a delayed proxy. Use get_oldest_active_lsn_cache() in that case.
 */
class PoorMansOldestLsnTracker {
public:
    PoorMansOldestLsnTracker(uint32_t buckets);
    ~PoorMansOldestLsnTracker();

    /**
     * Put the log_m::curr_lsn as of starting the given transaction.
     * @param[in] xct_id Some integer to identify the transaction. Probably pointer value
     * of xct_t. Anything works as far as it's well random.
     */
    void                enter(uint64_t xct_id, const lsn_t &curr_lsn);
    /**
     * Remove the LSN for the transaction. This must be called after enter().
     * No barrier taken. others will eventually see and being conservative is fine.
     */
    void                leave(uint64_t xct_id);
    /**
     * Scan all buckets and return the oldest LSN. It might return a \e conservative value
     * because the observed transaction might go away while scanning. Regarding
     * new transactions, they have larger value so doesn't matter.
     * This can take time.
     * @param[in] curr_lsn the current maximum LSN. this is used as the result when
     * there is no non-zero entries observed because, even if some transaction newly starts
     * and take a bucket we already scan, it's surely more than curr_lsn!
     * @return Oldest active LSN. If there is no active transaction, curr_lsn.
     */
    lsn_t               get_oldest_active_lsn(lsn_t curr_lsn);
    /** Returns the value of previous get_oldest_active_lsn() call. This is quick. */
    lsn_t               get_oldest_active_lsn_cache() const { return _cache; }
private:
    uint32_t            _buckets;
    lsndata_t*          _low_water_marks;
    lsn_t               _cache;
};

#endif // LOG_LSN_TRACKER_H
