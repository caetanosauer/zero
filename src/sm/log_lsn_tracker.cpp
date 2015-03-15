/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#include "log_lsn_tracker.h"
#include "lock_compt.h"
#include "w_defines.h"
#include "w_debug.h"
#include <AtomicCounter.hpp>

PoorMansOldestLsnTracker::PoorMansOldestLsnTracker(uint32_t buckets) {
    // same logic as in lock_core(). yes, stupid prime hashing. but see the name of this class.
    int b = 0; // count bits shifted
    for (_buckets = 1; _buckets < buckets; _buckets <<= 1) {
        b++;
    }
    w_assert1(b >= 6 && b <= 23);
    b -= 6;
    _buckets = primes[b];

    _low_water_marks = new lsndata_t[_buckets];
    w_assert1(_low_water_marks);
    ::memset(_low_water_marks, 0, sizeof(lsndata_t) * _buckets);
}
PoorMansOldestLsnTracker::~PoorMansOldestLsnTracker() {
#if W_DEBUG_LEVEL > 0
    for (uint32_t i = 0; i < _buckets; ++i) {
        if (_low_water_marks[i] != 0) {
            ERROUT(<<"Non-zero _low_water_marks! i=" << i << ", val=" << _low_water_marks[i]);
            w_assert1(_low_water_marks[i] == 0);
        }
    }
#endif // W_DEBUG_LEVEL > 0
    delete[] _low_water_marks;
}

void PoorMansOldestLsnTracker::enter(uint64_t xct_id, const lsn_t& curr_lsn) {
    lsndata_t data = curr_lsn.data();
    lsndata_t cas_tmp = 0;
    uint32_t index = xct_id % _buckets;
    DBGOUT4(<<"PoorMansOldestLsnTracker::enter. xct_id=" << xct_id
        << ", index=" << index <<", data=" << data);
    lsndata_t *address = _low_water_marks + index;
    int counter = 0;
    while (!lintel::unsafe::atomic_compare_exchange_strong<lsndata_t>(
        address, &cas_tmp, data)) {
        cas_tmp = 0;
        ++counter;
        if ((counter & 0xFFFF) == 0) {
            DBGOUT1(<<"WARNING: spinning on PoorMansOldestLsnTracker::enter..");
        } else if ((counter & 0xFFFFFF) == 0) {
            ERROUT(<<"WARNING: spinning on PoorMansOldestLsnTracker::enter for LONG time..");
        }
    }
    w_assert1(_low_water_marks[index] == data);
}

void PoorMansOldestLsnTracker::leave(uint64_t xct_id) {
    uint32_t index = xct_id % _buckets;
    DBGOUT4(<<"PoorMansOldestLsnTracker::leave. xct_id=" << xct_id
        << ", index =" << index <<", current value=" << _low_water_marks[index]);
    _low_water_marks[index] = 0;
}


lsn_t PoorMansOldestLsnTracker::get_oldest_active_lsn(lsn_t curr_lsn) {
    lsndata_t smallest = lsndata_max;
    for (uint32_t i = 0; i < _buckets; ++i) {
        if (_low_water_marks[i] != 0 && _low_water_marks[i] < smallest) {
            smallest = _low_water_marks[i];
        }
    }
    _cache = lsn_t(smallest == lsndata_max ? curr_lsn.data() : smallest);
    return _cache;
}
