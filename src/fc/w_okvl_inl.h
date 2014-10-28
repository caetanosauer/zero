/*
 * (c) Copyright 2013-, Hewlett-Packard Development Company, LP
 */
#ifndef W_OKVL_INL_H
#define W_OKVL_INL_H

/**
 * \file w_okvl_inl.h
 * This file contains the inline functions declared in w_okvl.h.
 */

#include "w_okvl.h"
#include <ostream>

/** Pretty name for each lock mode. */
const char* const element_mode_names[] = {
    "N", "IS", "IX", "S", "SIX", "X"
};

/**
* \brief Pre-defines frequently used lock modes as const objects.
* \Details This is for both convenience and performance.
* In many cases (most likely as a func param), we can just use these const static objects.
* These constants could be named like XX, SS, .. for short, but this lengthy name is more clear,
* so in long run it will help us.
*/
const okvl_mode ALL_N_GAP_N(okvl_mode::N, okvl_mode::N);
const okvl_mode ALL_S_GAP_N(okvl_mode::S, okvl_mode::N);
const okvl_mode ALL_X_GAP_N(okvl_mode::X, okvl_mode::N);
const okvl_mode ALL_N_GAP_S(okvl_mode::N, okvl_mode::S);
const okvl_mode ALL_S_GAP_S(okvl_mode::S, okvl_mode::S);
const okvl_mode ALL_X_GAP_S(okvl_mode::X, okvl_mode::S);
const okvl_mode ALL_N_GAP_X(okvl_mode::N, okvl_mode::X);
const okvl_mode ALL_S_GAP_X(okvl_mode::S, okvl_mode::X);
const okvl_mode ALL_X_GAP_X(okvl_mode::X, okvl_mode::X);

#define T true // to make following easier to read
#define F false
/**
*  compatibility_table[requested mode][granted mode]
*  means whether the requested lock mode is allowed given the already existing lock in granted mode.
*/
const bool compatibility_table[okvl_mode::COUNT][okvl_mode::COUNT] = {
/*req*/  //N  IS  IX   S SIX   X (granted)
/*N */    { T, T,  T,  T,  T,  T,},
/*IS*/    { T, T,  T,  T,  T,  F,},
/*IX*/    { T, T,  T,  F,  F,  F,},
/*S */    { T, T,  F,  T,  F,  F,},
/*SIX*/   { T, T,  F,  F,  F,  F,},
/*X */    { T, F,  F,  F,  F,  F,},
};

/**
*  implication_table[left][right]
*  means whether the left lock mode is implied by the right lock mode.
*/
const bool implication_table[okvl_mode::COUNT][okvl_mode::COUNT] = {
/*left*/  //N  IS  IX   S SIX   X (right)
/*N */    { T, T,  T,  T,  T,  T,},
/*IS*/    { F, T,  T,  T,  T,  T,},
/*IX*/    { F, F,  T,  F,  T,  T,},
/*S */    { F, F,  F,  T,  T,  T,},
/*SIX*/   { F, F,  F,  F,  T,  T,},
/*X */    { F, F,  F,  F,  F,  T,},
};
#undef T
#undef F

/**
*  parent_lock_modes[i] is the lock mode of parent of i
*        e.g. g_parent_lock_mode[X] is IX.
*/
const okvl_mode::element_lock_mode parent_lock_mode[] = {
    okvl_mode::N,         // N
    okvl_mode::IS,        // IS
    okvl_mode::IX,        // IX
    okvl_mode::IS,        // S
    okvl_mode::IX,        // SIX
    okvl_mode::IX         // X
};

/**
*  combined_lock_mode[requested mode][granted mode]
*  returns the lock mode after combining the two lock modes.
*  e.g., X + S = X, S + IX = SIX, etc.
*/
const okvl_mode::element_lock_mode combined_lock_modes[okvl_mode::COUNT][okvl_mode::COUNT] = {
/*req*/   //N                   IS           IX           S            SIX  X (granted)
/*N */    { okvl_mode::N,  okvl_mode::IS,  okvl_mode::IX,  okvl_mode::S,   okvl_mode::SIX, okvl_mode::X,},
/*IS*/    { okvl_mode::IS, okvl_mode::IS,  okvl_mode::IX,  okvl_mode::S,   okvl_mode::SIX, okvl_mode::X,},
/*IX*/    { okvl_mode::IX, okvl_mode::IX,  okvl_mode::IX,  okvl_mode::SIX, okvl_mode::SIX, okvl_mode::X,},
/*S */    { okvl_mode::S,  okvl_mode::S,   okvl_mode::SIX, okvl_mode::S,   okvl_mode::SIX, okvl_mode::X,},
/*SIX*/   { okvl_mode::SIX,okvl_mode::SIX, okvl_mode::SIX, okvl_mode::SIX, okvl_mode::SIX, okvl_mode::X,},
/*X */    { okvl_mode::X,  okvl_mode::X,   okvl_mode::X,   okvl_mode::X,   okvl_mode::X,   okvl_mode::X,},
};

inline bool okvl_mode::is_compatible_element(
    element_lock_mode requested,
    element_lock_mode granted) {
    return compatibility_table[requested][granted];
}

inline bool okvl_mode::is_implied_by_element(
    element_lock_mode left,
    element_lock_mode right) {
    return implication_table[left][right];
}

inline okvl_mode::okvl_mode() {
    clear();
}

inline okvl_mode::okvl_mode (const okvl_mode &r) {
    ::memcpy(modes, r.modes, OKVL_MODE_COUNT);
}

inline okvl_mode& okvl_mode::operator=(const okvl_mode& r) {
    ::memcpy(modes, r.modes, OKVL_MODE_COUNT);
    return *this;
}

inline okvl_mode::okvl_mode(element_lock_mode key_mode, element_lock_mode gap_mode) {
    clear();
    if (key_mode != N) {
        set_key_mode(key_mode);
    }
    if (gap_mode != N) {
        set_gap_mode(gap_mode);
    }
}

inline okvl_mode::okvl_mode(part_id part, element_lock_mode partition_mode) {
    clear();
    set_partition_mode(part, partition_mode);
}

inline okvl_mode::element_lock_mode okvl_mode::get_partition_mode(part_id partition) const {
    return  (element_lock_mode) modes[partition];
}

inline okvl_mode::element_lock_mode okvl_mode::get_key_mode() const {
    return  (element_lock_mode) modes[OKVL_PARTITIONS];
}

inline okvl_mode::element_lock_mode okvl_mode::get_gap_mode() const {
    return  (element_lock_mode) modes[OKVL_PARTITIONS + 1];
}

inline bool okvl_mode::is_empty() const {
    // because individual partition mode should leave an intent mode on key,
    // we can just check the key and gap.
    return (get_key_mode() == N && get_gap_mode() == N);
}

inline bool okvl_mode::is_keylock_empty() const {
    // same as above
    return (get_key_mode() == N);
}

inline bool okvl_mode::is_keylock_partition_empty() const {
    // If the key mode doesn't contain IS/IX, all individual partitions must be empty.
    return (get_key_mode() == N || get_key_mode() == S || get_key_mode() == X);
}

inline bool okvl_mode::_can_batch64(part_id part) {
    return (OKVL_PARTITIONS - part >= WORD_SIZE && part % WORD_SIZE == 0);
}

inline uint64_t okvl_mode::_get_batch64(part_id part) const {
    return reinterpret_cast<const uint64_t*>(modes)[part / WORD_SIZE];
}

inline uint64_t& okvl_mode::_get_batch64_ref(part_id part) {
    return reinterpret_cast<uint64_t*>(modes)[part / WORD_SIZE];
}

inline bool okvl_mode::contains_dirty_lock() const {
    bool ret = contains_dirty_key_lock();

    // If no X lock on key, check gap
    if (false == ret)
        return (X == get_gap_mode());

    return ret;
}

inline bool okvl_mode::contains_dirty_key_lock() const {
    if (get_key_mode() == X) {
        return true;
    } else if (get_key_mode() == IX || get_key_mode() == SIX) {
        // same above. They should have left IX in key.
        // so, we check individual partitions only in that case.
        for (part_id part = 0; part < OKVL_PARTITIONS; ++part) {
            if (_can_batch64(part) && _get_batch64(part) == 0) {
                part += sizeof(uint64_t) - 1; // skip bunch of zeros
                continue;
            } else if (get_partition_mode(part) == X) {
                return true;
            }
        }
    }
    return false;
}

inline void okvl_mode::set_partition_mode(part_id partition, element_lock_mode mode) {
    modes[partition] = (unsigned char) mode;

    // also set the intent mode to key
    element_lock_mode parent_mode = parent_lock_mode[mode];
    set_key_mode(combined_lock_modes[parent_mode][get_key_mode()]);
}

inline void okvl_mode::set_key_mode(element_lock_mode mode) {
    modes[OKVL_PARTITIONS] = mode;
}

inline void okvl_mode::set_gap_mode(element_lock_mode mode) {
    modes[OKVL_PARTITIONS + 1] = mode;
}

inline void okvl_mode::clear() {
    ::memset(modes, 0, sizeof(okvl_mode));
}

inline bool okvl_mode::is_compatible_request(
    const okvl_mode &requested) const {
    return okvl_mode::is_compatible(requested, *this);
}

inline bool okvl_mode::is_compatible_grant(
    const okvl_mode &granted) const {
    return okvl_mode::is_compatible(*this, granted);
}

inline bool okvl_mode::is_compatible(
    const okvl_mode &requested,
    const okvl_mode &granted) {
    // So far we use a straightforward for loop to check.
    // When k is small, we might want to apply some optimization,
    // but let's consider it later. Most likely this is not the major bottleneck.

    // 0. obvious optimization.
    if (requested.is_empty() || granted.is_empty()) {
        return true;
    }

    // 1. check gap. gap is totally orthogonal to any others, so this is it.
    if (!is_compatible_element(requested.get_gap_mode(), granted.get_gap_mode())) {
        return false;
    }

    // 2. check key of the request.
    // this is only key vs. key, just like intent lock mode compatibility.
    if (!is_compatible_element(requested.get_key_mode(), granted.get_key_mode())) {
        return false;
    }

    // 3. check individual partitions of the request.
    if (!requested.is_keylock_partition_empty() && !granted.is_keylock_partition_empty()) {
        for (part_id part = 0; part < OKVL_PARTITIONS; ++part) {
            if (_can_batch64(part)) {
                uint64_t requested_batch = requested._get_batch64(part);
                uint64_t granted_batch = granted._get_batch64(part);
                if (requested_batch == 0 || granted_batch == 0) {
                    part += WORD_SIZE - 1; // skip bunch of zeros
                    continue;
                }
            }
            element_lock_mode req = requested.get_partition_mode(part);
            if (!is_compatible_element(req, granted.get_partition_mode(part))) {
                return false;
            }
        }
    }

    return true;
}

inline bool okvl_mode::is_implied_by(const okvl_mode &superset) const {
    // obvious case
    element_lock_mode this_gap = get_gap_mode();
    element_lock_mode this_key = get_key_mode();
    element_lock_mode superset_gap = superset.get_gap_mode();
    element_lock_mode superset_key = superset.get_key_mode();
    if (!is_implied_by_element(this_gap, superset_gap)
        || !is_implied_by_element(this_key, superset_key)) {
        return false;
    }

    // check individual partitions.
    if (!is_keylock_partition_empty() && !superset.is_keylock_partition_empty()) {
        for (part_id part = 0; part < OKVL_PARTITIONS; ++part) {
            if (_can_batch64(part)) {
                uint64_t this_batch = _get_batch64(part);
                uint64_t superset_batch = superset._get_batch64(part);
                if (this_batch == 0 || this_batch == superset_batch) {
                    part += WORD_SIZE - 1; // skip bunch of zeros or exactly same locks
                    continue;
                }
            }
            if (!is_implied_by_element(get_partition_mode(part), superset.get_partition_mode(part))) {
                return false;
            }
        }
    }

    return true;
}

inline okvl_mode::part_id okvl_mode::compute_part_id(const void* uniquefier, int uniquefier_length) {
    const uint32_t HASH_SEED_32 = 0x35D0B891;
    const unsigned char HASH_SEED_8 = 0xDB;
    const int W = sizeof(uint32_t);
    uint64_t hash = 0;
    for (int word = 0; word < uniquefier_length / W; ++word) {
        hash = (hash * HASH_SEED_32) + reinterpret_cast<const uint32_t*>(uniquefier)[word];
    }

    for (int remaining = uniquefier_length / W * W; remaining < uniquefier_length; ++remaining) {
        hash = (hash * HASH_SEED_8) + reinterpret_cast<const unsigned char*>(uniquefier)[remaining];
    }

    // so far simply mod on hash. mod/div is expensive, but probably not an issue.
    return (okvl_mode::part_id) (hash % OKVL_PARTITIONS);
}

inline okvl_mode okvl_mode::combine(const okvl_mode& left, const okvl_mode& right) {
    // trivial optimization.
    // if either one has no individual partition mode, no need to combine them.
    if (left.is_keylock_partition_empty()) {
        okvl_mode ret(right);
        ret.set_key_mode(combined_lock_modes[left.get_key_mode()][right.get_key_mode()]);
        ret.set_gap_mode(combined_lock_modes[left.get_gap_mode()][right.get_gap_mode()]);
        return ret;
    } else if (right.is_keylock_partition_empty()) {
        okvl_mode ret(left);
        ret.set_key_mode(combined_lock_modes[left.get_key_mode()][right.get_key_mode()]);
        ret.set_gap_mode(combined_lock_modes[left.get_gap_mode()][right.get_gap_mode()]);
        return ret;
    }

    okvl_mode ret;
    for (part_id part = 0; part < OKVL_PARTITIONS; ++part) {
        if (OKVL_PARTITIONS - part >= WORD_SIZE && part % WORD_SIZE == 0) {
            uint64_t left_batch = left._get_batch64(part);
            uint64_t right_batch = right._get_batch64(part);
            if (left_batch == 0 || right_batch == 0 || left_batch == right_batch) {
                uint64_t result_batch = 0;
                if (left_batch == 0) {
                    result_batch = right_batch;
                } else if (right_batch == 0) {
                    // this means right_batch == 0 or left_batch == right_batch
                    // in either case:
                    result_batch = left_batch;
                }
                // batch-process bunch of zeros or exactly same locks
                ret._get_batch64_ref(part) = result_batch;
                part += WORD_SIZE - 1;
                continue;
            }
        }
        // here, we do not use set_partition_mode() to skip setting the intent modes on key.
        // as far as both left and right are in consistent state, just combining key alone is enough.
        ret.modes[part] = (unsigned char) combined_lock_modes[left.get_partition_mode(part)][right.get_partition_mode(part)];
    }
    ret.set_key_mode(combined_lock_modes[left.get_key_mode()][right.get_key_mode()]);
    ret.set_gap_mode(combined_lock_modes[left.get_gap_mode()][right.get_gap_mode()]);
    return ret;
}

inline bool okvl_mode::operator==(const okvl_mode& r) const {
    if (this == &r) {
        return true; // quick check in case this and r are the same object.
    } else if (get_key_mode() != r.get_key_mode()
        || get_gap_mode() != r.get_gap_mode()) {
        // another quick check.
        return false;
    } else if (is_keylock_partition_empty() && r.is_keylock_partition_empty()) {
        // then, if individual partitions are both empty, we are done.
        return true;
    }
    return ::memcmp(modes, r.modes, OKVL_PARTITIONS) == 0;
}
inline bool okvl_mode::operator!=(const okvl_mode& r) const {
    return !(operator==(r));
}

inline std::ostream& operator<<(std::ostream& o, const okvl_mode& v) {
    if (v.is_empty()) {
        o << "<Empty>";
    } else {
        for (okvl_mode::part_id part = 0; part < OKVL_PARTITIONS; ++part) {
            if (v.get_partition_mode(part) != okvl_mode::N) {
                o << "<key_" << part << "=" << element_mode_names[v.get_partition_mode(part)] << ">,";
            }
        }
        if (v.get_key_mode() != okvl_mode::N) {
            o << "<key_*=" << element_mode_names[v.get_key_mode()] << ">,";
        }
        if (v.get_gap_mode() != okvl_mode::N) {
            o << "<gap=" << element_mode_names[v.get_gap_mode()] << ">";
        }
    }
    return o;
}

#endif // W_OKVL_INL_H
