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

/** Pretty name for each lock mode. */
const char* const singular_mode_names[] = {
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
#undef T
#undef F

/**
*  parent_lock_modes[i] is the lock mode of parent of i
*        e.g. g_parent_lock_mode[X] is IX.
*/
const okvl_mode::singular_lock_mode parent_lock_mode[] = {
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
const okvl_mode::singular_lock_mode combined_lock_modes[okvl_mode::COUNT][okvl_mode::COUNT] = {
/*req*/   //N                   IS           IX           S            SIX  X (granted)
/*N */    { okvl_mode::N,  okvl_mode::IS,  okvl_mode::IX,  okvl_mode::S,   okvl_mode::SIX, okvl_mode::X,},
/*IS*/    { okvl_mode::IS, okvl_mode::IS,  okvl_mode::IX,  okvl_mode::S,   okvl_mode::SIX, okvl_mode::X,},
/*IX*/    { okvl_mode::IX, okvl_mode::IX,  okvl_mode::IX,  okvl_mode::SIX, okvl_mode::SIX, okvl_mode::X,},
/*S */    { okvl_mode::S,  okvl_mode::S,   okvl_mode::SIX, okvl_mode::S,   okvl_mode::SIX, okvl_mode::X,},
/*SIX*/   { okvl_mode::SIX,okvl_mode::SIX, okvl_mode::SIX, okvl_mode::SIX, okvl_mode::SIX, okvl_mode::X,},
/*X */    { okvl_mode::X,  okvl_mode::X,   okvl_mode::X,   okvl_mode::X,   okvl_mode::X,   okvl_mode::X,},
};


inline bool okvl_mode::is_compatible_singular(
    singular_lock_mode requested,
    singular_lock_mode granted) {
    return compatibility_table[requested][granted];
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

inline okvl_mode::okvl_mode(singular_lock_mode master_mode, singular_lock_mode gap_mode) {
    clear();
    if (master_mode != N) {
        set_master_mode(master_mode);
    }
    if (gap_mode != N) {
        set_gap_mode(gap_mode);
    }
}

inline okvl_mode::okvl_mode(part_id part, singular_lock_mode partition_mode) {
    clear();
    set_partition_mode(part, partition_mode);
}

inline okvl_mode::singular_lock_mode okvl_mode::get_partition_mode(part_id partition) const {
    return  (singular_lock_mode) modes[partition];
}

inline okvl_mode::singular_lock_mode okvl_mode::get_master_mode() const {
    return  (singular_lock_mode) modes[OKVL_PARTITIONS];
}

inline okvl_mode::singular_lock_mode okvl_mode::get_gap_mode() const {
    return  (singular_lock_mode) modes[OKVL_PARTITIONS + 1];
}

inline bool okvl_mode::is_empty() const {
    // because individual partition mode should leave an intent mode on master,
    // we can just check the master and gap.
    return (get_master_mode() == N && get_gap_mode() == N);
}

inline bool okvl_mode::is_keylock_empty() const {
    // same as above
    return (get_master_mode() == N);
}

inline bool okvl_mode::contains_dirty_lock() const {
    if (get_master_mode() == X) {
        return true;
    } else if (get_master_mode() == IX || get_master_mode() == SIX) {
        // same above. They should have left IX in master.
        // so, we check individual partitions only in that case.
        for (part_id part = 0; part < OKVL_PARTITIONS; ++part) {
            if (get_partition_mode(part) == X) {
                return true;
            }
        }
    }
    return (get_gap_mode() == X);
}

inline void okvl_mode::set_partition_mode(part_id partition, singular_lock_mode mode) {
    modes[partition] = (unsigned char) mode;
    
    // also set the intent mode to master
    singular_lock_mode parent_mode = parent_lock_mode[mode];
    set_master_mode(combined_lock_modes[parent_mode][get_master_mode()]);
}

inline void okvl_mode::set_master_mode(singular_lock_mode mode) {
    modes[OKVL_PARTITIONS] = mode;
}

inline void okvl_mode::set_gap_mode(singular_lock_mode mode) {
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

    // 1. check gap. gap is totally orthogonal to any others, so this is it.
    if (!is_compatible_singular(requested.get_gap_mode(), granted.get_gap_mode())) {
        return false;
    }
    
    // 2. check master partition of the request.
    // this is only master vs. master, just like intent lock mode compatibility.
    if (!is_compatible_singular(requested.get_master_mode(), granted.get_master_mode())) {
        return false;
    }

    // 3. check usual partitions of the request.
    // Because individual partition locks should have left intent mode on master,
    // we have to do this only when both of them have some locks on master.
    if (requested.get_master_mode() != N && granted.get_master_mode() != N) {
        for (part_id partition = 0; partition < OKVL_PARTITIONS; ++partition) {
            singular_lock_mode req = requested.get_partition_mode(partition);
            if (!is_compatible_singular(req, granted.get_partition_mode(partition))) {
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
    okvl_mode ret;
    for (part_id part = 0; part < OKVL_PARTITIONS; ++part) {
        // here, we do not use set_partition_mode() to skip setting the intent modes on master.
        // as far as both left and right are in consistent state, just combining master alone is enough.
        ret.modes[part] = (unsigned char) combined_lock_modes[left.get_partition_mode(part)][right.get_partition_mode(part)];
    }
    ret.set_master_mode(combined_lock_modes[left.get_master_mode()][right.get_master_mode()]);
    ret.set_gap_mode(combined_lock_modes[left.get_gap_mode()][right.get_gap_mode()]);
    return ret;
}

inline bool okvl_mode::operator==(const okvl_mode& r) const {
    if (this == &r) {
        return true; // quick check in case this and r are the same object.
    }
    return ::memcmp(modes, r.modes, OKVL_MODE_COUNT) == 0;
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
                o << "<key_" << part << "=" << singular_mode_names[v.get_partition_mode(part)] << ">,";
            }
        }
        if (v.get_master_mode() != okvl_mode::N) {
            o << "<key_*=" << singular_mode_names[v.get_master_mode()] << ">,";
        }
        if (v.get_gap_mode() != okvl_mode::N) {
            o << "<gap=" << singular_mode_names[v.get_gap_mode()] << ">";
        }
    }
    return o;
}

#endif // W_OKVL_INL_H
