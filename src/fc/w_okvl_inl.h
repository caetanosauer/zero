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
const w_okvl ALL_N_GAP_N(w_okvl::N, w_okvl::N);
const w_okvl ALL_S_GAP_N(w_okvl::S, w_okvl::N);
const w_okvl ALL_X_GAP_N(w_okvl::X, w_okvl::N);
const w_okvl ALL_N_GAP_S(w_okvl::N, w_okvl::S);
const w_okvl ALL_S_GAP_S(w_okvl::S, w_okvl::S);
const w_okvl ALL_X_GAP_S(w_okvl::X, w_okvl::S);
const w_okvl ALL_N_GAP_X(w_okvl::N, w_okvl::X);
const w_okvl ALL_S_GAP_X(w_okvl::S, w_okvl::X);
const w_okvl ALL_X_GAP_X(w_okvl::X, w_okvl::X);

#define T true // to make following easier to read
#define F false
/**
*  compatibility_table[requested mode][granted mode]
*  means whether the requested lock mode is allowed given the already existing lock in granted mode.
*/
const bool compatibility_table[w_okvl::COUNT][w_okvl::COUNT] = {
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
const w_okvl::singular_lock_mode parent_lock_mode[] = {
    w_okvl::N,         // N
    w_okvl::IS,        // IS
    w_okvl::IX,        // IX
    w_okvl::IS,        // S
    w_okvl::IX,        // SIX
    w_okvl::IX         // X
};

/**
*  combined_lock_mode[requested mode][granted mode]
*  returns the lock mode after combining the two lock modes.
*  e.g., X + S = X, S + IX = SIX, etc.
*/
const w_okvl::singular_lock_mode combined_lock_modes[w_okvl::COUNT][w_okvl::COUNT] = {
/*req*/   //N                   IS           IX           S            SIX  X (granted)
/*N */    { w_okvl::N,  w_okvl::IS,  w_okvl::IX,  w_okvl::S,   w_okvl::SIX, w_okvl::X,},
/*IS*/    { w_okvl::IS, w_okvl::IS,  w_okvl::IX,  w_okvl::S,   w_okvl::SIX, w_okvl::X,},
/*IX*/    { w_okvl::IX, w_okvl::IX,  w_okvl::IX,  w_okvl::SIX, w_okvl::SIX, w_okvl::X,},
/*S */    { w_okvl::S,  w_okvl::S,   w_okvl::SIX, w_okvl::S,   w_okvl::SIX, w_okvl::X,},
/*SIX*/   { w_okvl::SIX,w_okvl::SIX, w_okvl::SIX, w_okvl::SIX, w_okvl::SIX, w_okvl::X,},
/*X */    { w_okvl::X,  w_okvl::X,   w_okvl::X,   w_okvl::X,   w_okvl::X,   w_okvl::X,},
};


inline bool w_okvl::is_compatible_singular(
    singular_lock_mode requested,
    singular_lock_mode granted) {
    return compatibility_table[requested][granted];
}

inline w_okvl::w_okvl() {
    clear();
}

inline w_okvl::w_okvl (const w_okvl &r) {
    ::memcpy(modes, r.modes, OKVL_MODE_COUNT);
}

inline w_okvl& w_okvl::operator=(const w_okvl& r) {
    ::memcpy(modes, r.modes, OKVL_MODE_COUNT);
    return *this;
}

inline w_okvl::w_okvl(singular_lock_mode master_mode, singular_lock_mode gap_mode) {
    clear();
    if (master_mode != N) {
        set_master_mode(master_mode);
    }
    if (gap_mode != N) {
        set_gap_mode(gap_mode);
    }
}

inline w_okvl::w_okvl(part_id part, singular_lock_mode partition_mode) {
    clear();
    set_partition_mode(part, partition_mode);
}

inline w_okvl::singular_lock_mode w_okvl::get_partition_mode(part_id partition) const {
    return  (singular_lock_mode) modes[partition];
}

inline w_okvl::singular_lock_mode w_okvl::get_master_mode() const {
    return  (singular_lock_mode) modes[OKVL_PARTITIONS];
}

inline w_okvl::singular_lock_mode w_okvl::get_gap_mode() const {
    return  (singular_lock_mode) modes[OKVL_PARTITIONS + 1];
}

inline bool w_okvl::is_empty() const {
    // because individual partition mode should leave an intent mode on master,
    // we can just check the master and gap.
    return (get_master_mode() == N && get_gap_mode() == N);
}

inline bool w_okvl::is_keylock_empty() const {
    // same as above
    return (get_master_mode() == N);
}

inline bool w_okvl::contains_dirty_lock() const {
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

inline void w_okvl::set_partition_mode(part_id partition, singular_lock_mode mode) {
    modes[partition] = (unsigned char) mode;
    
    // also set the intent mode to master
    singular_lock_mode parent_mode = parent_lock_mode[mode];
    set_master_mode(combined_lock_modes[parent_mode][get_master_mode()]);
}

inline void w_okvl::set_master_mode(singular_lock_mode mode) {
    modes[OKVL_PARTITIONS] = mode;
}

inline void w_okvl::set_gap_mode(singular_lock_mode mode) {
    modes[OKVL_PARTITIONS + 1] = mode;
}

inline void w_okvl::clear() {
    ::memset(modes, 0, sizeof(w_okvl));
}

inline bool w_okvl::is_compatible_request(
    const w_okvl &requested) const {
    return w_okvl::is_compatible(requested, *this);
}

inline bool w_okvl::is_compatible_grant(
    const w_okvl &granted) const {
    return w_okvl::is_compatible(*this, granted);
}

inline bool w_okvl::is_compatible(
    const w_okvl &requested,
    const w_okvl &granted) {
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

inline w_okvl::part_id w_okvl::compute_part_id(const void* uniquefier, int uniquefier_length) {
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
    return (w_okvl::part_id) (hash % OKVL_PARTITIONS);
}

inline w_okvl w_okvl::combine(const w_okvl& left, const w_okvl& right) {
    w_okvl ret;
    for (part_id part = 0; part < OKVL_PARTITIONS; ++part) {
        // here, we do not use set_partition_mode() to skip setting the intent modes on master.
        // as far as both left and right are in consistent state, just combining master alone is enough.
        ret.modes[part] = (unsigned char) combined_lock_modes[left.get_partition_mode(part)][right.get_partition_mode(part)];
    }
    ret.set_master_mode(combined_lock_modes[left.get_master_mode()][right.get_master_mode()]);
    ret.set_gap_mode(combined_lock_modes[left.get_gap_mode()][right.get_gap_mode()]);
    return ret;
}

inline bool w_okvl::operator==(const w_okvl& r) const {
    if (this == &r) {
        return true; // quick check in case this and r are the same object.
    }
    return ::memcmp(modes, r.modes, OKVL_MODE_COUNT) == 0;
}
inline bool w_okvl::operator!=(const w_okvl& r) const {
    return !(operator==(r));
}

inline std::ostream& operator<<(std::ostream& o, const w_okvl& v) {
    if (v.is_empty()) {
        o << "<Empty>";
    } else {
        for (w_okvl::part_id part = 0; part < OKVL_PARTITIONS; ++part) {
            if (v.get_partition_mode(part) != w_okvl::N) {
                o << "<key_" << part << "=" << singular_mode_names[v.get_partition_mode(part)] << ">,";
            }
        }
        if (v.get_master_mode() != w_okvl::N) {
            o << "<key_*=" << singular_mode_names[v.get_master_mode()] << ">,";
        }
        if (v.get_gap_mode() != w_okvl::N) {
            o << "<gap=" << singular_mode_names[v.get_gap_mode()] << ">";
        }
    }
    return o;
}

#endif // W_OKVL_INL_H
