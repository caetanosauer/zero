/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */
#ifndef W_OKVL_H
#define W_OKVL_H

/**
 * \file w_okvl.h
 * \ingroup MACROS
 * This file defines the most basic enums/structs/functions for
 * the locking module based on Orthogonal Key Value Locking (OKVL).
 * All of them are header-only to be easily used across all modules.
 */

#include <string.h> // for memset/memcpy

// The number of partitions in Orthogonal Key Value Locking.
// Must be 1 or more. If the value is 1, it behaves just like OKRL.
// In the OKVL paper, this parameter is denoted as "k".
#define OKVL_PARTITIONS 1

// # of usual partitions, +1 for "wildcard" partition, and +1 for gap.
#define OKVL_MODE_COUNT (OKVL_PARTITIONS + 1 + 1)

/**
 * \brief Represents the lock mode of one key entry in the lock manager.
 * \details
 * It consists of lock mode for usual paritions, 1 wildcard partition, and 1 gap after the key.
 */
struct w_okvl {
    /** typedef for readability. it's just an integer. */
    typedef uint32_t part_id;

    /**
    * \enum singular_lock_mode
    * \brief Lock mode for one OKVL partition or a gap.
    * \details
    * This enum must be up to 256 entries because we assume it's stored in char.
    * Unlike the original shore-mt's enum, the order of the entries imply NOTHING.
    * (well, lock modes only have partitial orders, anyways)
    */
    enum singular_lock_mode {
        /** no lock                          */
        N = 0,

        /** intention share (read)           */
        IS,

        /** intention exclusive (write)      */
        IX,

        /** share (read)                     */
        S,

        /** share with intention exclusive   */
        SIX,

        /** exclusive (write)                */
        X,
        
        COUNT,
    };

    /**
    *  compatibility_table[requested mode][granted mode]
    *  means whether the requested lock mode is allowed given the already existing lock in granted mode.
    */
    static const bool compatibility_table[COUNT][COUNT];

    /**
    *  parent_lock_modes[i] is the lock mode of parent of i
    *        e.g. g_parent_lock_mode[X] is IX.
    */
    static const singular_lock_mode parent_lock_mode[COUNT];

    /**
    *  combined_lock_mode[requested mode][granted mode]
    *  returns the lock mode after combining the two lock modes.
    *  e.g., X + S = X, S + IX = SIX, etc.
    */
    static const singular_lock_mode combined_lock_modes[COUNT][COUNT];

    /**
     * Each byte represents the singular lock mode for a partition or a gap.
     * This actually means singular_lock_mode_t[OKVL_MODE_COUNT], but
     * explicitly uses char to make sure it's 1 byte.
     */
    unsigned char modes[OKVL_MODE_COUNT];
    
    w_okvl() {
        clear();
    }

    singular_lock_mode get_partition_mode(part_id partition) const;
    singular_lock_mode get_wildcard_mode() const;
    singular_lock_mode get_gap_mode() const;
    
    /** Returns whether the lock modes are completely empty (all NULL modes).*/
    bool is_empty() const;

    void set_partition_mode(part_id partition, singular_lock_mode mode);
    void set_wildcard_mode(singular_lock_mode mode);
    void set_gap_mode(singular_lock_mode mode);

    /** Clears all lock modes to be No-Lock. */
    void clear();
    
    /** Returns whether _this_ granted mode allows the _given_ requested mode. */
    bool is_compatible_request(const w_okvl &requested) const;

    /** Returns whether _this_ requested mode can be allowed by the _given_ granted mode. */
    bool is_compatible_grant(const w_okvl &granted) const;

    /** Static function to tell whether the two modes are compatible. */
    static bool is_compatible(const w_okvl &requested, const w_okvl &granted);

    /** Static function to check if two lock modes are compatible. */
    static bool is_compatible_singular(singular_lock_mode requested, singular_lock_mode granted);
    
    /** Determines the partition for the given uniquefier. */
    static part_id compute_part_id(const void* uniquefier, int uniquefier_length);
};

#define T true // to make following easier to read
#define F false
const bool w_okvl::compatibility_table[w_okvl::COUNT][w_okvl::COUNT] = {
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

const w_okvl::singular_lock_mode w_okvl::parent_lock_mode[] = {
    w_okvl::N,         // N
    w_okvl::IS,        // IS
    w_okvl::IX,        // IX
    w_okvl::IS,        // S
    w_okvl::IX,        // SIX
    w_okvl::IX         // X
};

const w_okvl::singular_lock_mode w_okvl::combined_lock_modes[w_okvl::COUNT][w_okvl::COUNT] = {
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

inline w_okvl::singular_lock_mode w_okvl::get_partition_mode(w_okvl::part_id partition) const {
    return  (singular_lock_mode) modes[partition];
}

inline w_okvl::singular_lock_mode w_okvl::get_wildcard_mode() const {
    return  (singular_lock_mode) modes[OKVL_PARTITIONS];
}

inline w_okvl::singular_lock_mode w_okvl::get_gap_mode() const {
    return  (singular_lock_mode) modes[OKVL_PARTITIONS + 1];
}

inline bool w_okvl::is_empty() const {
    // Check as 64bit int to speed up
    for (int word = 0; word < OKVL_MODE_COUNT / 8; ++word) {
        if (reinterpret_cast<const int64_t*>(modes)[word] != 0) {
            return false;
        }
    }

    for (int remaining = (OKVL_MODE_COUNT / 8) * 8; remaining < OKVL_MODE_COUNT; ++remaining) {
        if (modes[remaining] != N) {
            return false;
        }
    }
    
    return true;
}

inline void w_okvl::set_partition_mode(w_okvl::part_id partition, singular_lock_mode mode) {
    modes[partition] = (unsigned char) mode;
}

inline void w_okvl::set_wildcard_mode(singular_lock_mode mode) {
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

bool w_okvl::is_compatible(
    const w_okvl &requested,
    const w_okvl &granted) {
    // So far we use a straightforward for loop to check.
    // When k is small, we might want to apply some optimization,
    // but let's consider it later. Most likely this is not the major bottleneck.
    if (granted.is_empty() || requested.is_empty()) {
        return true;
    }

    // 1. check gap. gap is totally orthogonal to any others, so this is it.
    if (!is_compatible_singular(requested.get_gap_mode(), granted.get_gap_mode())) {
        return false;
    }
    
    // 2. check wildcard partition of the request.
    {
        singular_lock_mode req = requested.get_wildcard_mode();
        if (req != N) {
            // 2-A against wildcard partition of the granted.
            if (!is_compatible_singular(req, granted.get_wildcard_mode())) {
                return false;
            }
            
            // 2-B against individual partitions of the granted.
            for (part_id partition = 0; partition < OKVL_PARTITIONS; ++partition) {
                if (!is_compatible_singular(req, granted.get_partition_mode(partition))) {
                    return false;
                }
            }
        }
    }

    // 3. check usual partitions of the request.
    for (part_id partition = 0; partition < OKVL_PARTITIONS; ++partition) {
        singular_lock_mode req = requested.get_partition_mode(partition);
        if (!is_compatible_singular(req, granted.get_partition_mode(partition))) {
            return false;
        }
    }
    
    return true;
}

inline w_okvl::part_id w_okvl::compute_part_id(const void* uniquefier, int uniquefier_length) {
    const uint32_t HASH_SEED_32 = 0x35D0B891;
    const unsigned char HASH_SEED_8 = 0xDB;
    uint64_t hash = 0;
    for (int word = 0; word < uniquefier_length / 4; ++word)
    {
        hash = (hash * HASH_SEED_32) + reinterpret_cast<const uint32_t*>(uniquefier)[word];
    }

    for (int remaining = uniquefier_length / 4 * 4; remaining < uniquefier_length; ++remaining)
    {
        hash = (hash * HASH_SEED_8) + reinterpret_cast<const unsigned char*>(uniquefier)[remaining];
    }
    
    // so far simply mod on hash. mod/div is expensive, but probably not an issue.
    return (w_okvl::part_id) (hash % OKVL_PARTITIONS);
}

#endif // W_OKVL_H

