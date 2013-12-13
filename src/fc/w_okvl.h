/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */
#ifndef W_OKVL_H
#define W_OKVL_H

/**
 * \file w_okvl.h
 * This file defines the most basic enums/structs/functions for
 * the locking module based on Orthogonal Key Value Locking (OKVL).
 * All of them are header-only to be easily used across all modules.
 */

#include <string.h> // for memset/memcpy/memcmp
#include <ostream>

// The number of partitions in Orthogonal Key Value Locking.
// Must be 1 or more. If the value is 1, it behaves just like OKRL.
// In the OKVL paper, this parameter is denoted as "k".
#define OKVL_PARTITIONS 1

// # of usual partitions, +1 for "wildcard" partition, and +1 for gap.
#define OKVL_MODE_COUNT (OKVL_PARTITIONS + 1 + 1)

struct w_okvl_consts;

/**
 * \brief Represents the lock mode of one key entry in the lock manager.
 * \details
 * It consists of lock mode for usual paritions, 1 wildcard partition, and 1 gap after the key.
 * Use CONSTANTS to quickly get a frequently-used lock modes.
 * Otherwise, you have to instantiate this struct. Hope it fits on stack.
 */
struct w_okvl {
    friend std::ostream& operator<<(std::ostream&, const w_okvl& v);

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

    /** Pretty name for each lock mode. */
    static const char* const singular_mode_names[COUNT];

    /**
     * \enum lock_duration_t
     * \brief Duration for locks.
     */
    enum lock_duration_t {
        t_instant     = 0,    /* released as soon as the lock is acquired */
        t_short     = 1,    /* held until end of some operation         */
        t_medium     = 2,    /* held until explicitly released           */
        t_long     = 3,    /* held until xct commits                   */
        t_very_long = 4,    /* held across xct boundaries               */
        t_num_durations = 5 /* not a duration -- used for typed comparisons */
    };

    /** Gives frequently used lock modes as const objects. */
    static const w_okvl_consts CONSTANTS;

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

    /** Empty constructor that puts N for all partitions and gap. */
    w_okvl();

    /** Copy constructor. */
    w_okvl (const w_okvl &r);

    /** Copy constructor. */
    w_okvl& operator=(const w_okvl& r);

    /** Sets only the all-partition mode and gap mode. */
    w_okvl(singular_lock_mode wildcard_mode, singular_lock_mode gap_mode);

    /** Sets only a particular partition mode. */
    w_okvl(part_id part, singular_lock_mode partition_mode);
    
    singular_lock_mode get_partition_mode(part_id partition) const;
    singular_lock_mode get_wildcard_mode() const;
    singular_lock_mode get_gap_mode() const;
    
    /** Returns whether the lock modes are completely empty (all NULL modes).*/
    bool is_empty() const;
    
    /** Returns whether the key modes are completely empty, either wildcard or other partitions.*/
    bool is_keylock_empty() const;
    
    /** Returns whether this lock modes contain any X mode in it (e.g., XN. XX, NX etc).*/
    bool contains_x_lock() const;

    void set_partition_mode(part_id partition, singular_lock_mode mode);
    void set_wildcard_mode(singular_lock_mode mode);
    void set_gap_mode(singular_lock_mode mode);

    /** Clears all lock modes to be No-Lock. */
    void clear();
    
    /** Returns whether _this_ granted mode allows the _given_ requested mode. */
    bool is_compatible_request(const w_okvl &requested) const;

    /** Returns whether _this_ requested mode can be allowed by the _given_ granted mode. */
    bool is_compatible_grant(const w_okvl &granted) const;

    /** operator overloads. */
    inline bool operator==(const w_okvl& r) const;
    inline bool operator!=(const w_okvl& r) const;

    /** Static function to tell whether the two modes are compatible. */
    static bool is_compatible(const w_okvl &requested, const w_okvl &granted);

    /** Static function to check if two lock modes are compatible. */
    static bool is_compatible_singular(singular_lock_mode requested, singular_lock_mode granted);
    
    /** Determines the partition for the given uniquefier. */
    static part_id compute_part_id(const void* uniquefier, int uniquefier_length);

    /**
     * Returns the lock mode after combining the two lock modes.
     *  e.g., X + S = X, S + IX = SIX, etc.
     */
    static w_okvl combine(const w_okvl& left, const w_okvl& right);
};

const char* const w_okvl::singular_mode_names[] = {
    "N", "IS", "IX", "S", "SIX", "X"
};

/**
* \brief Pre-defines frequently used lock modes as const objects.
* \Details This is for both convenience and performance.
* In many cases (most likely as a func param), we can just use these const static objects.
* These constants could be named like XX, SS, .. for short, but this lengthy name is more clear,
* so in long run it will help us.
*/
struct w_okvl_consts {
    const w_okvl ALL_N_GAP_N;
    const w_okvl ALL_S_GAP_S;
    const w_okvl ALL_X_GAP_X;
    const w_okvl ALL_S_GAP_N;
    const w_okvl ALL_X_GAP_N;
    const w_okvl ALL_N_GAP_S;
    const w_okvl ALL_N_GAP_X;

    w_okvl_consts() :
        ALL_N_GAP_N(w_okvl::N, w_okvl::N),
        ALL_S_GAP_S(w_okvl::S, w_okvl::S),
        ALL_X_GAP_X(w_okvl::X, w_okvl::X),
        ALL_S_GAP_N(w_okvl::S, w_okvl::N),
        ALL_X_GAP_N(w_okvl::X, w_okvl::N),
        ALL_N_GAP_S(w_okvl::N, w_okvl::S),
        ALL_N_GAP_X(w_okvl::N, w_okvl::X)
    {
    }
};

const w_okvl_consts w_okvl::CONSTANTS;

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

inline w_okvl::w_okvl() {
    clear();
}

w_okvl::w_okvl (const w_okvl &r) {
    ::memcpy(modes, r.modes, OKVL_MODE_COUNT);
}

w_okvl& w_okvl::operator=(const w_okvl& r) {
    ::memcpy(modes, r.modes, OKVL_MODE_COUNT);
    return *this;
}

inline w_okvl::w_okvl(w_okvl::singular_lock_mode wildcard_mode, w_okvl::singular_lock_mode gap_mode) {
    clear();
    if (wildcard_mode != N) {
        set_wildcard_mode(wildcard_mode);
    }
    if (gap_mode != N) {
        set_gap_mode(gap_mode);
    }
}

inline w_okvl::w_okvl(w_okvl::part_id part, w_okvl::singular_lock_mode partition_mode) {
    clear();
    set_partition_mode(part, partition_mode);
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
    for (int word = 0; word < (OKVL_MODE_COUNT) / 8; ++word) {
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

inline bool w_okvl::is_keylock_empty() const {
    // Check as 64bit int to speed up
    const int COUNT_EXCEPT_GAP = OKVL_MODE_COUNT - 1;
    for (int word = 0; word < COUNT_EXCEPT_GAP / 8; ++word) {
        if (reinterpret_cast<const int64_t*>(modes)[word] != 0) {
            return false;
        }
    }
    for (int remaining = (COUNT_EXCEPT_GAP / 8) * 8; remaining < COUNT_EXCEPT_GAP; ++remaining) {
        if (modes[remaining] != N) {
            return false;
        }
    }
    return true;
}

inline bool w_okvl::contains_x_lock() const {
    for (part_id part = 0; part < OKVL_PARTITIONS; ++part) {
        if (get_partition_mode(part) == X) {
            return true;
        }
    }
    return (get_wildcard_mode() == X || get_gap_mode() == X);
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

w_okvl w_okvl::combine(const w_okvl& left, const w_okvl& right) {
    w_okvl ret;
    for (part_id part = 0; part < OKVL_PARTITIONS; ++part) {
        ret.set_partition_mode(part, combined_lock_modes[left.get_partition_mode(part)][right.get_partition_mode(part)]);
    }
    ret.set_wildcard_mode(combined_lock_modes[left.get_wildcard_mode()][right.get_wildcard_mode()]);
    ret.set_gap_mode(combined_lock_modes[left.get_gap_mode()][right.get_gap_mode()]);
    return ret;
}

inline bool w_okvl::operator==(const w_okvl& r) const {
    return ::memcmp(modes, r.modes, OKVL_MODE_COUNT) == 0;
}
inline bool w_okvl::operator!=(const w_okvl& r) const {
    return !(operator==(r));
}

inline std::ostream& operator<<(std::ostream& o, const w_okvl& v)
{
    if (v.is_empty()) {
        o << "<Empty>";
    } else {
        for (w_okvl::part_id part = 0; part < OKVL_PARTITIONS; ++part) {
            if (v.get_partition_mode(part) != w_okvl::N) {
                o << "key_" << part << "=" << w_okvl::singular_mode_names[v.get_partition_mode(part)] << ",";
            }
        }
        if (v.get_wildcard_mode() != w_okvl::N) {
            o << "key_*=" << w_okvl::singular_mode_names[v.get_wildcard_mode()] << ",";
        }
        if (v.get_gap_mode() != w_okvl::N) {
            o << "gap=" << w_okvl::singular_mode_names[v.get_gap_mode()] << ",";
        }
    }
    return o;
}

#endif // W_OKVL_H

