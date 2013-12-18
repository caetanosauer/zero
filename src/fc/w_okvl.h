/*
 * (c) Copyright 2013-, Hewlett-Packard Development Company, LP
 */
#ifndef W_OKVL_H
#define W_OKVL_H

/**
 * \file w_okvl.h
 * This file declares the most basic enums/structs/functions for
 * the locking module based on \e OKVL (Orthogonal Key Value Locking).
 * All of them are header-only to be easily used across all modules.
 * \note Inline functions and global constants are defined in \e w_okvl_inl.h
 * to make this file easier to read. Include it if you need them.
 */

#include <string.h> // for memset/memcpy/memcmp
#include <ostream>

/**
 * The number of partitions in OKVL.
 * Must be 1 or more. If the value is 1, it behaves just like OKRL.
 * In the OKVL paper, this parameter is denoted as "k".
 */
const int OKVL_PARTITIONS = 2;

/** # of individual partitions, +1 for "master" partition, and +1 for gap. */
const int OKVL_MODE_COUNT = (OKVL_PARTITIONS + 1 + 1);

struct w_okvl_consts;

/**
 * \brief Represents the lock mode of one key entry in the \e OKVL lock manager.
 * \details
 * It consists of lock mode for \e individual paritions,
 * 1 \e master partition, and 1 \e gap after the key.
 * There are constant instances to quickly get frequently-used lock modes.
 * Otherwise, you have to instantiate this struct. Hope it fits on stack.
 *
 * \section OKVL OKVL (Orthogonal Key Value Locking)
 * \e OKVL is a more concurrent and simple lock mode design compared to
 * ARIES \e KVL (Key Value Locking), Lomet's \e KRL (Key Range Locking),
 * and Graefe's \e OKRL (Orthogonal Key Range Locking).
 * The main difference is that OKVL \e partitions the key part of
 * the lock into \e k \e individual lock modes and their coarse level mode
 * called the \e master partition mode.
 * 
 * So, it consists of k singular lock modes (e.g., S and X) for
 * individual partitions, 1 singular lock mode for master,
 * and 1 singular lock mode for the gap between the key and the next key.
 * For example, suppose k=4 and a transaction (Xct-A) locks only the 2nd partition
 * with S; 1st=N, 2nd=S, 3rd=N, 4th=N, master=IS, gap=N.
 * Even when another transaction (Xct-B) wants to lock the key with X,
 * the locks are compatible unless Xct-B ends up hitting 2nd partition.
 * 
 * We determine the partition to lock based on the \e uniquefier of the key-value entry.
 * Thus, if the index is a unique index, OKVL behaves exactly same as OKRL.
 * However, for non-unique indexes with lots of duplicate keys
 * this will dramatically increase the concurrency.
 *
 * Note that sometimes we have to lock the master partition with
 * absolute lock mode (e.g., S) rather than intent mode (e.g., IS).
 * For example, a scanning transaction has to protect all entries under the key.
 * 
 * \section Uniquefier Uniquefier in non-unique indexes
 * It depends on the implementation of non-unique indexes.
 * Some database appends uniquefiers to keys (so, physically making it a unique index except
 * lock manager behavior) while other stores uniquefiers as data (thus, "value" is a list).
 * We employ the former approach, so uniquefier is the tail of each key.
 * 
 * \note For further understanding, you are strongly advised to
 * read the full document about OKVL under publications/papers/TBD.
 * Or, search "Orthogonal Key Value Locking" on Google Scholar.
 * It's a full-length paper, but it must be worth reading (otherwise punch us).
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

    /**
     * Each byte represents the singular lock mode for a partition or a gap.
     * This actually means singular_lock_mode[OKVL_MODE_COUNT], but
     * explicitly uses char to make sure it's 1 byte.
     */
    unsigned char modes[OKVL_MODE_COUNT];

    /** Empty constructor that puts N for all partitions and gap. */
    w_okvl();

    /** Copy constructor. */
    w_okvl (const w_okvl &r);

    /** Copy constructor. */
    w_okvl& operator=(const w_okvl& r);

    /** Sets only the master mode and gap mode. */
    w_okvl(singular_lock_mode master_mode, singular_lock_mode gap_mode);

    /** Sets only an individual partition mode (and its intent mode on master). */
    w_okvl(part_id part, singular_lock_mode partition_mode);
    
    singular_lock_mode get_partition_mode(part_id partition) const;
    singular_lock_mode get_master_mode() const;
    singular_lock_mode get_gap_mode() const;
    
    /** Returns whether the lock modes are completely empty (all NULL modes).*/
    bool is_empty() const;
    
    /** Returns whether the key modes are completely empty, either master or other partitions.*/
    bool is_keylock_empty() const;
    
    /**
     * \brief Returns whether this contains any lock mode that implies data update
     * _directly_ in the resource this lock protects (e.g., XN. XX, NX etc).
     * \details
     * We call such a lock \b Dirty \b Lock.
     * Note that IX is NOT a dirty lock. Only X (or in future increment lock)
     * itself meets the criteria.
     * This check is important for ELR and logging because they have to
     * know whether something has been (or might have been) updated or not.
     */
    bool contains_dirty_lock() const;

    /** Sets an individual partition mode (and its intent mode on master). */
    void set_partition_mode(part_id partition, singular_lock_mode mode);
    /** Sets the master mode. */
    void set_master_mode(singular_lock_mode mode);
    /** Sets the gap mode. */
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
#endif // W_OKVL_H
