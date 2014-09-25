/*
 * (c) Copyright 2013-, Hewlett-Packard Development Company, LP
 */
#ifndef W_OKVL_H
#define W_OKVL_H

/**
 * \defgroup OKVL
 * \ingroup SSMLOCK
 * \brief \b Orthogonal \b Key \b Value \b Locking (\b OKVL).
 * \details
 *
 * \section OVERVIEW Overview
 * \e OKVL is a more concurrent and simple lock mode design compared to
 * ARIES \e KVL (Key Value Locking), Lomet's \e KRL (Key Range Locking),
 * and Graefe's \e OKRL (Orthogonal Key Range Locking).
 * It combines techniques from those previous work with
 * \e partitioning of the key. In addition to the \e key lock mode, OKVL
 * has \e k \e partition lock modes.
 *
 * So, it consists of k \e element lock modes (e.g., S and X) for
 * individual partitions, 1 element lock mode for key,
 * and 1 element lock mode for the gap between the key and the next key.
 * For example, suppose k=4 and a transaction (Xct-A) locks only the 2nd partition
 * with S; 1st=N, 2nd=S, 3rd=N, 4th=N, key=IS, gap=N.
 * Even when another transaction (Xct-B) wants to lock the key with X,
 * the locks are compatible unless Xct-B ends up hitting 2nd partition.
 *
 * We determine the partition to lock based on the \e uniquefier of the key-value entry.
 * Thus, if the index is a unique index, OKVL behaves exactly same as OKRL.
 * However, for non-unique indexes with lots of duplicate keys
 * this will dramatically increase the concurrency.
 *
 * Note that sometimes we have to lock the entire key with
 * absolute lock mode (e.g., S) rather than intent mode (e.g., IS).
 * For example, a scanning transaction has to protect all entries under the key.
 *
 * \section Uniquefier Uniquefier in non-unique indexes
 * It depends on the implementation of non-unique indexes.
 * Some database appends uniquefiers to keys (so, physically making it a unique index except
 * lock manager behavior) while other stores uniquefiers as data (thus, "value" is a list).
 * We employ the former approach, so uniquefier is the tail of each key.
 *
 * \section HOWTO How To Use
 * Include w_okvl.h.
 * This file declares the most basic enums/structs/functions for
 * the locking module based on \e OKVL (Orthogonal Key Value Locking).
 * All of them are header-only to be easily used across all modules.
 * \note Inline functions and global constants are defined in \e w_okvl_inl.h
 * to make this file easier to read. Include it if you need them.
 *
 * \section REF References
 * For further understanding, you are strongly advised to
 * read the full document about OKVL under publications/papers/TBD.
 * Or, search "Orthogonal Key Value Locking" on Google Scholar.
 * It's a full-length paper, but it must be worth reading (otherwise punch us).
 */

#include <string.h> // for memset/memcpy/memcmp
#include <stdint.h>

/**
 * \brief The number of partitions in OKVL.
 * \details
 * Must be 1 or more. If the value is 1, it behaves just like OKRL.
 * In the OKVL paper, this parameter is denoted as "k".
 * \NOTE When this value is more than 1, it should be a prime number
 * because we currently divide hashes by this number to determine
 * the partition. A simple number, say "256", would result
 * in horrible hash collisions (yes, I actually hit the issue).
 * Also, it's a good idea to keep OKVL_MODE_COUNT within some multiply of
 * 16 (for example, 61+1+1=63, which would fit in one cache line).
 * Finally, this number should be reasonably small, say at most hundreds.
 * We tested even larger numbers and observed slow downs.
 * See our paper for more details.
 * \ingroup OKVL
 */
const uint32_t OKVL_PARTITIONS = 2; // 127; // 29;

/**
 * Number of partitions, +1 for key, and +1 for gap.
 * \ingroup OKVL
 */
const uint32_t OKVL_MODE_COUNT = (OKVL_PARTITIONS + 1 + 1);

/**
 * \brief Represents a lock mode of one key entry in the \e OKVL lock manager.
 * \ingroup OKVL
 * \details
 * It consists of lock mode for \e paritions,
 * 1 \e key, and 1 \e gap after the key.
 * There are constant instances to quickly get frequently-used lock modes.
 * Otherwise, you have to instantiate this struct. Hope it fits on stack.
 */
struct okvl_mode {
    /** typedef for readability. it's just an integer. */
    typedef uint16_t part_id;

    /**
    * \enum element_lock_mode
    * \brief Lock mode for one OKVL component (key, partition, or gap).
    * \details
    * This enum must be up to 256 entries because we assume it's stored in char.
    * Unlike the original shore-mt's enum, the order of the entries imply NOTHING.
    * (well, lock modes only have partitial orders, anyways)
    */
    enum element_lock_mode {
        /** no lock                          */ N = 0,
        /** intention share (read)           */ IS,
        /** intention exclusive (write)      */ IX,
        /** share (read)                     */ S,
        /** share with intention exclusive   */ SIX,
        /** exclusive (write)                */ X,
        /** Dummy entry to tell # of entries */ COUNT,
    };

    /**
     * Each byte represents the element lock mode for a component.
     * This actually means element_lock_mode[OKVL_MODE_COUNT], but
     * explicitly uses char to make sure it's 1 byte.
     */
    unsigned char modes[OKVL_MODE_COUNT];

    /** Empty constructor that puts N for all partitions and gap. */
    okvl_mode();

    /** Copy constructor. */
    okvl_mode (const okvl_mode &r);

    /** Copy constructor. */
    okvl_mode& operator=(const okvl_mode& r);

    /** Sets only the key mode and gap mode. */
    okvl_mode(element_lock_mode key_mode, element_lock_mode gap_mode);

    /** Sets only an individual partition mode (and its intent mode on key). */
    okvl_mode(part_id part, element_lock_mode partition_mode);

    element_lock_mode get_partition_mode(part_id partition) const;
    element_lock_mode get_key_mode() const;
    element_lock_mode get_gap_mode() const;

    /** Returns whether the lock modes are completely empty (all NULL modes).*/
    bool is_empty() const;

    /** Returns whether the key modes are completely empty, either key or its partitions.*/
    bool is_keylock_empty() const;

    /** Returns whether the key modes are either empty or key-mode only (S/X in key). */
    bool is_keylock_partition_empty() const;

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

    // Return whether this contains dirty lock on key, not gap
    bool contains_dirty_key_lock() const;

    /** Sets an individual partition mode (and its intent mode on key). */
    void set_partition_mode(part_id partition, element_lock_mode mode);
    /** Sets the key mode. */
    void set_key_mode(element_lock_mode mode);
    /** Sets the gap mode. */
    void set_gap_mode(element_lock_mode mode);

    /** Clears all lock modes to be No-Lock. */
    void clear();

    /** Returns whether _this_ granted mode allows the _given_ requested mode. */
    bool is_compatible_request(const okvl_mode &requested) const;

    /** Returns whether _this_ requested mode can be allowed by the _given_ granted mode. */
    bool is_compatible_grant(const okvl_mode &granted) const;

    /**
     * Returns whether this mode is \e implied by the given mode.
     * For example, NNNN_S is implied by NXXX_S and NSSN_S is implied by NXXN_X.
     * Note that "A is not implied by  B" does NOT always mean "B is implied by A".
     * NOTE "key=S" is logically implied by "key=IS partition= all S",
     * but this function does not check it to be efficient.
     * Do not use this method if it matters.
     */
    bool is_implied_by(const okvl_mode &superset) const;

    /** operator overloads. */
    bool operator==(const okvl_mode& r) const;
    bool operator!=(const okvl_mode& r) const;

    /** Static function to tell whether the two modes are compatible. */
    static bool is_compatible(const okvl_mode &requested, const okvl_mode &granted);

    /** Static function to check if two element lock modes are compatible. */
    static bool is_compatible_element(element_lock_mode requested, element_lock_mode granted);

    /**
     * Static function to tell whether left is implied by right.
     * Note that "A is not implied by B" does NOT always mean "B is implied by A".
     */
    static bool is_implied_by_element(element_lock_mode left, element_lock_mode right);

    /** Determines the partition for the given uniquefier. */
    static part_id compute_part_id(const void* uniquefier, int uniquefier_length);

    /**
     * Returns the lock mode after combining the two lock modes.
     *  e.g., X + S = X, S + IX = SIX, etc.
     */
    static okvl_mode combine(const okvl_mode& left, const okvl_mode& right);

private:
    /** We speed up comparisons by batching multiple lock modes into this size. */
    enum const_values_enum { WORD_SIZE = sizeof(uint64_t), };
    /** Tells if we can apply the common 64bit-batching technique for the given partition. */
    static bool _can_batch64 (part_id part);
    /** Returns the 64bit-batched lock modes for the given partition. */
    uint64_t    _get_batch64 (part_id part) const;
    /** Returns the 64bit-batched lock modes for the given partition. */
    uint64_t&   _get_batch64_ref (part_id part);
};
#endif // W_OKVL_H
