/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BTREE_VERIFY_H
#define BTREE_VERIFY_H

// classes and structs used in BTree verification
#include <map>
#include "basics.h"
#include "w_key.h"

/**
*  \brief The context object maintained throughout the verification.
* \details This object is used only for batch (in-detail) verification.
* For in-page verification, this isn't required.
*/
class verification_context {
public:
    verification_context (int hash_bits);
    ~verification_context ();

    /** bitmap to be flipped for each fact we collect. */
    char *_bitmap;
    /** byte length of _bitmap. should be multiply of 8 to make checking efficient. */
    int _bitmap_size;
    /** the number of bits we use for hashing. */
    const int _hash_bits;
    /** count of pages checked so far, for reporting purpose only. */
    int _pages_checked;
    /** count of pages that had in-page inconsistency. */
    int _pages_inconsistent;

    /** flips a bit corresponding to the specified fact. */
    void add_fact (PageID pid, int16_t level, bool high, size_t key_len, const char* key) {
        add_fact (pid, level, high, key_len, key, 0, NULL);
    }
    /** for the case the string consists of two parts (prefix and suffix). */
    void add_fact (PageID pid, int16_t level, bool high, size_t prefix_len, const char* prefix, size_t suffix_len, const char* suffix);
    /** overload for w_keystr_t. */
    void add_fact (PageID pid, int16_t level, bool high, const w_keystr_t &key) {
        add_fact (pid, level, high, key.get_length_as_keystr(), (const char*) key.buffer_as_keystr());
    }
    /** same as add_fact(), but has different function name to make the code more readable. */
    void add_expectation (PageID pid, int16_t level, bool high, size_t key_len, const char* key) {
        add_expectation (pid, level, high, key_len, key, 0, NULL);
    }
    /** for the case the string consists of two parts (prefix and suffix). */
    void add_expectation (PageID pid, int16_t level, bool high, size_t prefix_len, const char* prefix, size_t suffix_len, const char* suffix);
    /** overload for w_keystr_t. */
    void add_expectation (PageID pid, int16_t level, bool high, const w_keystr_t &key) {
        add_expectation (pid, level, high, key.get_length_as_keystr(), (const char*) key.buffer_as_keystr());
    }
    /** Returns if all bitmap entries are zero, implying that the BTree is consistent. */
    bool is_bitmap_clean () const;
private:
    static uint32_t _modify_hash (const char* data, size_t len, uint32_t hash_value);
    static uint32_t _modify_hash (uint32_t data, uint32_t hash_value);

    // prohibits default and copy constructor
    verification_context ();
    verification_context (const verification_context&);
};

/**
 * Contains all results for the volumn-level verification.
 */
class verify_volume_result {
public:
    verify_volume_result ();
    ~verify_volume_result();
    verification_context* get_or_create_context (StoreID store_id, int hash_bits);
    verification_context* get_context (StoreID store_id);

    std::map<StoreID, verification_context*> _results;
};

#endif // BTREE_VERIFY_H

