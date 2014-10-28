#ifndef W_KEY_H
#define W_KEY_H

#include <cassert>
#include <string.h> //#include <cstring>
#include <string>
#include <ostream>
#include <iostream> // for cout
#include <stdint.h>
#include "w_endian.h"

class w_keystr_t_test;
class cvec_t;

// for now, let's use 'weird' sign bytes which is good for debugging.
// later, just use 0, 1,.. for better compression.
const unsigned char SIGN_NEGINF = 42;
const unsigned char SIGN_REGULAR = 43;
const unsigned char SIGN_POSINF = 44;

typedef uint32_t w_keystr_len_t;

/**
 * \brief Key string class which can represent a few special values.
 * \details
 * This class differs from vec_t/cvec_t mainly for two points.
 *
 * First, w_keystr_t keeps its copy of string data like std::string.
 * vec_t/cvec_t just keeps pointers to original data, which is
 * efficient but cannot be used for some cases. For example,
 * when the original data is being changed.
 * 
 * Second, w_keystr_t represents a few special values, NULL,
 * Infimum and Supremum both as on-memory data and
 * on-disk data. vec_t/cvec_t loses neg_inf/pos_inf information
 * when it's written out to disk. As we need to store infimum/supremum
 * in B+Tree fence keys, we need a new class to store key data.
 * 
 * In sum, when you just want on-memory data representation,
 * use vec_t/cvec_t. When you need to represent a key to be stored
 * on disk, or to be compared with other keys on disk, use this class.
 * 
 * This is a tiny header-only class. All functions are defined here.
 */
class w_keystr_t {
    friend class w_keystr_t_test;
    friend std::ostream& operator<<(std::ostream&, const w_keystr_t& v);

public:
    /**
     * This class  provides only this empty constructor
     * and copy constructor from another w_keystr_t.
     * This is \e by \e design to prohibit incorrect use.
     * Explicitly use construct_regularkey()/construct_from_keystr()
     * to make sure if the input already has sign bytes.
     */
    w_keystr_t ();

    w_keystr_t (const w_keystr_t &r);
    w_keystr_t& operator=(const w_keystr_t& r);
    ~w_keystr_t();
    
    /**
     * Constructs from a general string without sign byte.
     * Use this method to convert user inputs to a keystr object.
     * Do NOT use this method to copy from other keystr object.
     * You will duplicate the sign byte and mess up all comparisons!
     * To copy from existing keystr object, just use copy constructor
     * or assignment operator.
     * 
     * Usage Example:\verbatim
w_keystr_t str1;
str1.construct_regularkey ("your_key", 8);\endverbatim
     * @param nonkeystr The string WITHOUT sign byte.
     * @param length The length WITHOUT sign byte.
     * @return whether this object is constructed. false if out-of-memory.
     * @see construct_from_keystr (const void *, w_keystr_len_t)
     */
    bool construct_regularkey(const void *nonkeystr, w_keystr_len_t length);

    /** Creates an object that represents negative infinity. */
    bool construct_neginfkey();
    /** Creates an object that represents positive infinity. */
    bool construct_posinfkey();

    /**
     * Constructs from a key string WITH sign byte.
     * @param keystr The string WITH sign byte.
     * @param length The length WITH sign byte.
     * @see construct_regularkey (const void *, w_keystr_len_t)
     */
    bool construct_from_keystr(const void *keystr, w_keystr_len_t length);

    /** Used when the key string consists of prefix and suffix. */
    bool construct_from_keystr(const void *keystr_prefix, w_keystr_len_t prefix_length,
        const void *keystr_suffix, w_keystr_len_t suffix_length);

    /**
     * Used when the key string consists of prefix, poor-man's key and suffix.
     * @param total_suffix_length key length including poorman's key and suffix.
     * Note that this might be smaller than sizeof(poormkey)!
     * E.g., total_suffix_length=1: returns prefix and only the first byte of poormkey.
     */
    bool construct_from_keystr_poormkey_16(
        const void *keystr_prefix, w_keystr_len_t prefix_length,
        w_keystr_len_t total_suffix_length, uint16_t poormkey,
        const void *keystr_suffix);
    // add 32 version if needed

    /**
     * Construct from cvec_t. This is implemented in vec_t.cpp.
     * So, link to libcommon if you want to use this method.
     */
    bool construct_from_vec(const cvec_t &vect);    

    /**
     * Copy data from cvec_t without inserting the type byte. This is implemented in vec_t.cpp.
     * So, link to libcommon if you want to use this method.
     */
    bool copy_from_vec(const cvec_t &vect);    

    /**
     *  This class does NOT throw exceptions on out-of-memory.
     *  Use this function to check if it's constructed.
     * @return whether this object is constructed. false if out-of-memory.
     */
    bool is_constructed () const;
    
    /** Returns if this object represents negative infinity. */
    bool is_neginf () const;
    /** Returns if this object represents positive infinity. */
    bool is_posinf () const;
    /** Returns if this object represents non-inifinity value. */
    bool is_regular () const;

    /** Returns the count of common leading bytes with the given key string. */
    w_keystr_len_t common_leading_bytes (const w_keystr_t &r) const;

    /** Returns the count of common leading bytes with the given key string. */
    w_keystr_len_t common_leading_bytes (const unsigned char*str, w_keystr_len_t len) const;

    /** Returns the count of common leading bytes in the two given strings. */
    static  w_keystr_len_t common_leading_bytes (const unsigned char*str1, w_keystr_len_t len1, const unsigned char*str2, w_keystr_len_t len2);
    
    /**
     * Compares two strings.
     * @return 0 if equal in string and length. <0 if str1<str2 OR str1=str2 and len1<len2, >0 therwise.
     */
    static int compare_bin_str (const void* str1, int len1, const void* str2, int len2);

    /**
     * Compares this string with another w_keystr_t object.
     * @param[in] r another string to compare with
     * @return 0 if equal. <0 if this<r, >0 if this>r
     */
    int compare (const w_keystr_t &r) const;

    /**
     * Compares this string with a string data WITH sign byte
     * which should be a serialized w_keystr_t.
     * @copydoc w_keystr_t::compare(const w_keystr_t&)
     */
    int compare_keystr (const void *keystr, w_keystr_len_t length) const;

    /**
     * Compares this string with a string data WITH sign byte
     * which should be a serialized w_keystr_t.
     * Note that non-key string can't represent infinity.
     * @copydoc w_keystr_t::compare(const w_keystr_t&)
     */
    int compare_nonkeystr (const void *nonkeystr, w_keystr_len_t length) const;

    /** operator overloads. */
    inline bool operator==(const w_keystr_t& r) const;
    inline bool operator!=(const w_keystr_t& r) const;
    inline bool operator< (const w_keystr_t& r) const;
    inline bool operator> (const w_keystr_t& r) const;
    inline bool operator<=(const w_keystr_t& r) const;
    inline bool operator>=(const w_keystr_t& r) const;

    /**
     * Copy the string data of this object to the given buffer WITHOUT sign byte.
     */
    void serialize_as_nonkeystr (void *buffer) const;
    /** For convenience. Returns std::string object.*/
    std::basic_string<unsigned char> serialize_as_nonkeystr () const;
    /** length of this string WITHOUT sign byte. */
    w_keystr_len_t get_length_as_nonkeystr () const;

    /**
     * Copy the string data of this object to the given buffer WITH sign byte.
     */
    void serialize_as_keystr (void *buffer) const;
    /** Returns the internal content WITH sign byte of this key string. */
    const void* buffer_as_keystr () const;
    /** length of this string WITH sign byte. */
    w_keystr_len_t get_length_as_keystr () const;

    /** discards all data and releases all resources. */
    void clear ();
private:
    /** raw data. this internal data starts with a sign byte. */
    unsigned char *_data;
    /** length of _data, which is 1 byte longer than the actual data. */
    uint32_t _strlen;
    /** allocated length of _data, which could be larger than _strlen. */
    uint32_t _memlen;

    /** re-allocate _data for required length if needed. */
    void _assure (w_keystr_len_t required_len);
};

inline void w_keystr_t::_assure (w_keystr_len_t required_len) {
    if (_memlen >= required_len) {
        return;
    }
    if (_data != NULL) {
        delete[] _data;
    }
    _data = new unsigned char[required_len];
    if (_data != NULL) {
        _memlen = required_len;
    } else {
        _memlen = 0;
    }
}

inline w_keystr_t::w_keystr_t () : _data (NULL), _strlen (0), _memlen(0) {
}

inline w_keystr_t::w_keystr_t (const w_keystr_t &r) : _data (NULL), _strlen (r._strlen), _memlen(0) {
    assert (r._strlen == 0 || r.is_neginf() || r.is_regular() || r.is_posinf());
    if (r._data != NULL) {
        _assure (_strlen);
        if (_data != NULL) {
            ::memcpy (_data, r._data, _strlen);
        }
    }
}
inline w_keystr_t& w_keystr_t::operator=(const w_keystr_t &r) {
    assert (r._strlen == 0 || r.is_neginf() || r.is_regular() || r.is_posinf());
    _strlen = r._strlen;
    if (r._data != NULL) {
        _assure (_strlen);
        if (_data != NULL) {
            ::memcpy (_data, r._data, _strlen);
        }
    }
    return *this;
}

inline bool w_keystr_t::construct_regularkey(const void *nonkeystr, w_keystr_len_t length) {
    assert (nonkeystr != NULL);
    _strlen = length + 1;
    _assure (_strlen);
    if (_data == NULL) return false;
    _data[0] = SIGN_REGULAR;
    ::memcpy (_data + 1, nonkeystr, length);
    return true;
}

inline bool w_keystr_t::construct_neginfkey() {
    _strlen = 1;
    _assure (_strlen);
    if (_data == NULL) return false;
    _data[0] = SIGN_NEGINF;
    return true;
}

inline bool w_keystr_t::construct_posinfkey() {
    _strlen = 1;
    _assure (_strlen);
    if (_data == NULL) return false;
    _data[0] = SIGN_POSINF;
    return true;
}

// used only for asserts
inline bool _valid_signbyte (const void *keystr) {
    return ((const unsigned char*)keystr)[0] == SIGN_NEGINF  // *
        || ((const unsigned char*)keystr)[0] == SIGN_REGULAR // +
        || ((const unsigned char*)keystr)[0] == SIGN_POSINF; // ,
}

inline bool w_keystr_t::construct_from_keystr(const void *keystr, w_keystr_len_t length) {
    assert (length == 0 || keystr != NULL);
    assert (length == 0 || _valid_signbyte(keystr));
    _strlen = length;
    _assure(_strlen);
    if (_data == NULL) return false;
    ::memcpy (_data, keystr, length);
    return true;
}

inline bool w_keystr_t::construct_from_keystr(const void *keystr_prefix, w_keystr_len_t prefix_length,
        const void *keystr_suffix, w_keystr_len_t suffix_length) {
    assert (keystr_prefix != NULL);
    assert ((prefix_length == 0 && _valid_signbyte(keystr_suffix))
        || _valid_signbyte(keystr_prefix));
    assert (keystr_suffix != NULL);
    _strlen = prefix_length + suffix_length;
    _assure(_strlen);
    if (_data == NULL) return false;
    ::memcpy (_data, keystr_prefix, prefix_length);
    ::memcpy (_data + prefix_length, keystr_suffix, suffix_length);
    return true;
}

inline bool w_keystr_t::construct_from_keystr_poormkey_16(
    const void *keystr_prefix, w_keystr_len_t prefix_length,
    w_keystr_len_t total_suffix_length, uint16_t poormkey,
    const void *keystr_suffix) {
    assert (keystr_prefix != NULL);
    _strlen = prefix_length + total_suffix_length;
    _assure(_strlen);
    if (_data == NULL) return false;
    ::memcpy (_data, keystr_prefix, prefix_length);
    if (total_suffix_length > 0) {
        if (total_suffix_length < sizeof(poormkey)) {
            uint16_t tmp;
            serialize16_be(&tmp, poormkey);
            _data[prefix_length] = *reinterpret_cast<unsigned char*>(&tmp);
            assert (reinterpret_cast<unsigned char*>(&tmp)[1] == 0);
        } else {
            serialize16_be(_data + prefix_length, poormkey);
            if (total_suffix_length > sizeof(poormkey)) {
                assert (keystr_suffix != NULL);
                ::memcpy (_data + prefix_length + sizeof(poormkey), keystr_suffix, total_suffix_length - sizeof(poormkey));
            }
        }
    }
    assert (_valid_signbyte(_data));
    return true;
}

inline w_keystr_t::~w_keystr_t() {
    clear();
}

inline bool w_keystr_t::is_constructed () const {
    return _data != NULL;
}


inline int w_keystr_t::compare_bin_str (const void* str1, int len1, const void* str2, int len2) {
    int d = ::memcmp (str1, str2, (len1 <= len2 ? len1 : len2));
    // if strings are same, compare the length
    if (d == 0) d = (int) len1 - (int) len2;
    return d;
}

inline int w_keystr_t::compare (const w_keystr_t &r) const {
    return compare_keystr (r._data, r._strlen);
}
inline w_keystr_len_t w_keystr_t::common_leading_bytes (const w_keystr_t &r) const {
    assert (_data != NULL);
    assert (r._data != NULL);
    assert (_valid_signbyte(_data));
    assert (_valid_signbyte(r._data));
    return common_leading_bytes (_data, _strlen, r._data, r._strlen);
}
inline w_keystr_len_t w_keystr_t::common_leading_bytes (const unsigned char*str, w_keystr_len_t len) const {
    assert (_data != NULL);
    assert (str != NULL);
    assert (_valid_signbyte(_data));
    assert (_valid_signbyte(str));
    return common_leading_bytes (_data, _strlen, str, len);
}

inline w_keystr_len_t w_keystr_t::common_leading_bytes (const unsigned char*str1, w_keystr_len_t len1, const unsigned char* str2, w_keystr_len_t len2)
{
    assert (str1 != NULL);
    assert (str2 != NULL);
    w_keystr_len_t cmp_len = (len1 <= len2) ? len1 : len2;
    for (w_keystr_len_t i = 0; i < cmp_len; ++i) {
        if (str1[i] !=  str2[i]) {
            return i;
        }
    }
    return cmp_len;
}

inline int w_keystr_t::compare_keystr (const void *keystr, w_keystr_len_t length) const
{
    assert (_data != NULL);
    assert (keystr != NULL);
    assert (_valid_signbyte(keystr));
    return compare_bin_str (_data, _strlen, keystr, length);
}
inline int w_keystr_t::compare_nonkeystr (const void *nonkeystr, w_keystr_len_t length) const
{
    assert (_data != NULL);
    assert (nonkeystr != NULL);
    if (is_neginf()) {
        return -1;
    }
    if (is_posinf()) {
        return 1;
    }
    return compare_bin_str (_data + 1, _strlen - 1, nonkeystr, length);
}
inline void w_keystr_t::clear () {
    delete[] _data; // this is okay with _data==NULL
    _data = NULL;
    _strlen = 0;
    _memlen = 0;
}

inline bool w_keystr_t::is_neginf () const {
    if (_strlen == 0) {
        return false;
    }
    assert (is_constructed());
    return ((const char*)_data)[0] == SIGN_NEGINF;
}
inline bool w_keystr_t::is_posinf () const {
    if (_strlen == 0) {
        return false;
    }
    assert (is_constructed());
    return ((const char*)_data)[0] == SIGN_POSINF;
}
inline bool w_keystr_t::is_regular () const {
    if (_strlen == 0) {
        return false;
    }
    assert (is_constructed());
    return ((const char*)_data)[0] == SIGN_REGULAR;
}

inline void w_keystr_t::serialize_as_nonkeystr (void *buffer) const {
    assert (buffer != NULL);
    assert (is_constructed());
    assert (!is_neginf() && !is_posinf()); // these can't be serialized as non-key
    ::memcpy (buffer, _data + 1, _strlen - 1);
}
inline std::basic_string<unsigned char> w_keystr_t::serialize_as_nonkeystr () const {
    assert (is_constructed());
    assert (!is_neginf() && !is_posinf()); // these can't be serialized as non-key
    return std::basic_string<unsigned char> (_data + 1, _strlen - 1);
}
inline w_keystr_len_t w_keystr_t::get_length_as_nonkeystr () const {
    return _strlen - 1;
}

inline void w_keystr_t::serialize_as_keystr (void *buffer) const {
    if (_strlen == 0) {
        return;
    }
    assert (buffer != NULL);
    assert (is_constructed());
    ::memcpy (buffer, _data, _strlen);
}
inline const void* w_keystr_t::buffer_as_keystr () const {
    return _data;
}
inline w_keystr_len_t w_keystr_t::get_length_as_keystr () const {
    return _strlen;
}
    
inline std::ostream& operator<<(std::ostream& o, const w_keystr_t& v)
{
    if (!v.is_constructed()) {
        o << "<Not constructed>";
    } else if (v.is_neginf()) {
        o << "<Negative Infinity>";
    } else if (v.is_posinf()) {
        o << "<Positive Infinity>";
    } else {
        o << std::string(reinterpret_cast<const char*>(v._data), v._strlen); // this will not show >127 correctly, but this is just a debug function
    }
    return o;
}

inline bool w_keystr_t::operator==(const w_keystr_t& r) const { return compare(r) == 0; }
inline bool w_keystr_t::operator!=(const w_keystr_t& r) const { return compare(r) != 0; }
inline bool w_keystr_t::operator< (const w_keystr_t& r) const { return compare(r) <  0; }
inline bool w_keystr_t::operator> (const w_keystr_t& r) const { return compare(r) >  0; }
inline bool w_keystr_t::operator<=(const w_keystr_t& r) const { return compare(r) <= 0; }
inline bool w_keystr_t::operator>=(const w_keystr_t& r) const { return compare(r) >= 0; }

#endif // W_KEY_H
