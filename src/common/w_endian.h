#ifndef ENDIAN_H
#define ENDIAN_H

#include "shore-config.h"
#include <stdint.h>

/** A few helper functions about endianness. */

// tells the endianness. Rather than using this function,
// consider #ifdef on WORDS_BIGENDIAN/WORDS_LITTLEENDIAN.
// but, sometimes this function is useful.
#ifdef WORDS_BIGENDIAN
inline bool is_big_endian() { return true; }
inline bool is_little_endian() { return false; }
// ok
#else // WORDS_BIGENDIAN
    #ifdef WORDS_LITTLEENDIAN
    inline bool is_big_endian() { return false; }
    inline bool is_little_endian() { return true; }
    #else // WORDS_LITTLEENDIAN
    // sanity check on endian definition
    #error "Huh?! Neither BigEndian nor LittleEndian? config.h seems not included in the path."
    #endif // WORDS_LITTLEENDIAN
#endif // WORDS_BIGENDIAN

// in the following function names,
// ho: the default endianness of the host machine. useful to restore the original value.
// be: means the output is ALWAYS big-endian. useful to make sure memcmp works in all architecture.
//// le: means the output is ALWAYS little-endian. probably no use. (so, not implemented)
// note that unsigned types are assumed to make sure memcmp works after serialize_be().
// if your data type is signed, convert it to unsigned type first.

/** convert a 16bit unsigned integer to big-endian byte array (which can be compared by memcmp).*/
inline void serialize16_be (void *dest, uint16_t value)
{
#ifdef WORDS_BIGENDIAN
    *reinterpret_cast<uint16_t*>(dest) = value;
#else // WORDS_BIGENDIAN
    unsigned char *b = reinterpret_cast<unsigned char*> (dest);
    b[0] = value >> 8;
    b[1] = value & 0xFF;
#endif // WORDS_BIGENDIAN
}

/** convert a 32bit unsigned integer to big-endian byte array (which can be compared by memcmp).*/
inline void serialize32_be (void *dest, uint32_t value)
{
#ifdef WORDS_BIGENDIAN
    *reinterpret_cast<uint32_t*>(dest) = value;
#else // WORDS_BIGENDIAN
    unsigned char *b = reinterpret_cast<unsigned char*> (dest);
    b[0] = value >> 24;
    b[1] = (value & 0xFF0000) >> 16;
    b[2] = (value & 0xFF00) >> 8;
    b[3] = value & 0xFF;
#endif // WORDS_BIGENDIAN
}

/** convert a 64bit unsigned integer to big-endian byte array (which can be compared by memcmp).*/
inline void serialize64_be (void *dest, uint64_t value)
{
#ifdef WORDS_BIGENDIAN
    *reinterpret_cast<uint64_t*>(dest) = value;
#else // WORDS_BIGENDIAN
    unsigned char *b = reinterpret_cast<unsigned char*> (dest);
    b[0] = value >> 56;
    b[1] = (value & 0xFF000000000000) >> 48;
    b[2] = (value & 0xFF0000000000) >> 40;
    b[3] = (value & 0xFF00000000) >> 32;
    b[4] = (value & 0xFF000000) >> 24;
    b[5] = (value & 0xFF0000) >> 16;
    b[6] = (value & 0xFF00) >> 8;
    b[7] = value & 0xFF;
#endif // WORDS_BIGENDIAN
}

/** convert a big-endian byte array  (which can be compared by memcmp) to a 16bit unsigned integer.*/
inline uint16_t deserialize16_ho (const void *buf)
{
#ifdef WORDS_BIGENDIAN
    return *reinterpret_cast<const uint16_t*>(buf);
#else // WORDS_BIGENDIAN
    const unsigned char *b = reinterpret_cast<const unsigned char*> (buf);
    return (b[0] << 8) | b[1];
#endif // WORDS_BIGENDIAN
}

/** convert a big-endian byte array  (which can be compared by memcmp) to a 32bit unsigned integer.*/
inline uint32_t deserialize32_ho (const void *buf)
{
#ifdef WORDS_BIGENDIAN
    return *reinterpret_cast<const uint32_t*>(buf);
#else // WORDS_BIGENDIAN
    const unsigned char *b = reinterpret_cast<const unsigned char*> (buf);
    return (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3];
#endif // WORDS_BIGENDIAN
}

/** convert a big-endian byte array  (which can be compared by memcmp) to a 64bit unsigned integer.*/
inline uint64_t deserialize64_ho (const void *buf)
{
#ifdef WORDS_BIGENDIAN
    return *reinterpret_cast<const uint64_t*>(buf);
#else // WORDS_BIGENDIAN
    const unsigned char *b = reinterpret_cast<const unsigned char*> (buf);
    return ((uint64_t) b[0] << 56) | ((uint64_t) b[1] << 48)
        | ((uint64_t) b[2] << 40) | ((uint64_t) b[3] << 32)
        | ((uint64_t) b[4] << 24) | ((uint64_t) b[5] << 16)
        | ((uint64_t) b[6] << 8) | (uint64_t) b[7];
#endif // WORDS_BIGENDIAN
}

// overloading for easier use. But, be VERY careful when using
// the following methods. The above methods are recommended to avoid
// unexpected type cast.
inline void serialize_be (void *dest, uint16_t value) {
    serialize16_be(dest, value);
}
inline void serialize_be (void *dest, uint32_t value) {
    serialize32_be(dest, value);
}
inline void serialize_be (void *dest, uint64_t value) {
    serialize64_be(dest, value);
}

inline void deserialize_ho(uint16_t &value) {
    value = deserialize16_ho(&value);
}
inline void deserialize_ho(uint32_t &value) {
    value = deserialize32_ho(&value);
}
inline void deserialize_ho(uint64_t &value) {
    value = deserialize64_ho(&value);
}

#endif // ENDIAN_H
