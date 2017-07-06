/*
 * MIT License
 *
 * Copyright (c) 2016 Caetano Sauer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef FOSTER_BTREE_ENCODING_H
#define FOSTER_BTREE_ENCODING_H

/**
 * \file encoding.h
 *
 * Classes and utilities to encode objets, tuples, or arbitrary key-value pairs into the format
 * accepted by the lower-level slot array, which supports fixed-length keys (usually a poor man's
 * normalized key) and uninterpreted byte sequences as payloads.
 *
 * Encoding and decoding functionality is implemented with the Encoder policy, which supports both
 * stateless and stateful encoding. The former does not maintain any internal buffers or metadata,
 * and always performs encoding/decoding on-the-fly. The latter allows for more complex
 * serialization schemes, where an intermediate buffer is required; this would be useful to encode
 * arbitrary tuples or objects, for instance.
 *
 * These functions only support scalar types for fixed-length keys and payloads and string for
 * variable length. Other variable-length types like vector result in a compilation error due to a
 * static assertion failure. This is not considered a serious restriction though, as strings can
 * easily be used to encode binary data in C++.
 */

#include <climits>
#include <type_traits>
#include <cstdint>
#include <cstring>
#include <string>
#include <tuple>

// #include "assertions.h"

/**
 * This header is copied from the GitHub repository caetanosauer/foster-btree
 *
 * Author: Caetano Sauer (caetanosauer@gmail.com)
 */

using std::string;

namespace foster {

/**
 * \brief Utility function to swap the endianness of an arbitrary input variable.
 *
 * Credit due to stackoverflow user alexandre-c
 */
template <typename T>
T swap_endianness(T u)
{
    static_assert (CHAR_BIT == 8, "CHAR_BIT != 8");

    union
    {
        T u;
        unsigned char u8[sizeof(T)];
    } source, dest;

    source.u = u;

    for (size_t k = 0; k < sizeof(T); k++) {
        dest.u8[k] = source.u8[sizeof(T) - k - 1];
    }

    return dest.u;
}

/**
 * \brief Dummy prefixing function that returns its input unaltered.
 */
template <class K>
struct NoPrefixing
{
    K operator()(const K& key) { return key; }
};

/**
 * \brief Function object used for prefixing, i.e., extracting a poor man's normalized key.
 *
 * Takes a key K, which must be of a scalar type, and extracts the appropriate poor man's normalized
 * key of type PMNK_Type.
 *
 * There are two possible behaviors with respect to endianness:
 * 1. If K and PMNK_Type are of the same size, no endianness conversion is performed. In this case,
 * the function basically behaves as a cast that maintains the same byte representation. For
 * example, a simple cast from a float 27.5 into an int would yield 27, which does not have the same
 * byte representation.
 * 2. If K is larger than PMNK_Type (as per sizeof), then the endianness must be swapped twice:
 * first to extract the sizeof(PMNK_Type) most significant bytes of the key, and then to restore the
 * original little-endian representation used for comparisons. This overhead my be substantial.
 * However, scalar keys with smaller PMNK type is not a situation for which we aim to optimize at
 * the moment. It might be best to simply use an 8-byte PMNK for an 8-byte key, for instance. This
 * also saves the overhead of encoding the rest of the key on the payload area of the slot array.
 */
template <class K, class PMNK_Type>
struct PoormanPrefixing
{
    static_assert(std::is_scalar<K>::value,
            "Encoding with poor man's normalized keys requires scalar or string key types");
    static_assert(sizeof(K) >= sizeof(PMNK_Type),
            "The type of a poor man's normalized key cannot be larger than the type of the original key");

    PMNK_Type operator()(const K& key)
    {
        union {
            K swapped;
            PMNK_Type prefix;
        };

        if (sizeof(K) >= sizeof(PMNK_Type)) {
            swapped = swap_endianness<K>(key);
            prefix = swap_endianness<PMNK_Type>(prefix);
        }
        else { // same size -- do nothing
            swapped = key;
        }
        if (std::is_signed<K>::value && std::is_unsigned<PMNK_Type>::value) {
            // when normalizing singed integer into unsigned, most significant bit must be flipped
            return prefix ^ (1 << (sizeof(PMNK_Type) * 8 - 1));
        }
        return prefix;
    }
};

/**
 * \brief Specialization of PoormanPrefixing for string keys.
 *
 * This is probably the common case, where keys are of variable length. In this case, the first
 * sizeof(PMNK_Type) bytes are extracted and converted into little-endian representation.
 */
template <class PMNK_Type>
struct PoormanPrefixing<string, PMNK_Type>
{
    PMNK_Type operator()(const string& key)
    {
        union {
            unsigned char bytes[sizeof(PMNK_Type)];
            PMNK_Type prefix;
        };

        size_t amount = sizeof(PMNK_Type);
        if (key.length() < amount) {
            prefix = 0;
            amount = key.length();
        }
        memcpy(&bytes, key.data(), amount);
        prefix = swap_endianness<PMNK_Type>(prefix);
        return prefix;
    }
};

template <class T>
class DummyEncoder
{
public:
    using Type = T;

    /** \brief Returns encoded length of a decoded value */
    static size_t get_payload_length(const T&) { return 0; }

    /** \brief Returns length of an encoded value */
    static size_t get_payload_length(void*) { return 0; }

    /** \brief Dummy encoding == do nothing */
    static char* encode(char* p, const T&) { return p; }

    /** \brief Dummy decoding == do nothing */
    static const char* decode(const char* p, T*) { return p; }
};

// template <class T>
// class AssignmentEncoder
// {
// public:
//     using Type = T;

//     /** \brief Returns encoded length of a decoded value */
//     static size_t get_payload_length(const T&) { return sizeof(T); }

//     /** \brief Returns length of an encoded value */
//     static size_t get_payload_length(void*) { return sizeof(T); }

//     /** \brief Encodes a given value into a given address using the assignment operator */
//     static char* encode(char* dest, const T& value)
//     {
//         new (dest) T {value};
//         // T* value_p = reinterpret_cast<T*>(dest);
//         // *value_p = value;
//         return dest + sizeof(T);
//     }

//     /**
//      * \brief Decodes a given memory area into a key-value pair
//      *
//      * A key or value argument given as a nullptr is not decoded. If both are nullptr, the function
//      * does nothing.
//      */
//     static const char* decode(const char* src, T* value_p)
//     {
//         const T* value_src = reinterpret_cast<const T*>(src);
//         if (value_p) {
//             *value_p = *value_src;
//         }
//         return src + sizeof(T);
//     }
// };

template <class T>
class InlineEncoder
{
public:
    using Type = T;

    static size_t get_payload_length(const T&) { return sizeof(T); }

    static size_t get_payload_length(void*) { return sizeof(T); }

    static char* encode(char* dest, const T& value)
    {
        memcpy(dest, &value, sizeof(T));
        return dest + sizeof(T);
    }

    static const char* decode(const char* src, T* value_p)
    {
        if (value_p) {
            memcpy(value_p, src, sizeof(T));
        }
        return src + sizeof(T);
    }
};

template <>
class InlineEncoder<string>
{
public:

    using Type = string;
    using LengthType = uint16_t;

    /** \brief Returns encoded length of a decoded value */
    static size_t get_payload_length(const string& value)
    {
        return sizeof(LengthType) + value.length();
    }

    /** \brief Returns length of an encoded value */
    static size_t get_payload_length(void* ptr)
    {
        return *(reinterpret_cast<LengthType*>(ptr)) + sizeof(LengthType);
    }

    /** \brief Encodes a string using a length field followed by the contents */
    static char* encode(char* dest, const string& value)
    {
        LengthType length = static_cast<LengthType>(value.length());
        *(reinterpret_cast<LengthType*>(dest)) = length;
        dest += sizeof(LengthType);
        memcpy(dest, value.data(), value.length());
        dest += value.length();
        return dest;
    }

    /**
     * \brief Decodes a given memory area into a key-value pair
     *
     * A key or value argument given as a nullptr is not decoded. If both are nullptr, the function
     * does nothing.
     */
    static const char* decode(const char* src, string* value_p)
    {
        LengthType length = *(reinterpret_cast<const LengthType*>(src));
        if (value_p) {
            value_p->assign(src + sizeof(LengthType), length);
        }
        return src + sizeof(LengthType) + length;
    }
};

/**
 * Helper calss to encode a tuple by applying a given encoder to each field
 * recursively. Encodes field N of tuple and all the following ones in
 * a recursive call. Recursion stops when N equals the tuple size.
 *
 * This is stolen from Stroustrup's book seciton 28.6.4, 4th ed.
 */
template <template <typename T> class FieldEncoder, size_t N = 0>
struct TupleEncodingHelper
{
    using NextEncoder = TupleEncodingHelper<FieldEncoder, N+1>;

    template <size_t K, typename... T>
    using FieldEnc = FieldEncoder<typename std::tuple_element<K, std::tuple<T...>>::type>;

    template <typename... T>
    static typename std::enable_if<(N < sizeof...(T)), size_t>::type
    get_payload_length(const std::tuple<T...>& t) // non-empty tuple
    {
        return NextEncoder::get_payload_length(t) +
            FieldEnc<N, T...>::get_payload_length(std::get<N>(t));
    }

    template <typename... T>
    static typename std::enable_if<!(N < sizeof...(T)), size_t>::type
    get_payload_length(const std::tuple<T...>&) // empty tuple
    {
        return 0;
    }

    template <typename... T>
    static typename std::enable_if<(N < sizeof...(T)), size_t>::type
    get_payload_length(void* ptr) // non-empty tuple
    {
        size_t flen = FieldEnc<N, T...>::get_payload_length(ptr);
        char* charptr = reinterpret_cast<char*>(ptr) + flen;
        return NextEncoder::get_payload_length(charptr);
    }

    template <typename... T>
    static typename std::enable_if<!(N < sizeof...(T)), size_t>::type
    get_payload_length(void* ptr) // empty tuple
    {
        return 0;
    }

    template <typename... T>
    static typename std::enable_if<(N < sizeof...(T)), char*>::type
    encode(char* dest, const std::tuple<T...>& t) // non-empty tuple
    {
        char* next = FieldEnc<N, T...>::encode(dest, std::get<N>(t));
        return NextEncoder::encode(next, t);
    }

    template <typename... T>
    static typename std::enable_if<!(N < sizeof...(T)), char*>::type
    encode(char* dest, const std::tuple<T...>&) // non-empty tuple
    {
        return dest;
    }

    template <typename... T>
    static typename std::enable_if<(N < sizeof...(T)), const char*>::type
    decode(const char* src, std::tuple<T...>* t) // non-empty tuple
    {
        // We can't skip decoding a tuple because we don't know the length
        // of the whole tuple before hand
        if (!t) {
            t = new std::tuple<T...>{};
        }
        const char* next = FieldEnc<N, T...>::decode(src, &std::get<N>(*t));
        return NextEncoder::decode(next, t);
    }

    template <typename... T>
    static typename std::enable_if<!(N < sizeof...(T)), const char*>::type
    decode(const char* src, std::tuple<T...>*) // non-empty tuple
    {
        return src;
    }
};

template <size_t N, template <typename T> class FieldEncoder, typename... Types>
struct VariadicEncodingHelper
{
    // generic case -- never instantiated
};

template <template <typename T> class FieldEncoder, typename... Types>
struct VariadicEncodingHelper<0, FieldEncoder, Types...>
{
    static size_t get_payload_length() { return 0; }

    static size_t get_payload_length(void* ptr) { return ptr; }

    static char* encode(char* dest) { return dest; }

    static const char* decode(const char* src) { return src; }
};

template <size_t N, template <typename T> class FieldEncoder, typename T, typename... Types>
struct VariadicEncodingHelper<N, FieldEncoder, T, Types...>
{
    using NextEncoder = VariadicEncodingHelper<N-1, FieldEncoder, Types...>;

    static size_t get_payload_length(const T& head, const Types&... tail)
    {
        return FieldEncoder<T>::get_payload_length(head) +
            NextEncoder::get_payload_length(tail...);
    }

    static size_t get_payload_length(void* ptr)
    {
        size_t flen = FieldEncoder<T>::get_payload_length(ptr);
        char* nextptr = reinterpret_cast<char*>(ptr) + flen;
        return flen + NextEncoder::get_payload_length(nextptr);
    }

    static char* encode(char* dest, const T& head, const Types&... tail)
    {
        char* nextptr = FieldEncoder<T>::encode(dest, head);
        return NextEncoder::encode(nextptr, tail...);
    }

    static const char* decode(const char* src, T* head, Types*... tail)
    {
        const char* nextptr = FieldEncoder<T>::decode(src, head);
        return NextEncoder::decode(nextptr, tail...);
    }
};

template <template <typename T> class FieldEncoder, typename... Types>
using VariadicEncoder = VariadicEncodingHelper<sizeof...(Types), FieldEncoder, Types...>;

template <>
template <typename... Types>
class InlineEncoder<std::tuple<Types...>>
{
public:

    using Tuple = std::tuple<Types...>;
    using Helper = TupleEncodingHelper<InlineEncoder>;

    static size_t get_payload_length(const Tuple& value)
    {
        return Helper::get_payload_length(value);
    }

    static size_t get_payload_length(void* ptr)
    {
        return Helper::get_payload_length(ptr);
    }

    static char* encode(char* dest, const Tuple& value)
    {
        return Helper::encode(dest, value);
    }

    static const char* decode(const char* src, Tuple* value_p)
    {
        return Helper::decode(src, value_p);
    }
};

/**
 * \brief Base class of all encoders which use a common PMNK extraction mechanism.
 */
template <class K, class PMNK_Type = K>
class PMNKEncoder
{
public:
    /**
     * The function is picked at compile time based on the type parameters. If key and PMNK are of
     * the same type, no conversion is required and the dummy NoPrefixing is used. Otherwise,
     * PoormanPrefixing, the default PMNK extractor, is chosen.
     */
    using PrefixingFunction = typename
        std::conditional<std::is_same<PMNK_Type, K>::value,
        NoPrefixing<K>,
        PoormanPrefixing<K, PMNK_Type>>::type;

    static PMNK_Type get_pmnk(const K& key)
    {
        // TODO: compiler should be able to inline this -- verify!
        return PrefixingFunction{}(key);
    }
};

template <class KeyEncoder, class ValueEncoder,
         class PMNK_Type = typename KeyEncoder::Type>
class CompoundEncoder : public PMNKEncoder<typename KeyEncoder::Type, PMNK_Type>
{
public:

    using K = typename KeyEncoder::Type;
    using V = typename ValueEncoder::Type;
    using PMNK = PMNK_Type;

    using ActualKeyEncoder = typename std::conditional<std::is_same<K, PMNK_Type>::value,
        DummyEncoder<K>, KeyEncoder>::type;

    /** \brief Returns encoded length of a key-value pair */
    static size_t get_payload_length(const K& key, const V& value)
    {
        return ActualKeyEncoder::get_payload_length(key) + ValueEncoder::get_payload_length(value);
    }

    /** \brief Returns length of an encoded payload */
    static size_t get_payload_length(void* addr)
    {
        size_t ksize = ActualKeyEncoder::get_payload_length(addr);
        char* p = reinterpret_cast<char*>(addr) + ksize;
        size_t vsize = ValueEncoder::get_payload_length(p);
        return ksize + vsize;
    }

    /** \breif Encodes a given key-value pair into a given memory area */
    static void encode(void* dest, const K& key, const V& value)
    {
        char* p = reinterpret_cast<char*>(dest);
        p = ActualKeyEncoder::encode(p, key);
        ValueEncoder::encode(p, value);
    }

    /**
     * \brief Decodes a given memory area into a key-value pair
     *
     * A key or value argument given as a nullptr is not decoded. If both are nullptr, the function
     * does nothing.
     */
    static void decode(const void* src, K* key, V* value = nullptr, const PMNK_Type* pmnk = nullptr)
    {
        const char* p = reinterpret_cast<const char*>(src);
        p = decode_key(src, key, pmnk);
        decode_value(p, value);
    }

    static char* decode_key(const void* src, K* key, const PMNK_Type* pmnk = nullptr)
    {
        const char* p = reinterpret_cast<const char*>(src);
        p = ActualKeyEncoder::decode(p, key);

        // If we are not encoding key explicitly, but just reusing PMNK, we assign it here
        if (key && std::is_same<K, PMNK_Type>::value) {
            // assert<1>(pmnk, "PMNK required to decode this key");
            *key = *pmnk;
        }

        return const_cast<char*>(p);
    }

    static void decode_value(const char* src, V* value)
    {
        ValueEncoder::decode(src, value);
    }
};

template <class K, class V, class PMNK_Type = K>
using DefaultEncoder = CompoundEncoder<InlineEncoder<K>, InlineEncoder<V>, PMNK_Type>;

/**
 * Helper type function to get a 2-argument encoder template from a 3-argument one.
 * This means that we specify only the PMNK type and let the user pick whatever K and V
 * they want.
 */
template <typename PMNK_Type>
struct GetEncoder
{
    template <typename K, typename V>
    struct type : foster::DefaultEncoder<K, V, PMNK_Type> {};
};

} // namespace foster

#endif
