/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#ifndef W_MARKABLE_POINTER_H
#define W_MARKABLE_POINTER_H

/**
 * \defgroup MARKPTR Markable Pointers
 * \brief Constitutes bit-stealing version of pointers \e to \e struct/class.
 * \ingroup IDIOMS
 * \details
 * This is a common technique to implement lock-free data structures in C/C++ [HERLIHY].
 * Because pointers to struct/class in C++ is without doubt an even integer,
 * the last bit of the pointer is always 0. We use the last bit to store additional flag
 * in the 8 byte pointer. We can then use the value for 8-byte atomic operations efficiently.
 * @see http://stackoverflow.com/questions/19389243/stealing-bits-from-a-pointer
 */

#include "w_defines.h"
#include <stdint.h>
#include <iostream>
#include <Lintel/AtomicCounter.hpp>

const uint64_t MARK_POINTER_ON              = 0x0000000000000001LL;
const uint64_t MARK_POINTER_OFF             = 0x0000000000000000LL;
const uint64_t MARK_POINTER_VALUE_MASK      = 0x0000000000000001LL;
const uint64_t MARK_POINTER_POINTER_MASK    = 0xFFFFFFFFFFFFFFFELL;

/**
 * \brief Markable Pointer
 * \ingroup MARKPTR
 * \details
 * This tiny class is completely header-only. To use, just include w_markable_pointer.h.
 */
template<class T>
class MarkablePointer {
public:
    /** Empty NULL constructor. */
    MarkablePointer() : _pointer(0) {}

    /** Constructs with initial pointer, which might be already stashed. */
    explicit MarkablePointer(T* pointer) : _pointer(cast_to_int(pointer)) {}

    /** Constructs with initial pointer and value. */
    MarkablePointer(T* pointer, bool mark) : _pointer(combine(pointer, mark)) {}

    /** Copy constructor. */
    MarkablePointer(const MarkablePointer &other) : _pointer(other._pointer) {}
    /** Copy. */
    MarkablePointer& operator=(const MarkablePointer &other) {
        _pointer = other._pointer;
        return *this;
    }

    /**
     * \b Non-atomically marks the pointer, stashing TRUE into the pointer.
     * @see atomic_attempt_mark()
     */
    void    mark() { _pointer |= MARK_POINTER_ON; }
    /** Returns if the pointer is marked in the stashed boolen flag. */
    bool    is_marked() const { return (_pointer & MARK_POINTER_VALUE_MASK) != 0; }

    /** Returns the original pointer without stashed value. */
    T*      get_pointer() const { return cast_to_ptr(_pointer & MARK_POINTER_POINTER_MASK); }
    /** Shorthand for get_pointer()->bluh. */
    T*      operator->() const { return get_pointer(); }

    bool    is_null() const { return (_pointer & MARK_POINTER_POINTER_MASK) == 0; }
    uint64_t as_int() const { return _pointer; }

    /**
     * \brief Atomic compare and set for both pointer and mark (stashed value).
     * See Figure 9.23 of [HERLIHY]
     * @param[in] expected_pointer test value for pointer
     * @param[in] new_pointer if succeeds this value is set to pointer
     * @param[in] expected_mark test value for mark
     * @param[in] new_mark if succeeds this value is set to mark
     * @return whether the CAS succeeds.
     */
    bool                atomic_cas(T* expected_pointer, T* new_pointer,
                       bool expected_mark, bool new_mark);

    /**
     * \brief Atomic swap for both pointer and mark (stashed value).
     * @param[in] new_ptr the value to set
     * @return old value before swap
     */
    MarkablePointer     atomic_swap(const MarkablePointer &new_ptr);

    /**
     * Returns an integer value that combined the pointer and the mark to stash.
     */
    static uintptr_t    combine(T* ptr, bool mark) {
        return reinterpret_cast<uintptr_t>(ptr) | (mark ? MARK_POINTER_ON : MARK_POINTER_OFF);
    }

    /** ISO/IEC 9899:2011 compliant way of casting a pointer to an int. */
    static uintptr_t    cast_to_int(T* ptr) {
        return reinterpret_cast<uintptr_t>(reinterpret_cast<void*>(ptr));
    }
    /** ISO/IEC 9899:2011 compliant way of casting an int to a pointer. */
    static T*           cast_to_ptr(uintptr_t ptr) {
        return reinterpret_cast<T*>(reinterpret_cast<void*>(ptr));
    }

private:
    /** The pointer and stashed value. */
    uintptr_t _pointer;
};

template <class T>
inline bool MarkablePointer<T>::atomic_cas(T* expected_pointer, T* desired_pointer,
                                 bool expected_mark, bool desired_mark) {
    uintptr_t expected = combine(expected_pointer, expected_mark);
    uintptr_t desired = combine(desired_pointer, desired_mark);
    return lintel::unsafe::atomic_compare_exchange_strong<uintptr_t>(
        &_pointer, &expected, desired);
}

template <class T>
inline MarkablePointer<T> MarkablePointer<T>::atomic_swap(const MarkablePointer<T>& new_ptr) {
    uintptr_t old = lintel::unsafe::atomic_exchange<uintptr_t>(&_pointer, new_ptr._pointer);
    return MarkablePointer<T>(cast_to_ptr(old));
}

template <class T>
inline std::ostream& operator<<(std::ostream &o, const MarkablePointer<T> &x) {
    o << "Markable pointer ";
    if (x.is_null()) {
        o << "<NULL>";
    } else {
        o << x.as_int() << " <marked=" << x.is_marked()
            << ", ptr=" << *(x.get_pointer()) << ">";
    }
    return o;
}

#endif // W_MARKABLE_POINTER_H
