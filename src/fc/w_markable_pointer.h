/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#ifndef W_MARKABLE_POINTER_H
#define W_MARKABLE_POINTER_H

#include "w_defines.h"
#include <stdint.h>
#include <cassert>
#include <iostream>
#include <AtomicCounter.hpp>

/**
 * \brief Lock-Free Data Structures and Algorithm
 * \defgroup LOCKFREE Lock-Free
 * \ingroup IDIOMS
 * \details
 * Classes to implemente Lock-Free Data Structures such as LockFreeList and LockFreeQueue.
 * These data structures do not cause blocking, thus are more appropriate for many-core
 * execution. They just need a few atomic operations. They might be still slow on
 * many-socket machines because of expensive inter-socket communications, but way better
 * than traditional blocking implementations.
 *
 * \section REF References
 *   \li [KOGAN11] "Wait-Free Queues With Multiple Enqueuers and Dequeuers".
 *   Alex Kogan and Erez Petrank. PPoPP'11.
 *
 *   \li [MICH02] "High Performance Dynamic Lock-Free Hash Tables and List-Based Sets".
 *   Maged M. Michael. SPAA'02.
 *
 *   \li [MICH04] "Hazard pointers: Safe memory reclamation for lock-free objects."
 *   Maged M Michael. PDS'04.
 *
 *   \li [MICH96] "Simple, Fast, and Practical Non-Blocking and Blocking Concurrent Queue
 *  Algorithms". Maged M. Michael and Michael L. Scott. PODC'96.
 *
 *   \li [HERLIHY] "The Art of Multiprocessor Programming". Maurice Herlihy, Nir Shavit.
 *   (shame if you don't own a copy yet work on lock-free algorithms!)
 */

/**
 * Type of ABA counter.
 * \ingroup LOCKFREE
 */
typedef uint16_t aba_stamp;

/**
 * \brief Bit-stealing Atomic Markable Pointer
 * \ingroup LOCKFREE
 * @tparam T Type of the pointed class/struct. Must not be unaligned types like char
 * (unless you make sure the pointed address is aligned).
 * \details
 * \section MARK Marked For Death
 * This is a common technique to implement lock-free data structures in C/C++ [HERLIHY].
 * Because pointers to struct/class in C++ is without doubt an even integer,
 * the last bit of the pointer is always 0. We use the last bit to store additional flag
 * in the 8 byte pointer. We can then use the value for 8-byte atomic operations efficiently.
 *
 * \NOTE To address a common misunderstanding of lock-free list/queue: the "mark for death"
 * bit in the pointer is NOT marking the pointed object (e.g., "next") for death.
 * It's often marking the object that holds the pointer for death because it is what
 * we want to atomically guarantee when we link/delink on the next pointer.
 * So, x->next.is_marked() means x is marked for death, not x->next.
 *
 * \section ABA ABA Counter
 * As described in Chap 10.6 of [HERLIHY], there is the \b ABA \b problem.
 * When we new/delete classes/structs, they reuse the same memory area.
 * Hence, we need to make sure the same pointer is differentiable.
 * The common solution is to implant some counter in the 64bit pointer like the mark for death.
 * Intel64/AMD64 so far uses only 48 bits (http://en.wikipedia.org/wiki/X86-64),
 * so we steal the most significant 16 bits to store a stamp value.
 * 16 bits is not perfect for avoiding ABA problems, but double-word CAS is quite
 * environment dependant. So, we pick this approach.
 * If 16 bits are not enough, garbage collector should a coordinated object reclamation
 * like what Chap 10.6 of [HERLIHY] recommends.
 *
 * \section ATOMIC Atomic Functions and Non-Atomic Functions
 * Methods whose name start with "atomic_" provide atomic semantics.
 * Other method are \e NOT atomic. So, appropriately use memory barriers to protect your
 * access. However, other methods are \e regular, meaning you will not see an utter
 * garbage but only see either the old value or new value (some valid value other thread set).
 * This is possible because this class contains only 8 bytes information.
 * In x86_64, an aligned 8 byte access is always regular though not atomic.
 *
 * When you want to call more than one non-atomic methods of MarkablePointer,
 * it's a good idea to \e copy a shared MarkablePointer variable to a local
 * MarkablePointer variable then access the local one.
 * For example,
 * \code{.cpp}
 * // This might be bad! (unless only used as inputs for atomic_cas)
 * bool marked = some_shared_markable_pointer.is_marked();
 *   ... many lines in-between
 * aba_stamp stamp = some_shared_markable_pointer.get_aba_stamp();
 * \endcode
 * \code{.cpp}
 * // This is at least regular.
 * MarkablePointer<Hoge> copied(some_shared_markable_pointer);
 * bool marked = copied.is_marked();
 *   ... many lines in-between
 * aba_stamp stamp = copied.get_aba_stamp();
 * \endcode
 * The copy/assignment operators of this class use the ACCESS_ONCE semantics to prohibit
 * compiler from doing something dangerous for this purpose.
 *
 * Still, CPU might do something and this is not atomic either.
 * In case it matters, use the atomic_cas().
 *
 * \section DEP Dependency
 * This tiny class is completely header-only. To use, just include w_markable_pointer.h.
 *
 * @see http://stackoverflow.com/questions/19389243/stealing-bits-from-a-pointer
 * @see http://www.1024cores.net/home/lock-free-algorithms/tricks/pointer-packing
 */
template<class T>
class MarkablePointer {
public:
    /** The lowest bit is stolen for mark for death. */
    static const uint64_t MARK_BIT                = 0x0000000000000001LL;
    /** The original pointer uses only 17-63th bits. */
    static const uint64_t POINTER_MASK            = 0x0000FFFFFFFFFFFELL;
    /** Bit-shift count for ABA counter. */
    static const uint64_t STAMP_SHIFT             = 48;
    /** We store a stamp value in the high 16 bits. */
    static const uint64_t STAMP_MASK              = 0xFFFF000000000000LL;

    /** Empty NULL constructor. */
    MarkablePointer() : _pointer(0) {}

    /** Constructs with initial pointer, mark and ABA stamp. */
    MarkablePointer(T* pointer, bool mark, aba_stamp stamp = 0)
        : _pointer(combine(pointer, mark, stamp)) {}

    /** Copy constructor. This is regular though might not be atomic. */
    MarkablePointer(const MarkablePointer<T> &other) {
        operator=(other);
    }
    /** Copy assignment. This is regular though might not be atomic. */
    MarkablePointer& operator=(const MarkablePointer<T> &other) {
        // ACCESS_ONCE semantics to make it at least regular.
        _pointer = static_cast<const volatile uintptr_t &>(other._pointer);
        return *this;
    }
    /**
     * [Non-atomic] Equality operator on the contained pointer value.
     */
    bool operator==(const MarkablePointer &other) const {
        return _pointer == other._pointer;
    }
    /**
     * [Non-atomic] Inequality operator on the contained pointer value.
     */
    bool operator!=(const MarkablePointer &other) const {
        return _pointer != other._pointer;
    }

    /**
     * [Non-atomic] Marks the pointer for death, stashing TRUE into the pointer.
     * @see atomic_cas()
     */
    void        set_mark(bool on) { _pointer = (_pointer & ~MARK_BIT) | (on ? MARK_BIT : 0); }
    /** [Non-atomic] Returns if the pointer is marked in the stashed boolen flag. */
    bool        is_marked() const { return (_pointer & MARK_BIT) != 0; }

    /** [Non-atomic] Returns the ABA counter. */
    aba_stamp   get_aba_stamp() const { return (_pointer & STAMP_MASK) >> STAMP_SHIFT; }
    /** [Non-atomic] Sets the ABA counter. */
    void        set_aba_stamp(aba_stamp stamp) {
        uint64_t stamp_shifted = static_cast<uint64_t>(stamp) << STAMP_SHIFT;
        _pointer = (_pointer & (~STAMP_MASK)) | stamp_shifted;
    }
    /** [Non-atomic] Increase the ABA counter by one. */
    void        increase_aba_stamp() {
        _pointer += (1LL << STAMP_SHIFT);
    }


    /** [Non-atomic] Returns the original pointer without stashed value. */
    T*          get_pointer() const { return cast_to_ptr(_pointer & POINTER_MASK); }
    /** [Non-atomic] Shorthand for get_pointer()->bluh. */
    T*          operator->() const { return get_pointer(); }

    /** [Non-atomic] Tells if the pointer is null. */
    bool        is_null() const { return (_pointer & POINTER_MASK) == 0; }
    /** [Non-atomic] Returns integer representation of the pointer and stashed values. */
    uint64_t    as_int() const { return _pointer; }

    /**
     * \brief [Atomic] Compare and set for both pointer and mark (stashed value).
     * See Figure 9.23 of [HERLIHY]
     * @param[in] expected_pointer test value for pointer
     * @param[in] new_pointer if succeeds this value is set to pointer
     * @param[in] expected_mark test value for mark
     * @param[in] new_mark if succeeds this value is set to mark
     * @param[in] expected_stamp test value for ABA stamp
     * @param[in] new_stamp if succeeds this value is set to ABA stamp
     * @return whether the CAS succeeds.
     */
    bool                atomic_cas(T* expected_pointer, T* new_pointer,
                       bool expected_mark, bool new_mark,
                       aba_stamp expected_stamp, aba_stamp new_stamp);

    /**
     * [Atomic] Overload to receive MarkablePointer.
     * @param[in] expected test value
     * @param[in] desired if succeeds this value is set
     * @return whether the CAS succeeds.
     */
    bool                atomic_cas(const MarkablePointer &expected,
                                   const MarkablePointer &desired);

    /**
     * \brief [Atomic] Swap pointer, mark, and stamp altogether.
     * @param[in] new_ptr the value to set
     * @return old value before swap
     */
    MarkablePointer     atomic_swap(const MarkablePointer &new_ptr);

    /**
     * Returns an integer value that combined the pointer and the mark to stash.
     */
    static uintptr_t    combine(T* ptr, bool mark, aba_stamp stamp) {
        assert((cast_to_int(ptr)  & (~POINTER_MASK)) == 0);
        uint64_t stamp_shifted = static_cast<uint64_t>(stamp) << STAMP_SHIFT;
        return cast_to_int(ptr) | (mark ? MARK_BIT : 0) | stamp_shifted;
    }

    /** ISO/IEC 9899:2011 compliant way of casting a pointer to an int. */
    static uintptr_t    cast_to_int(T* ptr) {
        return reinterpret_cast<uintptr_t>(reinterpret_cast<void*>(ptr));
    }
    /** ISO/IEC 9899:2011 compliant way of casting an int to a pointer. */
    static T*           cast_to_ptr(uintptr_t ptr) {
        return reinterpret_cast<T*>(reinterpret_cast<void*>(ptr));
    }

protected:
    /** The pointer and stashed flags. */
    uintptr_t _pointer;
};

/**
 * \brief Atomic Markable Pointer Chain for lock-free list/queue/etc
 * \ingroup LOCKFREE
 * @tparam NEXT Pointed class
 * \details
 * \section WHY What it is for
 * Container class like list/queue often needs an object to hold both a value
 * and a pointer to \e next such object. For example, Chap 9.8 and Chap 10.6 of [HERLIHY]
 * use "Node" class for that purpose. This is required because we need to atomically switch
 * some value and a pointer together in those lock-free algorithm. Otherwise we are risking
 * SEGFAULT when the pointed object gets revoked by other threads.
 *
 * One way to achieve it is to define a Node class in the container (as in [HERLIHY]),
 * but that means lock-free methods have to allocate and deallocate \b objects,
 * which is usually expensive and might not be lock-free!
 *
 * The solution is to have "next" MarkablePointer in the containee class itself.
 * This class is a template for such use and you can either just derive from this class.
 * Yes, it's 8 more bytes, but sometimes we need to pay the price.
 * @see http://blog.memsql.com/common-pitfalls-in-writing-lock-free-algorithms/
 * @see MarkablePointer
 *
 * \section DEP Dependency
 * This tiny class is completely header-only. To use, just include w_markable_pointer.h.
 */
template <class NEXT>
struct MarkablePointerChain {
    MarkablePointerChain() : next() {}
    MarkablePointerChain(const MarkablePointerChain& other)
        : next(other.next) {}
    MarkablePointerChain& operator=(const MarkablePointerChain& other) {
        next = other.next;
        return *this;
    }

    /** Next pointer. */
    MarkablePointer< NEXT >   next;
};

template <class T>
inline bool MarkablePointer<T>::atomic_cas(T* expected_pointer, T* desired_pointer,
    bool expected_mark, bool desired_mark, aba_stamp expected_stamp, aba_stamp new_stamp) {
    uintptr_t expected = combine(expected_pointer, expected_mark, expected_stamp);
    uintptr_t desired = combine(desired_pointer, desired_mark, new_stamp);
    return lintel::unsafe::atomic_compare_exchange_strong<uintptr_t>(
        &_pointer, &expected, desired);
}
template <class T>
inline bool MarkablePointer<T>::atomic_cas(const MarkablePointer &expected,
                                   const MarkablePointer &desired) {
    uintptr_t expected_tmp = expected._pointer;
    return lintel::unsafe::atomic_compare_exchange_strong<uintptr_t>(
        &_pointer, &expected_tmp, desired._pointer);
}

template <class T>
inline MarkablePointer<T> MarkablePointer<T>::atomic_swap(const MarkablePointer<T>& new_ptr) {
    uintptr_t old = lintel::unsafe::atomic_exchange<uintptr_t>(&_pointer, new_ptr._pointer);
    MarkablePointer<T> ret;
    ret._pointer = old;
    return ret;
}

template <class T>
inline std::ostream& operator<<(std::ostream &o, const MarkablePointer<T> &x) {
    o << "Markable pointer ";
    if (x.is_null()) {
        o << "<NULL>";
    } else {
        o << x.as_int() << " <marked=" << x.is_marked() << ", stamp=" << x.get_aba_stamp()
            << ", ptr=" << *(x.get_pointer()) << ">";
    }
    return o;
}

#endif // W_MARKABLE_POINTER_H
