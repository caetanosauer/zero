#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include "w_defines.h"
#include <stdexcept>
#include "tls.h"
#include "block_alloc.h"

class xct_t;
class plog_xct_t;
class logrec_t;

class sm_naive_allocator
{
public:
    void* allocate(size_t n)
    {
        return malloc(n);
    }

    void release(void* p, size_t)
    {
        free(p);
    }
};

class sm_tls_allocator
{
private:
public:

    template<typename T>
    T* allocate(size_t n);

    template<typename T>
    void release(T* p, size_t n = 0);

};

/*
 * Macros to define class-specific new and delete operators that
 * make use of smlevel_0::allocator
 */

#define DEFINE_SM_ALLOC(type) \
    void* type::operator new(size_t s) \
    {\
        return smlevel_0::allocator.allocate<type>(s);\
    }\
\
    void type::operator delete(void* p, size_t s)\
    {\
        smlevel_0::allocator.release<type>((type*) p, s);\
    }\

/**
 * C++ STL allocator that uses posix_memalign.
 * Used for direct I/O buffers.
 * Adapted from:
 * http://jmabille.github.io/blog/2014/12/06/aligned-memory-allocator/
 */
template <typename T, size_t Alignment = sizeof(T)>
struct memalign_allocator
{
    typedef T value_type;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef T& reference;
    typedef const T& const_reference;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    static_assert (Alignment > 0 && (Alignment & (Alignment - 1)) == 0,
            "Alignment argument of memalign_allocator must be power of 2");

    template <class U>
    struct rebind
    {
        typedef memalign_allocator<U, Alignment> other;
    };

    inline memalign_allocator() throw() {}
    inline memalign_allocator(const memalign_allocator&) throw() {}

    template <class U>
    inline memalign_allocator(const memalign_allocator<U, Alignment>&) throw() {}

    inline ~memalign_allocator() throw() {}

    inline pointer address(reference r) { return &r; }
    inline const_pointer address(const_reference r) const { return &r; }

    inline void construct(pointer p, const_reference value) { new (p) value_type(value); }
    inline void destroy(pointer p) { p->~value_type(); }

    inline size_type max_size() const throw() { return size_type(-1) / sizeof(T); }

    inline bool operator==(const memalign_allocator&) { return true; }
    inline bool operator!=(const memalign_allocator& rhs) { return !operator==(rhs); }

    pointer allocate(size_type count, const_pointer = 0)
    {
        pointer p = nullptr;
        int res = posix_memalign((void**) &p, Alignment, sizeof(value_type) * count);
        if (res != 0) {
            throw std::bad_alloc();
        }

        return p;
    }

    void deallocate(pointer p, size_t = 1)
    {
        free(p);
    }
};

#endif
