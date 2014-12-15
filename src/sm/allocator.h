#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include "w_defines.h"

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

#endif
