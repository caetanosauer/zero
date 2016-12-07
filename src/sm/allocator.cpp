#include "allocator.h"

template<typename T>
T* sm_tls_allocator::allocate(size_t n)
{
    return (T*) malloc(n);
}

template<typename T>
void sm_tls_allocator::release(T* p, size_t)
{
    // fallback to naive allocation
    free(p);
}
