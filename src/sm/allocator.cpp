#include "allocator.h"

#include "plog_xct.h"
#include "tls.h"
#include "block_alloc.h"

DECLARE_TLS(block_pool<xct_t>, xct_pool);
DECLARE_TLS(block_pool<xct_t::xct_core>, xct_core_pool);
DECLARE_TLS(block_pool<logrec_t>, logrec_pool);
DECLARE_TLS(block_pool<plog_xct_t>, plog_xct_pool);

template<>
xct_t* sm_tls_allocator::allocate<xct_t>(size_t n)
{
    return (xct_t*) xct_pool->acquire();
}

template<>
void sm_tls_allocator::release(xct_t* p, size_t n)
{
    xct_pool->release(p);
}

template<>
xct_t::xct_core* sm_tls_allocator::allocate<xct_t::xct_core>(size_t n)
{
    return (xct_t::xct_core*) xct_core_pool->acquire();
}

template<>
void sm_tls_allocator::release(xct_t::xct_core* p, size_t n)
{
    xct_core_pool->release(p);
}

template<>
logrec_t* sm_tls_allocator::allocate(size_t n)
{
    return (logrec_t*) logrec_pool->acquire();
}

template<>
void sm_tls_allocator::release(logrec_t* p, size_t n)
{
    logrec_pool->release(p);
}

template<>
plog_xct_t* sm_tls_allocator::allocate(size_t n)
{
    return (plog_xct_t*) plog_xct_pool->acquire();
}

template<>
void sm_tls_allocator::release(plog_xct_t* p, size_t n)
{
    plog_xct_pool->release(p);
}

template<typename T>
T* sm_tls_allocator::allocate(size_t n)
{
    return (T*) malloc(n);
}

template<typename T>
void sm_tls_allocator::release(T* p, size_t n)
{
    // fallback to naive allocation
    free(p);
}
