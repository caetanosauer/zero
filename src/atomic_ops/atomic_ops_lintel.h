#ifndef	_ATOMIC_OPS_LINTEL_H
#define	_ATOMIC_OPS_LINTEL_H

#include <sys/types.h>
#include <inttypes.h>
#include <../../Lintel/include/Lintel/AtomicCounter.hpp>

/*
inline void membar_enter(void) //Aquire
{ lintel::atomic_thread_fence(lintel::memory_order_acquire); }

inline void membar_exit(void) //Release
{ lintel::atomic_thread_fence(lintel::memory_order_release); }

inline void membar_producer(void) //Release (#StoreStore = sfence)
{ lintel::atomic_thread_fence(lintel::memory_order_release); }

inline void membar_consumer(void) //Aquire (#LoadLoad = lfence
{ lintel::atomic_thread_fence(lintel::memory_order_acquire); }

inline uint32_t atomic_cas_32(volatile uint32_t * object,
			      uint32_t expected, uint32_t desired)
{
  lintel::unsafe::atomic_compare_exchange_strong<uint32_t>((uint32_t*)object, &expected, desired);
  return expected;
}

inline uint64_t atomic_cas_64(volatile uint64_t * object, uint64_t expected, uint64_t desired)
{
  lintel::unsafe::atomic_compare_exchange_strong((uint64_t*)object, &expected, desired);
  return expected;
}
*/
inline void atomic_inc_uint(volatile uint_t * object)
{ lintel::unsafe::atomic_fetch_add((uint_t*)object, 1U); }

inline void atomic_dec_uint(volatile uint_t * object)
{ lintel::unsafe::atomic_fetch_sub((uint_t*)object, 1U); }

inline void atomic_dec_32(volatile uint32_t * object)
{ lintel::unsafe::atomic_fetch_sub((uint32_t*)object, (uint32_t)1U); }


inline void atomic_add_short(volatile ushort_t * object, short operand)
{ lintel::unsafe::atomic_fetch_add((ushort_t*)object, operand); }

inline void atomic_add_32(volatile uint32_t *object, int32_t operand)
{ lintel::unsafe::atomic_fetch_add((uint32_t*)object, operand); }

inline void atomic_add_int(volatile uint_t *object, int operand)
{ lintel::unsafe::atomic_fetch_add((uint_t*)object, operand); }

inline void atomic_add_long(volatile ulong_t *object, long operand)
{ lintel::unsafe::atomic_fetch_add((ulong_t*)object, operand); }

inline uint_t atomic_add_int_nv(volatile uint_t * object, int operand)
{ return lintel::unsafe::atomic_fetch_add((uint_t*) object, operand) + operand; }

inline ulong_t atomic_add_long_nv(volatile ulong_t *object, long operand)
{ return lintel::unsafe::atomic_fetch_add((ulong_t*)object, operand) + operand; }

inline uint32_t atomic_add_32_nv(volatile uint32_t *object, int32_t operand)
{ return lintel::unsafe::atomic_fetch_add((uint32_t*) object, operand) + operand; }


inline uint32_t atomic_swap_32(volatile uint32_t *object, uint32_t desired)
{ return lintel::unsafe::atomic_exchange((uint64_t*)object, desired); }

inline void *atomic_cas_ptr(volatile void *object, void *expected, void *desired)
{
  lintel::unsafe::atomic_compare_exchange_strong((void**)object, &expected, desired);
  return expected;
}

inline void *atomic_swap_ptr(volatile void *object, void *desired)
{ return lintel::unsafe::atomic_exchange((void**)object, desired); }

#endif	/* _ATOMIC_OPS_LINTEL_H */
