#ifndef	_SYS_ATOMIC_H
#define	_SYS_ATOMIC_H
#include <sys/types.h>
#include <inttypes.h>
#ifdef	__cplusplus
extern "C" {
#endif
extern void atomic_inc_uint(volatile uint_t *);
extern void atomic_dec_32(volatile uint32_t *);
extern void atomic_dec_uint(volatile uint_t *);
extern void atomic_add_short(volatile ushort_t *, short);
extern void atomic_add_32(volatile uint32_t *, int32_t);
extern void atomic_add_int(volatile uint_t *, int);
extern void atomic_add_long(volatile ulong_t *, long);
extern uint32_t atomic_add_32_nv(volatile uint32_t *, int32_t);
extern uint_t atomic_add_int_nv(volatile uint_t *, int);
extern ulong_t atomic_add_long_nv(volatile ulong_t *, long);
extern uint32_t atomic_cas_32(volatile uint32_t *, uint32_t, uint32_t);
extern void *atomic_cas_ptr(volatile void *, void *, void *);
extern uint64_t atomic_cas_64(volatile uint64_t *, uint64_t, uint64_t);
extern uint32_t atomic_swap_32(volatile uint32_t *, uint32_t);
extern void *atomic_swap_ptr(volatile void *, void *);
extern void membar_enter(void);
extern void membar_exit(void);
extern void membar_producer(void);
extern void membar_consumer(void);
#ifdef	__cplusplus
}
#endif
#endif
