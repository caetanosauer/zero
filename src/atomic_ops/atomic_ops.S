/* This will be processed only if
 * the atomic_ops are not available HAVE_ATOMIC_H
*/

#include "./../../config/shore-config.h"
#ifndef HAVE_ATOMIC_H

#if defined(ARCH_LP64)

#if defined(__sparcv9)
#include "asm_linkage/usr/src/lib/libbc/sparc/inc/sparc/asm_linkage.h"
#include "atomic_ops/sparcv9/atomic.s"

#elif defined(__i386__) || defined(i386) || defined(amd64) || defined(x86_64)
#include "atomic_ops/amd64/atomic.s"

#elif defined(IA64)
#error atomic_ops not ported to ia-64


#else
#error Atomic ops not ported to (unknown architecture)
#endif /* __sparcv9, etc */

/*ARCH_LP64*/
#endif


#if !defined(ARCH_LP64)

#if defined(I386) || defined(__i386__)
#include "asm_linkage/usr/src/uts/intel/sys/asm_linkage.h"
#include "atomic_ops/i386/atomic.s"

#elif defined(__sparcv8plus)
#include "asm_linkage/usr/src/lib/libbc/sparc/inc/sparc/asm_linkage.h"
#include "atomic_ops/sparc/atomic.s"

#else
#error Atomic ops not ported to (unknown architecture)
#endif

/*ILP32*/
#endif
/* HAVE_ATOMIC_H */
#endif
