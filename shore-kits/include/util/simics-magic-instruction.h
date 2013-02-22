/*
 * simics-magic-instruction.h
 *
 * This file is part of Virtutech Simics
 *
 * Copyright (C) 1998-2003 Virtutech AB, All Rights Reserved
 *
 * Parts of this product may be derived from systems developed at the
 * Swedish Institute of Computer Science (SICS). Licensed from SICS.
 * Copyright (C) 1991-1997, SICS, All Rights Reserved
 *
 * The Virtutech Simics API is distributed under license. Please refer to
 * the 'LICENSE' file for details.
 *
 * For documentation on the Virtutech Simics API, please refer to the
 * Simics Reference Manual. Please report any difficulties you encounter
 * with this API through the Virtutech Simics website at www.simics.com
 *
 * This file was automatically extracted from Simics source code.
 *
 *
 */

#ifndef _SIMICS_SIMICS_MAGIC_INSTRUCTION_H
#define _SIMICS_SIMICS_MAGIC_INSTRUCTION_H


#define __MAGIC_CASSERT(p) do {                                 \
        typedef int __check_magic_argument[(p) ? 1 : -1];       \
} while (0)

#if defined(__GNUC__)

#if defined(__alpha)

#define MAGIC(n) do {                                   \
	__MAGIC_CASSERT(!(n));                          \
        __asm__ __volatile__ (".long 0x70000000");      \
} while (0)

#elif defined(__sparc)

#define MAGIC(n) do {                                   \
	__MAGIC_CASSERT((n) > 0 && (n) < (1U << 22));   \
        __asm__ __volatile__ ("sethi " #n ", %g0");     \
} while (0)

#define MAGIC_BREAKPOINT MAGIC(0x40000)

#elif (defined(__i386) || defined(__x86_64))

#define MAGIC(n) do {                           \
	__MAGIC_CASSERT(!(n));                  \
        __asm__ __volatile__ ("xchg %bx,%bx");  \
} while (0)

#define MAGIC_BREAKPOINT MAGIC(0)

#elif defined(__ia64)

#define MAGIC(n) do {                                           \
	__MAGIC_CASSERT((n) >= 0 && (n) < 0x100000);            \
        __asm__ __volatile__ ("nop (0x100000 + " #n ");;");     \
} while (0)

#define MAGIC_BREAKPOINT MAGIC(0x40000)

#elif defined(__powerpc__)

#define MAGIC(n) do {                                   \
	__MAGIC_CASSERT((n) >= 0 && (n) < 32);          \
        __asm__ __volatile__ ("fmr " #n ", " #n);       \
} while (0)

#define MAGIC_BREAKPOINT MAGIC(0)

#elif defined(__arm__)

#define MAGIC(n) do {                                   \
        __MAGIC_CASSERT((n) == 0);                      \
        __asm__ __volatile__ ("orreq r0, r0, r0");      \
} while (0)

#define MAGIC_BREAKPOINT MAGIC(0)

#elif defined(__mips__)

#define MAGIC(n) do {                                   \
	__MAGIC_CASSERT((n) >= 0 && (n) <= 0xffff);     \
        __asm__ __volatile__ (".word 0x24000000+" #n);	\
} while (0)

#define MAGIC_BREAKPOINT MAGIC(0)

#else  /* !__sparc && !__i386 && !__powerpc__ */
#error "Unsupported architecture"
#endif /* !__sparc && !__i386 && !__powerpc__ */

#elif defined(__SUNPRO_C) || defined(__SUNPRO_CC)

#if defined(__sparc)

#define MAGIC(n) do {                                   \
	__MAGIC_CASSERT((n) > 0 && (n) < (1U << 22));   \
        asm ("sethi " #n ", %g0");                      \
} while (0)

#define MAGIC_BREAKPOINT MAGIC(0x40000)

#else  /* !__sparc */
#error "Unsupported architecture"
#endif /* !__sparc */

#elif defined(__DECC)

#if defined(__alpha)

#define MAGIC(n) do {                           \
	__MAGIC_CASSERT(!(n));                  \
        asm (".long 0x70000000");               \
} while (0)

#else  /* !__alpha */
#error "Unsupported architecture"
#endif /* !__alpha */

#else  /* !__GNUC__ && !__SUNPRO_C && !__SUNPRO_CC && !__DECC */

#ifdef _MSC_VER
#define MAGIC(n)
#define MAGIC_BREAKPOINT
#pragma message("MAGIC() macro needs attention!")
#else
#error "Unsupported compiler"
#endif

#endif /* !__GNUC__ && !__SUNPRO_C && !__SUNPRO_CC && !__DECC  */



#endif /* _SIMICS_SIMICS_MAGIC_INSTRUCTION_H */
