/****************** headers and symbols. *********************/

/* Defined if the system has the type `char_t'. */
#cmakedefine HAVE_CHAR_T
/* Defined if the system has the type `long_t'. */
#cmakedefine HAVE_LONG_T
/* Defined if the system has the type `short_t'. */
#cmakedefine HAVE_SHORT_T
/* Defined if the system has the type `uchar_t'. */
#cmakedefine HAVE_UCHAR_T
/* Defined if the system has the type `ulong_t'. */
#cmakedefine HAVE_ULONG_T
/* Defined if the system has the type `ushort_t'. */
#cmakedefine HAVE_USHORT_T

/* Defined if you have the <atomic.h> header file. */
#cmakedefine HAVE_ATOMIC_H

/* Defined if you have the <sys/mman.h> header file. */
#cmakedefine HAVE_SYS_MMAN_H

/* Defined if sys/mman.h has `MAP_ALIGN'. */
#cmakedefine HAVE_DECL_MAP_ALIGN

/* Defined if sys/mman.h has `MAP_ANON'. */
#cmakedefine HAVE_DECL_MAP_ANON

/* Defined if sys/mman.h has `MAP_ANONYMOUS'. */
#cmakedefine HAVE_DECL_MAP_ANONYMOUS

/* Defined if sys/mman.h has `MAP_FIXED'. */
#cmakedefine HAVE_DECL_MAP_FIXED

/* Defined if sys/mman.h has `MAP_NORESERVE'. */
#cmakedefine HAVE_DECL_MAP_NORESERVE

/* Defined if you have the <dirent.h> header file. */
#cmakedefine HAVE_DIRENT_H

/* Defined if you have the <fcntl.h> header file. */
#cmakedefine HAVE_FCNTL_H

/* Defined if you have the <inttypes.h> header file. */
#cmakedefine HAVE_INTTYPES_H

/* Defined if you have the <memory.h> header file. */
#cmakedefine HAVE_MEMORY_H

/* Defined if you have the <ndir.h> header file, and it defines `DIR'. */
#cmakedefine HAVE_NDIR_H

/* Defined if you have the <netdb.h> header file. */
#cmakedefine HAVE_NETDB_H

/* Defined if you have the <netinet/in.h> header file. */
#cmakedefine HAVE_NETINET_IN_H

/* Defined if you have the <semaphore.h> header file. */
#cmakedefine HAVE_SEMAPHORE_H

/* Defined if you have the <stdint.h> header file. */
#cmakedefine HAVE_STDINT_H

/* Defined if you have the <stdlib.h> header file. */
#cmakedefine HAVE_STDLIB_H

/* Defined if you have the <sys/ioctl.h> header file. */
#cmakedefine HAVE_SYS_IOCTL_H

/* Defined if you have the <sys/param.h> header file. */
#cmakedefine HAVE_SYS_PARAM_H

/* Defined if you have the <sys/stat.h> header file. */
#cmakedefine HAVE_SYS_STAT_H

/* Defined if you have the <sys/types.h> header file. */
#cmakedefine HAVE_SYS_TYPES_H

/* Defined if you have the <valgrind.h> header file. */
#cmakedefine HAVE_VALGRIND_H

/* Defined if you have the <valgrind/valgrind.h> header file. */
#cmakedefine HAVE_VALGRIND_VALGRIND_H

/* Defined if you have the <google/profiler.h> header file. */
#cmakedefine HAVE_GOOGLE_PROFILER_H

/* Defined if you have the <numa.h> header file. */
#cmakedefine HAVE_NUMA_H

/****************** std functions. *****************************/

/* Defined if you have the `vprintf' function. */
#cmakedefine HAVE_VPRINTF

/* Defined if you have the `getopt' function. */
#cmakedefine HAVE_GETOPT

/* Defined if you have the `memalign' function. */
#cmakedefine HAVE_MEMALIGN

/* Defined if you have the `membar_enter' function. */
#cmakedefine HAVE_MEMBAR_ENTER

/* Defined if you have the `memcntl' function. */
#cmakedefine HAVE_MEMCNTL

/* Defined if you have the `posix_memalign' function. */
#cmakedefine HAVE_POSIX_MEMALIGN

/* Defined if you have the `posix_spawn' function. */
#cmakedefine HAVE_POSIX_SPAWN

/* Defined if you have the `valloc' function. */
#cmakedefine HAVE_VALLOC

/* Defined if you have the `strerror' function. */
#cmakedefine HAVE_STRERROR

/****************** non-std functions. ************************/

/* Defined if you have the `clock_gettime' function in time.h or sys/time.h. */
#cmakedefine HAVE_CLOCK_GETTIME
/* Defined if you have the `gethrtime' function in time.h or sys/time.h. */
#cmakedefine HAVE_GETHRTIME
/* Defined if you have the `gettimeofday' function in time.h or sys/time.h. */
#cmakedefine HAVE_GETTIMEOFDAY

/* Defined if you have the pthread_attr_getstack function in pthread.h. */
#cmakedefine HAVE_PTHREAD_ATTR_GETSTACK
/* Defined if you have the pthread_attr_getstacksize function in pthread.h. */
#cmakedefine HAVE_PTHREAD_ATTR_GETSTACKSIZE

/* Defined if you have the `getpagesizes' function in sys/mman.h. */
#cmakedefine HAVE_GETPAGESIZES

/****************** OS-dependent sizes ************************/
/* The size of `pthread_t', as computed by sizeof. */
#define SIZEOF_PTHREAD_T ${SIZEOF_PTHREAD_T}

/* huge page size in kb. */
#cmakedefine HUGEPAGESIZE ${HUGEPAGESIZE}


/****************** parameters. *****************************/
/*for in-memory str streams*/
#cmakedefine W_USE_COMPAT_STRSTREAM

/* configured page size */
#cmakedefine SM_PAGESIZE ${SM_PAGESIZE}

/* # of bits used for dreadlock. */
#cmakedefine SM_DREADLOCK_BITCOUNT ${SM_DREADLOCK_BITCOUNT}

/* # of bits used as identity. */
#cmakedefine SM_DREADLOCK_FINGERS ${SM_DREADLOCK_FINGERS}

/* enable-pthread-mutex ? */
#cmakedefine USE_PTHREAD_MUTEX


/****************** OS/architecture. **************************/

/* 64 bits? */
#cmakedefine ARCH_LP64

/* operating system */
#cmakedefine Linux

/* operating system */
#cmakedefine MacOSX

/* operating system */
#cmakedefine SOLARIS2

/* architecture */
#cmakedefine Sparc

/* architecture */
#cmakedefine amd64

/* architecture */
#cmakedefine x86_64


/* Define WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#cmakedefine WORDS_BIGENDIAN
