/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/
/*<std-header orig-src='shore' incl-file-exclusion='DYNARRAY_CPP'>

 $Id: dynarray.cpp,v 1.3 2010/07/07 21:43:45 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

/**\cond skip */
#include <stdio.h>
#include "dynarray.h"
#include "shore-config.h"
#include <errno.h>
#include <sys/mman.h>
#include <algorithm>
#include <cstdlib>
#include <cassert>
#include <cstring>

// no system I know of *requires* larger pages than this
static size_t const MM_PAGE_SIZE = 8192;
// most systems can't handle bigger than this, and we need a sanity check
static size_t const MM_MAX_CAPACITY = MM_PAGE_SIZE*1024*1024*1024;

static size_t align_up(size_t bytes, size_t align) {
    size_t mask = align - 1;
    return (bytes+mask) &~ mask;
}

#ifdef HAVE_DECL_MAP_ALIGN 
#define USE_MAP_ALIGN 1
#endif

int dynarray::init(size_t max_size, size_t align) {
    // round up to the nearest page boundary
    max_size = align_up(max_size, MM_PAGE_SIZE);
    
    // validate inputs
    if(max_size > MM_MAX_CAPACITY)
	return EFBIG;
    if(MM_PAGE_SIZE > max_size)
	return EINVAL;
    if((align & -align) != align)
	return EINVAL;

    /*
      The magical incantation below tells mmap to reserve address
      space within the process without actually allocating any
      memory. We are then free to re-map any subset of that
      reservation using MAP_FIXED (using MAP_FIXED without a
      reservation always fails).

      Note that MAP_FIXED is smart enough to mix and match different
      sets of permissions, so we can extend the array simply by
      remapping 0..new_size with R/W permissions, and can blow
      everything away by unmapping 0..reserved_size.

      Tested on both Linux-2.6.18/x86 and Solaris-10/Sparc.
    */

    static int const PROTS = PROT_NONE;
    int flags = MAP_NORESERVE | MAP_ANON | MAP_PRIVATE;

    // Allocate extra for forced alignment (no effect for align=0)
    align = std::max(align, MM_PAGE_SIZE);
#if USE_MAP_ALIGN
    char* align_arg = (char*) align;
    size_t align_extra = 0;
    flags |= MAP_ALIGN;
#else
    char* align_arg = 0;
    size_t align_extra = align; // - MM_PAGE_SIZE;
    // fprintf(stderr, "%zx = %zx - %d\n", align_extra, align, MM_PAGE_SIZE);
#endif
    union { void* v; uintptr_t n; char* c; }
    u={mmap(align_arg, max_size+align_extra, PROTS, flags, -1, 0)};

    if(u.v == MAP_FAILED) {
        fprintf(stderr, "mmap failed: %s", strerror(errno));
        abort();
    }

    // fprintf(stderr, "mmap(%zx) -> %p\n", max_size + align_extra, u.v);

#if !USE_MAP_ALIGN
    /* Verify alignment...

       This is incredibly annoying: in all probability the system will
       give us far stronger alignment than we request, but Linux
       doesn't actually promise anything more strict than
       page-aligned. Solaris does promise to always do 1MB or 4MB
       alignment, but it also provides a MAP_ALIGN which moots the
       whole issue.

       Unfortunately Linux insists on not being helpful, so we have to
       request more than needed, then chop off the extra in a way that
       gives us the desired alignment. That extra could be the
       little bit that pushes the system over the edge and gives us
       ENOMEM, but we're kind of stuck.
     */
#ifdef TEST_ME
    std::fprintf(stderr, "start: %p	end:%p\n", u.c, u.c+max_size+align_extra);
#endif
    long aligned_base = align_up(u.n, align);
    if(long extra=aligned_base-u.n) {
#if 0
	fprintf(stderr, "chopping off %zx bytes of prefix for start: %zx ; %zx/%zx\n",
		extra, aligned_base, extra, align_extra);
#endif
	munmap(u.c, extra);
	u.n = aligned_base;
	align_extra -= extra;
    }
    if(align_extra > 0) {
#if 0
	fprintf(stderr, "chopping %zx bytes of postfix for end: %p\n", align_extra, u.c+max_size);
#endif
	munmap(u.c+max_size, align_extra);
    }
#endif

    _base = u.c;
    _capacity = max_size;
    _size = 0;
    return 0;
}

int dynarray::init(dynarray const &to_copy, size_t max_size) {
    max_size = std::max(max_size, to_copy.capacity());
    if(int err=init(max_size))
	return err;

    std::memmove(_base, to_copy._base, to_copy.size());
    return 0;
}

int dynarray::fini() {
    //    int stack_tmp;
    // fprintf(stderr, "munmap %p %d (%p)...", _base, _capacity, &stack_tmp);
    if(int err=munmap(_base, _capacity)) {
        fprintf(stderr, "munmap failed: %s", strerror(errno));
        abort();
	return err;
    }
    // fprintf(stderr, "done.\n");
    _base = 0;
    _size = 0;
    _capacity = 0;
    return 0;
}

int dynarray::resize(size_t new_size) {
    // round up to the nearest page boundary
    new_size = align_up(new_size, MM_PAGE_SIZE);

    // validate
    if(_size > new_size)
	return EINVAL;

    static int const PROTS = PROT_READ | PROT_WRITE;
    static int const FLAGS = MAP_FIXED | MAP_ANON | MAP_PRIVATE;

    // remap the new range as RW. Don't mess w/ the existing region!!
    void* result = mmap(_base+_size, new_size-_size, PROTS, FLAGS, -1, 0);
    if (result == MAP_FAILED) {
        fprintf(stderr, "mmap failed: %s", strerror(errno));
        abort();
    }
    if(result == MAP_FAILED)
	return errno;

    _size = new_size;
    return 0;
}

int dynarray::ensure_capacity(size_t min_size) {
    min_size  = align_up(min_size, MM_PAGE_SIZE);
    int err = 0;
    if(size() < min_size) {
	size_t next_size = std::max(min_size, 2*size());
	err = resize(next_size);
	
	// did we reach a limit?
	if(err == EFBIG) 
	    err = resize(min_size);
    }
    return err;
}
/**\endcond skip */

