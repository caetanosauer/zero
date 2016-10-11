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

/*<std-header orig-src='shore'>

 $Id: io.cpp,v 1.43 2010/08/03 14:24:52 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/


/*
 *   NewThreads is Copyright 1992, 1993, 1994, 1995, 1996, 1997 by:
 *
 *    Josef Burger    <bolo@cs.wisc.edu>
 *    Dylan McNamee    <dylan@cse.ogi.edu>
 *      Ed Felten       <felten@cs.princeton.edu>
 *
 *   All Rights Reserved.
 *
 *   NewThreads may be freely used as long as credit is given
 *   to the above authors and the above copyright is maintained.
 */

/**\cond skip */
#define    IO_C

#include <w.h>
#include <w_debug.h>
#include <w_stream.h>
#include <cstdlib>
#include <cstring>
#include <sys/time.h>
#include <sys/wait.h>
#include <new>
#include <sys/stat.h>
#include <sys/mman.h>
#if defined(HAVE_HUGETLBFS)
#include <fcntl.h>
#endif
#include "sthread.h"
#include "sthread_stats.h"
#include <sdisk.h>
#include <sdisk_unix.h>

#if defined(HUGEPAGESIZE) && (HUGEPAGESIZE == 0)
#undef HUGEPAGESIZE
#endif

extern class sthread_stats SthreadStats;

std::vector<sdisk_t*> sthread_t::_disks;
unsigned        sthread_t::open_max = 0;
unsigned        sthread_t::open_count = 0;

static          queue_based_lock_t    protectFDs;

int       sthread_t:: _disk_buffer_disalignment(0);
size_t    sthread_t:: _disk_buffer_size(0);
char *    sthread_t:: _disk_buffer (NULL);

int sthread_t::do_unmap()
{
    // munmap isn't strictly necessary since this will
    // cause the sm to croak and the mapping will be done at
    // process-end

#ifdef WITHOUT_MMAP
    ::free( _disk_buffer - _disk_buffer_disalignment );
    _disk_buffer = NULL;
    _disk_buffer_disalignment = 0;
    return 0;
#endif

#if 0
    fprintf(stderr, "%d: munmap disalignment %d addr %p, size %lu\n",
            __LINE__,
            _disk_buffer_disalignment,
            ( _disk_buffer -  _disk_buffer_disalignment),
            _disk_buffer_size);
#endif

    int err =
        munmap( _disk_buffer -  _disk_buffer_disalignment,  _disk_buffer_size);

    if(err) {
        cerr << "munmap returns " << err
            << " errno is " <<  errno  << " " << strerror(errno)
            << endl;
        w_assert1(!err);
    }

     _disk_buffer = NULL;
     _disk_buffer_size = 0;
     _disk_buffer_disalignment = 0;

    return err;
}

void sthread_t::align_for_sm(size_t requested_size)
{
    char * _disk_buffer2  = (char *)alignon( _disk_buffer, SM_PAGESIZE);
    if( _disk_buffer2 !=  _disk_buffer)
    {
        // We made the size big enough that we can align it here
        _disk_buffer_disalignment = ( _disk_buffer2 -  _disk_buffer);
        w_assert1( _disk_buffer_disalignment < SM_PAGESIZE);
        w_assert1( _disk_buffer_size -  _disk_buffer_disalignment
            >= requested_size);

         _disk_buffer =  _disk_buffer2;

    }
}

long sthread_t::get_max_page_size(long system_page_size)
{
    long max_page_size = 0;
#ifdef HAVE_GETPAGESIZES
    {
        int nelem = getpagesizes(NULL, 0);
        if(nelem >= 0) {
            size_t *pagesize = new size_t[nelem];
            int err = getpagesizes (pagesize, nelem);
            if(err >= 0) {
                for(int i=0; i < nelem; i++) {
                   if ( pagesize[i] > max_page_size) {
                       max_page_size = pagesize[i];
           }
                }
            } else {
           cerr << "getpagesizes(pagesize, " << nelem << ") failed. "
            << " errno is " <<  errno  << " " << strerror(errno)
            << endl;
            }
            delete[] pagesize;
        } else {
           cerr << "getpagesizes(NULL,0) failed. "
            << " errno is " <<  errno  << " " << strerror(errno)
           << endl;
        }
    }
#else
    max_page_size = system_page_size;
#endif
    /*
    cerr << "Max    page size is " << max_page_size
        << "( " << int(max_page_size/1024) << " KB) " << endl;
    cerr << "System page size is " << system_page_size
        << "( " << int(system_page_size/1024) << " KB) " << endl;
    */
    return max_page_size;
}

void sthread_t::align_bufsize(size_t size, long system_page_size,
                                                long max_page_size)
{
    // ***********************************************************
    //
    //  PROPERLY ALIGN ARGUMENTS TO MMAP
    //
    // The max page size should be a multiple of the system page size -
    // that should be a given.

    w_assert0(alignon(max_page_size, system_page_size) == max_page_size);
    //
    // The size requested must be multiples of
    // the page size to be used as well as of the system page size,
    // and while it doesn't have to be a multiple of the SM page
    // size, it must at least accommodate the size requested, which
    // is a multiple of the SM page size.
    // ***********************************************************
    _disk_buffer_size  = alignon(size, max_page_size);
    w_assert1(_disk_buffer_size >= size); // goes without saying

    // should now be aligned on both page sizes
    w_assert1(size_t(alignon(_disk_buffer_size, max_page_size))
        == _disk_buffer_size);
    w_assert1(size_t(alignon(_disk_buffer_size, system_page_size))
        == _disk_buffer_size);
}

#if defined(HAVE_HUGETLBFS) && defined(HUGEPAGESIZE) && (HUGEPAGESIZE > 0)
void clear(char *buf_start, size_t requested_size)
{
    // Try reading first: do it in pages
    size_t requested_huge_pages = requested_size / (HUGEPAGESIZE*1024);
    size_t requested_pages = requested_size / SM_PAGESIZE;
    for(size_t j=0; j < requested_pages; j++) {
        for(size_t i=0; i < SM_PAGESIZE; i++) {
            size_t offset = j*SM_PAGESIZE + i;
            size_t hugepagenum =  offset / (HUGEPAGESIZE*1024);
            size_t hugepageoffset =  offset - (hugepagenum *
                    (HUGEPAGESIZE*1024));
            char x = buf_start[offset];

            // shut the compiler up:
            if(int(i) < 0) fprintf(stderr, "0x%d 0x%x, 0x%x, 0x%x", x,
                    int(hugepagenum), int(hugepageoffset), int(requested_huge_pages));
        }
    }

#if W_DEBUG_LEVEL > 4
    fprintf(stderr, "clearing %ld bytes starting at %p\n",
            requested_size, buf_start);
#endif
    memset(buf_start, 0, requested_size);
}
#else
void clear(char *buf_start, size_t requested_size )
{
    memset(buf_start, 0, requested_size);
}
#endif


w_rc_t sthread_t::set_bufsize_normal(
    size_t size, char *&buf_start /* in/out*/, long system_page_size)
{
    size_t requested_size = size; // save for asserts later

    // ***********************************************************
    //
    //  GET PAGE SIZES
    //
    // ***********************************************************
    long max_page_size = get_max_page_size(system_page_size);
    w_assert1(system_page_size <= max_page_size);

    // ***********************************************************
    //
    //  GET FILE DESCRIPTOR FOR MMAP
    //
    // ***********************************************************
    int fd(-1); // must be -1 if not mapping to a file

    // ***********************************************************
    //
    //  GET FLAGS FOR MMAP
    //
    // If posix mmapped file are available, _POSIX_MAPPED_FILES is defined
    // in <unistd.h> to be > 0
    //
    // That should give you these flags:
    // MAP_FIXED, MAP_PRIVATE, MAP_NORESERVE, MAP_ANONYMOUS
    // If MAP_ANONYMOUS is not there, MAP_ANON might be.
    //
    // However... systems aren't exactly in sync here, so configure.ac
    // checks for each of these flags.
    //
    // ***********************************************************
    int flags1 = MAP_PRIVATE;
    int flags2 = MAP_PRIVATE;

#ifdef HAVE_DECL_MAP_ANONYMOUS
    flags1  |= MAP_ANONYMOUS;
    flags2  |= MAP_ANONYMOUS;
#elif defined (HAVE_DECL_MAP_ANON)
    flags1  |= MAP_ANON;
    flags2  |= MAP_ANON;
#else
#endif

#ifdef HAVE_DECL_MAP_NORESERVE
    flags1  |= MAP_NORESERVE;
#endif
#ifdef HAVE_DECL_MAP_FIXED
    flags2  |= MAP_FIXED;
#endif

#ifdef HAVE_DECL_MAP_ALIGN
    flags1 |= MAP_ALIGN;
#endif
    // add one SM_PAGESIZE to the size requested before alignment,
    // and then do our own alignment at the end
    // In the case of MAP_ALIGN this shouldn't be necessary, but
    // we have so many different cases, it's going to be unreadable
    // if we try to avoid this in the one case, so do it in every case.
    size += SM_PAGESIZE;
    align_bufsize(size, system_page_size, max_page_size);

    // ***********************************************************
    //
    // FIRST MMAP: get a mapped region from the kernel.
    // If we are using hugetlbfs, fd will be >= 0 and
    // we won't have to do the remap -- the first mapping will
    // give us the best page sizes we can get.  In that case,
    // skip the first mmap and do exactly one "second mmap"
    //
    // ***********************************************************

    errno = 0;
    _disk_buffer = (char*) mmap(0, _disk_buffer_size,
               PROT_NONE,
               flags1,
               fd,   /* fd */
               0     /* off_t */
               );

    if (_disk_buffer == MAP_FAILED) {
        cerr
            << __LINE__ << " "
            << "mmap (size=" << _disk_buffer_size
            << " = " << int(_disk_buffer_size/1024)
            << " KB ) returns " << long(_disk_buffer)
            << " errno is " <<  errno  << " " << strerror(errno)
            << " flags " <<  flags1
            << " fd " <<  fd
            << endl;
        return RC(fcMMAPFAILED);
    }
#if W_DEBUG_LEVEL > 4
    else
    {
        cerr
            << __LINE__ << " "
            << "mmap SUCCESS! (size=" << _disk_buffer_size
            << " = " << int(_disk_buffer_size/1024)
            << " KB ) returns " << long(_disk_buffer)
            << " errno is " <<  errno  << " " << strerror(errno)
            << " flags " <<  flags1
            << " fd " <<  fd
            << endl;
    }
#endif


    // ***********************************************************
    //
    // RE-MMAP: break up the mapped region into max_page_size
    // chunks and remap them.
    //
    // ***********************************************************
    int nchunks = _disk_buffer_size / max_page_size;
    w_assert1(size_t(nchunks * max_page_size) == _disk_buffer_size);


    for(int i=0; i < nchunks; i++)
    {
        char *addr = _disk_buffer + (i * max_page_size);
        char *sub_buffer = (char*) mmap(addr,
               max_page_size,
                       PROT_READ | PROT_WRITE, /* prot */
                       flags2,
                       fd,   /* fd */
                       0     /* off_t */
                       );

        if (sub_buffer == MAP_FAILED) {
            cerr
                << __LINE__ << " "
                << "mmap (addr=" << long(addr )
                << ", size=" << max_page_size << ") returns -1;"
                << " errno is " <<  errno  << " " << strerror(errno)
                << " flags " <<  flags2
                << " fd " <<  fd
                << endl;
            do_unmap();
            return RC(fcMMAPFAILED);
        }
        w_assert1(sub_buffer == addr);
#ifdef HAVE_MEMCNTL
        struct memcntl_mha info;
        info.mha_cmd = MHA_MAPSIZE_VA;
        info.mha_flags = 0;
        info.mha_pagesize = max_page_size;
        // Ask the kernel to use the max page size here
        if(memcntl(sub_buffer, max_page_size, MC_HAT_ADVISE, (char *)&info, 0, 0) < 0)

        {
            cerr << "memcntl (chunk " << i << ") returns -1;"
                << " errno is " <<  errno  << " " << strerror(errno)
                << " requested size " <<  max_page_size  << endl;
            do_unmap();
            return RC(fcMMAPFAILED);
        }
#endif
    }

    align_for_sm(requested_size);
    buf_start = _disk_buffer;
    clear(buf_start, requested_size);
    return RCOK;
}

#ifdef WITHOUT_MMAP
w_rc_t
sthread_t::set_bufsize_memalign(size_t size, char *&buf_start /* in/out*/,
    long system_page_size)
{
    size_t requested_size = size; // save for asserts later

    // ***********************************************************
    //
    //  GET PAGE SIZES
    //
    // ***********************************************************

    long max_page_size = system_page_size;

    align_bufsize(size, system_page_size, max_page_size);

    w_assert1(_disk_buffer == NULL);

#ifdef HAVE_POSIX_MEMALIGN
    void *addr;
    int e = posix_memalign(&addr, SM_PAGESIZE, size);
    if (e == 0) {
        _disk_buffer = (char *)addr;
    } else {
        _disk_buffer = 0;
    }
#elif  HAVE_MEMALIGN
    _disk_buffer =  (char *)memalign(SM_PAGESIZE, size);
#elif  HAVE_VALLOC
    size += SM_PAGESIZE; // for alignment, add  a page and align it after.
    _disk_buffer =  valloc(size);
#else
    size += SM_PAGESIZE; // for alignment, add  a page and align it after.
    _disk_buffer =  malloc(size);
#endif
    if (_disk_buffer == 0) {
        cerr
            << __LINE__ << " "
            << "could not allocate memory (alignment=" << SM_PAGESIZE
        << "," << size << ") returns -error;"
            << " errno is " << strerror(errno)
            << endl;
        return RC(fcINTERNAL);
    }
    align_for_sm(requested_size);
    buf_start = _disk_buffer;
    clear(buf_start, requested_size);
    return RCOK;
}
#endif

#if defined(HAVE_HUGETLBFS)

#if HUGEPAGESIZE>0
#else
#   error You have configured to use hugetlbfs but you have no hugepagesize
#   error Look for Hugepagesize in /proc/meminfo
#endif

static const char *hugefs_path(NULL);
w_rc_t
sthread_t::set_hugetlbfs_path(const char *what)
{
    if(strcmp(what, "NULL")==0) {
        // Do not use tlbfs
        hugefs_path = NULL;
        return RCOK;
    }

    // stat the path to make sure it at least exists.
    // TODO: check the permissions and all that
    struct stat statbuf;
    int e=stat(what, &statbuf);
    if(e) {
        fprintf(stderr, "Could not stat \"%s\"\n", what);
        int fd = ::open(what, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            fprintf(stderr, "Could not create \"%s\"\n", what);
            return RC(stBADPATH);
        } else {
            cerr << " created " << what << endl;
        }
    }
    hugefs_path = what;
    // fprintf(stderr, "path is %s\n", hugefs_path);
    return RCOK;
}

w_rc_t
sthread_t::set_bufsize_huge(
    size_t size,
    char *&buf_start /* in/out*/,
    long system_page_size)
{
    size_t requested_size = size; // save for asserts later

    // ***********************************************************
    //
    //  GET PAGE SIZES
    //
    // ***********************************************************

    long max_page_size = 1024 * HUGEPAGESIZE;
    // I don't know how to get this programatically

    w_assert1(system_page_size <= max_page_size);

    // ***********************************************************
    //
    //  GET FILE DESCRIPTOR FOR MMAP
    //
    // ***********************************************************
    // TODO: verify that this file can be multiply mapped
    // by diff users (i.e., don't need unique file name for each sm)


    if(hugefs_path == NULL)
    {
        fprintf(stderr, "path is %s\n", hugefs_path);
        fprintf(stderr,
            "Need path to huge fs. Use ::set_hugetlbfs_path(path)\n");
        return RC(fcMMAPFAILED);
    }
    int fd = ::open(hugefs_path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        cerr << " could not open " << hugefs_path << endl;
        return RC(fcMMAPFAILED);
    }

    // ***********************************************************
    //
    //  GET FLAGS FOR MMAP
    //
    // If posix mmapped file are available, _POSIX_MAPPED_FILES is defined
    // in <unistd.h> to be > 0
    //
    // That should give you these flags:
    // MAP_FIXED, MAP_PRIVATE, MAP_NORESERVE, MAP_ANONYMOUS
    // If MAP_ANONYMOUS is not there, MAP_ANON might be.
    //
    // However... systems aren't exactly in sync here, so configure.ac
    // checks for each of these flags.
    //
    // ***********************************************************
    int flags =
        MAP_PRIVATE;

    /* NOTE: cannot use ANONYMOUS for hugetlbfs*/

#ifdef HAVE_DECL_MAP_ALIGN
    flags |=  MAP_ALIGN;
    fprintf(stderr, "%d: adding flag 0x%x %s\n", __LINE__,
            MAP_ALIGN, "MAP_ALIGN");
#endif
    // add one SM_PAGESIZE to the size requested before alignment,
    // and then do our own alignment at the end
    // In the case of MAP_ALIGN this shouldn't be necessary, but
    // we have so many different cases, it's going to be unreadable
    // if we try to avoid this in the one case, so do it in every case.
    size += SM_PAGESIZE;

    align_bufsize(size, system_page_size, max_page_size);

    // ***********************************************************
    //
    // MMAP: get a mapped region from the kernel.
    //
    // ***********************************************************

    w_assert1(_disk_buffer == NULL);

    errno = 0;
    // mmap ( 0, length, protection, flags, fd, 0)
    _disk_buffer = (char*) mmap(0, _disk_buffer_size,
               (PROT_READ | PROT_WRITE), /* prot */
               flags,
               fd,   /* fd */
               0     /* off_t */
               );

    if (_disk_buffer == MAP_FAILED) {
        cerr
            << __LINE__ << " "
            << "mmap (size=" << _disk_buffer_size << ") returns "
            <<  long(_disk_buffer)
            << " errno is " <<  errno  << " " << strerror(errno)
            << " prot " <<  (PROT_READ | PROT_WRITE)
            << " flags " <<  flags
            << " fd " <<  fd
            << endl;
        close(fd);
        return RC(fcMMAPFAILED);
    }
#if W_DEBUG_LEVEL > 4
    else
    {
        fprintf(stderr,
    "%d mmap SUCCESS! (size= %lu, %lu KB) returns %p errno %d/%s prot 0x%x flags 0x%x fd %d\n",
        __LINE__,
        _disk_buffer_size,
        _disk_buffer_size/1024,
        _disk_buffer, errno, strerror(errno),
        (PROT_READ | PROT_WRITE),
        flags, fd);
        fprintf(stderr,
    "%d mmap (size= %lu, %lu KB) (requested_size %d, %d KB) buf-requested is %d\n",
        __LINE__,
        _disk_buffer_size,
        _disk_buffer_size/1024,
        int(requested_size),
        int(requested_size/1024),
        int(_disk_buffer_size-requested_size) );


    }
#endif

    align_for_sm(requested_size);
    buf_start = _disk_buffer;
    clear(buf_start, requested_size);
    return RCOK;
}
#endif

/********************************************************************

NOTES: HUGETLBFS: To minimize tlb misses:

Make sure the region uses the largest page size available so that it
will require the fewest tlb entries possible.

If we have hugetlbfs, use the given path and get an fd for it. (This requires
a shore config argument -- HUGETLBFS_PATH, set in shore.def).

If not, do the following:
1) mmap a region with PROT_NONE & MAP_PRIVATE, MAP_NORESERVE
    (if present)
2) remap in chunks of max page size.
2-a) If we have memcntl, use it to request the largest page size to be used
2-b) re-map sections using largest page size available
    with the protection PROT_READ | PROT_WRITE,
    and flags MAP_FIXED

To find out the max page size available:

If we have a hugetlbfs (as in, on linux 2.6), we are stuck with what
it gives us.

If not, and we have getpagesizes() use it to
get the largest page size we have on this machine,
else
use sysconf(_SC_PHYS_PAGES).

For whatever page size we come up with, make sure the size we request is
a multiple of the system pages size and of the page size we are trying
to use and the SM page size.

The resulting buffer address must be aligned wrt the SM page size as
well as the page size we are trying to use.  To ensure that:
if the system doesn't give us a MAP_ALIGN option, we'll
add one SM_PAGESIZE to the requested size and and then do the
alignment ourselves at the end.

To make sure the region we get is continguous: Assume mmap does it right
on any given call; use MAP_FIXED when remapping.

Finally, if the use configured with --without-mmap, then bypass all this
and just alloc the memory using posix_memalign, memalign,or valloc.

********************************************************************/

w_rc_t
sthread_t::set_bufsize(size_t size, char *&buf_start /* in/out*/,
    bool
#if defined(HAVE_HUGETLBFS)
    // This argument is used only by the unit tests.
    use_normal_if_huge_fails /*=false*/
#endif
    )
{
    if (_disk_buffer && size == 0) {
        do_unmap();
        return RCOK;
    }

    if (_disk_buffer) {
        cerr << "Can't re-allocate disk buffer without disabling"
            << endl;
        return RC(fcINTERNAL);
    }

    buf_start = 0;

    long system_page_size = sysconf(_SC_PAGESIZE);

#ifdef WITHOUT_MMAP
    // If the user configured --without-mmap, then don't even
    // bother with the mmap attempts below.
    return set_bufsize_memalign(size, buf_start, system_page_size);
#endif

#if defined(HAVE_HUGETLBFS)
    // Ok, we have to have configured for hugefs AND we have to
    // have set a path for it.  If we have no path string,
    // we have chosen not to use hugetlbfs.  This is the result
    // of setting run-time options sm_hugetlbfs_path to "NULL".
    // So if we've set the path to "NULL", we will just use the
    // "normal way".
    if(hugefs_path != NULL) {
        w_rc_t rc =  set_bufsize_huge(size, buf_start, system_page_size);
        if( !rc.is_error() ) {
#if W_DEBUG_LEVEL > 10
            cout << "Using hugetlbfs size " << size
                << " system_page_size " << system_page_size
                << " path " << hugefs_path << ". " << endl;
#endif
            return rc;
        }
        if(!use_normal_if_huge_fails)
        {
            return rc;
        }
        // else, try the other way
        cerr << "Skipping hugetlbfs sue to mmap failure: " << rc << endl;
    } else {
        cout << "Skipping hugetlbfs based on user option. " << endl;
    }
#endif
    return set_bufsize_normal(size, buf_start, system_page_size);
}


char  *
sthread_t::set_bufsize(size_t size)
{
    w_rc_t    e;
    char    *start;

    if(size==0) { do_unmap(); return NULL; }

    e = set_bufsize(size, start);

    if (e.is_error()) {
        cerr << "Hidden Failure: set_bufsize(" << size << "):"
            << endl << e << endl;
        return 0;
    }

    /* compatability on free */
    if (size == 0)
        start = 0;

    return start;
}

sdisk_t* sthread_t::get_disk(int& fd)
{
    // CS: this method should fix problem with concurrent access to _disks
    // (a new design would be better, but I'm trying to work around Shore's problems)
    fd -= fd_base;

    CRITICAL_SECTION(cs, protectFDs);

    sdisk_t* disk = _disks[fd];
    if (fd < 0 || fd >= (int)open_max || !disk) {
        W_COERCE(RC(stBADFD));
    }

    return disk;
}

w_rc_t
sthread_t::open(const char* path, int flags, int mode, int &ret_fd)
{
    /* default return value */
    ret_fd = -1;

    CRITICAL_SECTION(cs, protectFDs);

    if (open_count >= open_max) {
        // CS: I have no idea what the comment below means
        // This was originally done because when we used a separate
        // process for blocking i/o, we could
        // have many more open files than sthread_t::open_max.
        // But with threading, we are stuck with the the os limit O(1024).
        // For now, we use the original code because open_max starts out 0.
        // TODO : We need to use os limit here, acquire the array once.
        // I suppose it's worth doing this dynamically for several reasons.
        // Not all threads do I/O, for one thing.

        open_max += 64;
        _disks.resize(open_max, nullptr);
    }

    unsigned  i;
    for (i = 0; i < open_max; i++) {
        if (!_disks[i]) { break; }
    }
    if (i == open_max) { return RC(stINTERNAL); }

    sdisk_t* disk = new sdisk_unix_t(path);
    w_assert0(disk);
    W_DO(disk->open(path, flags, mode));

    _disks[i] = disk;
    open_count++;
    ret_fd = fd_base + i;

    return RCOK;
}



/*
 *  sthread_t::close(fd)
 *
 *  Close a file previously opened with sthread_t::open().
 */

w_rc_t sthread_t::close(int fd)
{
    sdisk_t* disk = get_disk(fd);

    // sync before close
    W_DO(disk->sync());
    W_DO(disk->close());

    {
        CRITICAL_SECTION(cs, protectFDs);
        _disks[fd] = nullptr;
        open_count--;
    }

    delete disk;
    return RCOK;
}

/*
 *  sthread_t::write(fd, buf, n)
 *  sthread_t::writev(fd, iov, iovcnt)
 *  sthread_t::read(fd, buf, n)
 *  sthread_t::readv(fd, iov, iovcnt)
 *  sthread_t::fsync(fd)
 *  sthread_t::ftruncate(fd, len)
 *
 *  Perform I/O.
 *
 *  XXX Currently I/O operations that don't have a complete character
 *  count return with a "SHORTIO" error.  In the future,
 *  there should be two forms of read and
 *  write operations.  The current style which returns errors
 *  on "Short I/O", and a new version which can return a character
 *  count, or "Short I/O" if a character count can't be
 *  determined.
 *
 *  XXX various un-const casts are included below.  Some of them
 *  will be undone when cleanup hits.  Others should be
 *  propogated outward to the method declarations in sthread.h
 *  to match what the underlying OSs may guarantee.
 */

w_rc_t    sthread_t::read(int fd, void* buf, int n)
{
    sdisk_t* disk = get_disk(fd);

    int    done = 0;
    w_rc_t    e;

    e = disk->read(buf, n, done);
    if (!e.is_error() && done != n)
        e = RC(stSHORTIO);

    return e;
}


w_rc_t    sthread_t::write(int fd, const void* buf, int n)
{
    sdisk_t* disk = get_disk(fd);

    int    done = 0;
    w_rc_t    e;

    e = disk->write(buf, n, done);
    if (!e.is_error() && done != n)
        e = RC(stSHORTIO);

    return e;
}


w_rc_t    sthread_t::readv(int fd, const iovec_t *iov, size_t iovcnt)
{
    sdisk_t* disk = get_disk(fd);

    int    done = 0;
    int    total = 0;
    w_rc_t    e;

    total = sdisk_t::vsize(iov, iovcnt);

    e = disk->readv(iov, iovcnt, done);
    if (!e.is_error() && done != total)
        e = RC(stSHORTIO);

    return e;
}


w_rc_t    sthread_t::writev(int fd, const iovec_t *iov, size_t iovcnt)
{
    sdisk_t* disk = get_disk(fd);

    int    done = 0;
    int    total = 0;
    w_rc_t    e;

    total = sdisk_t::vsize(iov, iovcnt);

    e = disk->writev(iov, iovcnt, done);
    if (!e.is_error() && done != total)
        e = RC(stSHORTIO);

    return e;
}


w_rc_t    sthread_t::pread(int fd, void *buf, int n, fileoff_t pos)
{
    sdisk_t* disk = get_disk(fd);

    int    done = 0;
    w_rc_t    e;

    errno = 0;
    e = disk->pread(buf, n, pos, done);
    if (!e.is_error() && done != n) {
        e = RC(stSHORTIO);
    }

    return e;
}

/*
 * A version of pread that tolerates short IO, i.e., less bytes being read
 * than requested in n. Number of bytes read is returned in 'done'.
 */
w_rc_t sthread_t::pread_short(int fd, void *buf, int n, fileoff_t pos,
       int& done)
{
    sdisk_t* disk = get_disk(fd);

    return disk->pread(buf, n, pos, done);
}


w_rc_t    sthread_t::pwrite(int fd, const void *buf, int n, fileoff_t pos)
{
    sdisk_t* disk = get_disk(fd);

    int    done = 0;
    w_rc_t    e;

    e = disk->pwrite(buf, n, pos, done);
    if (!e.is_error() && done != n)
        e = RC(stSHORTIO);

    return e;
}


w_rc_t    sthread_t::fsync(int fd)
{
    sdisk_t* disk = get_disk(fd);
    return disk->sync();
}

w_rc_t    sthread_t::ftruncate(int fd, fileoff_t n)
{
    sdisk_t* disk = get_disk(fd);
    return  disk->truncate(n);
}

w_rc_t sthread_t::frename(int fd, const char* oldname, const char* newname)
{
    sdisk_t* disk = get_disk(fd);
    return disk->rename(oldname, newname);
}

w_rc_t sthread_t::lseek(int fd, fileoff_t pos, int whence, fileoff_t& ret)
{
    sdisk_t* disk = get_disk(fd);
    return disk->seek(pos, whence, ret);
}


w_rc_t sthread_t::lseek(int fd, fileoff_t offset, int whence)
{
    fileoff_t    dest;
    w_rc_t        e;

    e = sthread_t::lseek(fd, offset, whence, dest);
    if (!e.is_error() && whence == SEEK_AT_SET && dest != offset)
        e = RC(stSHORTSEEK);

    return e;
}


w_rc_t    sthread_t::fstat(int fd, filestat_t &st)
{
    sdisk_t* disk = get_disk(fd);
    return disk->stat(st);
}

w_rc_t    sthread_t::fisraw(int fd, bool &isRaw)
{
    filestat_t    st;

    isRaw = false;        /* default value */

    W_DO(fstat(fd, st));    /* takes care of errors */

    isRaw = st.is_device ;
    return RCOK;
}


void    sthread_t::dump_io(ostream &s)
{
    s << "I/O:";
    s << " open_max=" << int(open_max);
    s << " open_count=" << open_count;
    s << endl;
}

extern "C" void dump_io()
{
    sthread_t::dump_io(cout);
    cout << flush;
}
/**\endcond skip */
