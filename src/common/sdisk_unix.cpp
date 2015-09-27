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

 $Id: sdisk_unix.cpp,v 1.27 2010/12/08 17:37:50 nhall Exp $

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

#if defined(linux) && !defined(_GNU_SOURCE)
/*
 *  XXX this done to make O_DIRECT available as an I/O choice.
 *  Unfortunately, it needs to pollute the other headers, otw
 *  the features will be set and access won't be possible
 */
#define _GNU_SOURCE
#endif

#include "w_defines.h"

#include <errno.h>

/*  -- do not edit anything above this line --   </std-header>*/

/**\cond skip */
/*
 *   NewThreads I/O is Copyright 1995, 1996, 1997, 1998 by:
 *
 *    Josef Burger    <bolo@cs.wisc.edu>
 *
 *   All Rights Reserved.
 *
 *   NewThreads I/O may be freely used as long as credit is given
 *   to the above author(s) and the above copyright is maintained.
 */

#include <w.h>
#include <sthread.h>
#include <sdisk.h>
#include <sdisk_unix.h>
#include <sthread_stats.h>
extern class sthread_stats SthreadStats;

#include "os_fcntl.h"
#include <cerrno>
#include <sys/stat.h>

#include <sys/uio.h>

#define    HAVE_IO_VECTOR

// TODO deal with these HAVE_IO*
// TODO : is vector i/o ok with pthreads?

#include <os_interface.h>

#define CHECK_ERRNO(n) \
    if (n == -1) { \
        W_RETURN_RC_MSG(fcOS, << "Kernel errno code: " << errno); \
    }

int    sdisk_unix_t::convert_flags(int sflags)
{
    int    flags = 0;

    /* 1 of n */
    switch (modeBits(sflags)) {
    case OPEN_RDWR:
        flags |= O_RDWR;
        break;
    case OPEN_WRONLY:
        flags |= O_WRONLY;
        break;
    case OPEN_RDONLY:
        flags |= O_RDONLY;
        break;
    }

    /* m of n */
    /* could make a data driven flag conversion, :-) */
    if (hasOption(sflags, OPEN_CREATE))
        flags |= O_CREAT;
    if (hasOption(sflags, OPEN_TRUNC))
        flags |= O_TRUNC;
    if (hasOption(sflags, OPEN_EXCL))
        flags |= O_EXCL;
    if (hasOption(sflags, OPEN_SYNC))
        flags |= O_SYNC;
    if (hasOption(sflags, OPEN_APPEND))
        flags |= O_APPEND;
    /*
     * From the open man page:
     *      O_DIRECT
              Try to minimize cache effects of the I/O to and from this  file.
              In  general  this  will degrade performance, but it is useful in
              special situations, such  as  when  applications  do  their  own
              caching.   File I/O is done directly to/from user space buffers.
              The I/O is synchronous, i.e., at the completion of a read(2)  or
              write(2),  data  is  guaranteed to have been transferred.  Under
              Linux 2.4 transfer sizes, and the alignment of user  buffer  and
              file  offset  must all be multiples of the logical block size of
              the file system. Under Linux 2.6 alignment must  fit  the  block
              size of the device.
    */
    if (hasOption(sflags, OPEN_DIRECT))
        flags |= O_DIRECT;


    return flags;
}


sdisk_unix_t::~sdisk_unix_t()
{
    if (_fd != FD_NONE)
        W_COERCE(close());
}


w_rc_t    sdisk_unix_t::make(const char *name, int flags, int mode,
               sdisk_t *&disk)
{
    sdisk_unix_t    *ud;
    w_rc_t        e;

    disk = 0;    /* default value*/

    ud = new sdisk_unix_t(name);
    if (!ud)
        return RC(fcOUTOFMEMORY);

    e = ud->open(name, flags, mode);
    if (e.is_error()) {
        delete ud;
        return e;
    }

    disk = ud;
    return RCOK;
}


w_rc_t    sdisk_unix_t::open(const char *name, int flags, int mode)
{
    if (_fd != FD_NONE)
        return RC(stBADFD);    /* XXX in use */

    _fd = ::os_open(name, convert_flags(flags), mode);
    CHECK_ERRNO(_fd);

    return RCOK;
}

w_rc_t    sdisk_unix_t::close()
{
    if (_fd == FD_NONE)
        return RC(stBADFD);    /* XXX closed */

    int    n;

    n = ::os_close(_fd);
    CHECK_ERRNO(n);

    _fd = FD_NONE;
    return RCOK;
}




w_rc_t    sdisk_unix_t::read(void *buf, int count, int &done)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    int    n;
    n = ::os_read(_fd, buf, count);
    CHECK_ERRNO(n);

    done = n;

    return RCOK;
}

w_rc_t    sdisk_unix_t::write(const void *buf, int count, int &done)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    int    n;

    n = ::os_write(_fd, buf, count);
    CHECK_ERRNO(n);

    done = n;

    return RCOK;
}

#ifdef HAVE_IO_VECTOR
w_rc_t    sdisk_unix_t::readv(const iovec_t *iov, int iovcnt, int &done)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    int    n;
    n = ::os_readv(_fd, (const struct iovec *)iov, iovcnt);
    CHECK_ERRNO(n);

    done = n;

    return RCOK;
}

w_rc_t    sdisk_unix_t::writev(const iovec_t *iov, int iovcnt, int &done)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    int    n;

    n = ::os_writev(_fd, (const struct iovec *)iov, iovcnt);
    CHECK_ERRNO(n);

    done = n;

    return RCOK;
}
#endif

w_rc_t    sdisk_unix_t::pread(void *buf, int count, fileoff_t pos, int &done)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    int    n;
    n = ::os_pread(_fd, buf, count, pos);
    CHECK_ERRNO(n);

    done = n;

    return RCOK;
}


w_rc_t    sdisk_unix_t::pwrite(const void *buf, int count, fileoff_t pos,
                int &done)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    int    n;

    n = ::os_pwrite(_fd, buf, count, pos);
    CHECK_ERRNO(n);

    done = n;

    return RCOK;
}

w_rc_t    sdisk_unix_t::seek(fileoff_t pos, int origin, fileoff_t &newpos)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    switch (origin) {
    case SEEK_AT_SET:
        origin = SEEK_SET;
        break;
    case SEEK_AT_CUR:
        origin = SEEK_CUR;
        break;
    case SEEK_AT_END:
        origin = SEEK_END;
        break;
    }

    fileoff_t    l=0;
    l = ::os_lseek(_fd, pos, origin);
    CHECK_ERRNO(l);

    newpos = l;

    return RCOK;
}

w_rc_t    sdisk_unix_t::rename(const char* oldname, const char* newname)
{
    int n = ::os_rename(oldname, newname);
    CHECK_ERRNO(n);

    return RCOK;
}

w_rc_t    sdisk_unix_t::truncate(fileoff_t size)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);
    int    n = ::os_ftruncate(_fd, size);
    CHECK_ERRNO(n);

    return RCOK;
}

w_rc_t    sdisk_unix_t::sync()
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    int n = os_fsync(_fd);

    /* fsync's to r/o files and devices can fail ok */
    if (n == -1 && (errno == EBADF || errno == EINVAL))
        n = 0;

    CHECK_ERRNO(n);

    return RCOK;
}


w_rc_t    sdisk_unix_t::stat(filestat_t &st)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    os_stat_t    sys;
    int n = os_fstat(_fd, &sys);
    CHECK_ERRNO(n);

    st.st_size = sys.st_size;
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
    st.st_block_size = sys.st_blksize;
#else
    st.st_block_size = 512;    /* XXX */
#endif

    st.st_device_id = sys.st_dev;
    st.st_file_id = sys.st_ino;

    int mode = (sys.st_mode & S_IFMT);
    st.is_file = (mode == S_IFREG);
    st.is_dir = (mode == S_IFDIR);
#ifdef S_IFBLK
    st.is_device = (mode == S_IFBLK);
#else
    st.is_device = false;
#endif
    st.is_device = st.is_device || (mode == S_IFCHR);

    return RCOK;
}

/**\endcond skip */
