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

/*<std-header orig-src='shore' incl-file-exclusion='SDISK_H'>

 $Id: sdisk.h,v 1.23 2010/05/26 01:21:28 nhall Exp $

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

#ifndef SDISK_H
#define SDISK_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/


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

/**\cond skip */
class sdisk_base_t {
public:

    struct iovec_t {
        void    *iov_base;
        size_t    iov_len;

        iovec_t(void *base = 0, int len = 0)
            : iov_base(base), iov_len(len) { }
    };

    /* Don't use off_t, cause we may want the system off_t */
#if !defined(LARGEFILE_AWARE) && !defined(ARCH_LP64)
    /* XXX if !LARGEFILE, should choose on per operating system
       to pick the native size. */
    typedef int32_t fileoff_t;
#else
    typedef int64_t fileoff_t;
#endif

    struct    filestat_t {
        fileoff_t    st_size;
        fileoff_t    st_file_id;
        unsigned    st_device_id;
        unsigned    st_block_size;
        bool        is_file;
        bool        is_dir;
        bool        is_device;

        filestat_t() : st_size(0),
            st_file_id(0), st_device_id(0), st_block_size(0),
            is_file(false), is_dir(false),
            is_device(false)
        { }
    };

    /* posix-compatabile file modes, namespace contortion */
    enum {
        /* open modes ... 1 of n */
        OPEN_RDONLY=0,
        OPEN_WRONLY=0x1,
        OPEN_RDWR=0x2,
        MODE_FLAGS=0x3,        // internal

        /* open options ... m of n */
        OPEN_TRUNC=0x10,
        OPEN_EXCL=0x20,
        OPEN_CREATE=0x40,
        OPEN_SYNC=0x80,
        OPEN_APPEND=0x100,
        OPEN_DIRECT=0x200,
        OPTION_FLAGS=0x3f0    // internal
    };

    /* seek modes; contortions to avoid namespace problems */
    enum {
        SEEK_AT_SET=0,        // absolute
        SEEK_AT_CUR=1,        // from current position
        SEEK_AT_END=2        // from end-of-file
    };

    /* utility functions */
    static    int    vsize(const iovec_t *iov, int iovcnt);
};


/* sdisk is an interface class which isn't useful by itself */

class sdisk_t : public sdisk_base_t {
protected:
    sdisk_t() { }

    /* methods for lookint at open flags to extract I/O mode and options */
    static    int    modeBits(int mode);
    static    bool    hasMode(int mode, int wanted);
    static    bool    hasOption(int mode, int wanted);

public:
    virtual    ~sdisk_t() { }

    virtual w_rc_t    open(const char *name, int flags, int mode) = 0;
    virtual    w_rc_t    close() = 0;

    virtual    w_rc_t    read(void *buf, int count, int &done) = 0;
    virtual    w_rc_t    write(const void *buf, int count, int &done) = 0;

    virtual    w_rc_t    readv(const iovec_t *iov, int ioc, int &done);
    virtual w_rc_t    writev(const iovec_t *iov, int ioc, int &done);

    virtual    w_rc_t    pread(void *buf, int count, fileoff_t pos, int &done);
    virtual    w_rc_t    pwrite(const void *buf, int count,
                   fileoff_t pos, int &done);

    virtual w_rc_t    seek(fileoff_t pos, int origin, fileoff_t &newpos) = 0;

    virtual w_rc_t    rename(const char* oldname, const char* newname) = 0;

    virtual w_rc_t    truncate(fileoff_t size) = 0;
    virtual w_rc_t    sync();

    virtual    w_rc_t    stat(filestat_t &stat);
};

/**\endcond skip */

/*<std-footer incl-file-exclusion='SDISK_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
