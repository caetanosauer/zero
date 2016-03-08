/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */


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

/*<std-header orig-src='shore' incl-file-exclusion='SRV_LOG_H'>

 $Id: partition.h,v 1.6 2010/08/23 14:28:18 nhall Exp $

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

#ifndef PARTITION_H
#define PARTITION_H
#include "w_defines.h"

#include "logrec.h"
#include <mutex>

class log_storage; // forward

class partition_t {
public:
    typedef smlevel_0::fileoff_t          fileoff_t;
    typedef smlevel_0::partition_number_t partition_number_t;

    enum { XFERSIZE = 8192 };
    enum { invalid_fhdl = -1 };

    partition_t(log_storage*, partition_number_t);
    virtual ~partition_t() { }

    partition_number_t num() const   { return _num; }

    rc_t open_for_append();
    rc_t open_for_read();
    rc_t close_for_append();
    rc_t close_for_read();

    rc_t read(logrec_t *&r, lsn_t &ll, lsn_t* prev_lsn = NULL);
    void release_read();

    rc_t flush(lsn_t lsn, const char* const buf, long start1, long end1,
            long start2, long end2);

    bool is_open_for_read() const
    {
        return (_fhdl_rd != invalid_fhdl);
    }

    bool is_open_for_append() const
    {
        return (_fhdl_app != invalid_fhdl);
    }

    size_t get_size(bool must_be_skip = true);

    void set_size(size_t size) { _size = size; }

    rc_t prime_buffer(char* buffer, lsn_t lsn, size_t& prime_offset);

    void destroy();

private:
    partition_number_t    _num;
    log_storage*          _owner;
    long                  _size;
    int                   _fhdl_rd;
    int                   _fhdl_app;
    static int            _artificial_flush_delay;  // in microseconds
    char*                 _readbuf;

    void             fsync_delayed(int fd);
    rc_t scan_for_size(bool must_be_skip);

    // Serialize read calls, which use the same buffer
    mutex _read_mutex;
};

#endif
