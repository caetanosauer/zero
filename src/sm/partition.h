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


/*  -- do not edit anything above this line --   </std-header>*/

typedef enum    {  /* partition_t::_mask values */
    m_exists=0x2,
    m_open_for_read=0x4,
    m_open_for_append=0x8,
    m_flushed=0x10    // has no data cached
} partition_mask_values;

class log_storage; // forward
class partition_t {
public:
    typedef smlevel_0::fileoff_t          fileoff_t;
    typedef smlevel_0::partition_number_t partition_number_t;
#if SM_PAGESIZE < 8192
    enum { XFERSIZE = 8192 };
#else
    enum { XFERSIZE = 8192 };
    // TODO: decide enum { XFERSIZE = SM_PAGESIZE };
#endif
    enum { invalid_fhdl = -1 };
    enum { nosize = -1 };

    NORET             partition_t() :
                            _num(0),
                            _mask(0),
                            _size(0),
                            _eop(0),
                            _owner(0),
                            _fhdl_rd(invalid_fhdl),
                            _fhdl_app(invalid_fhdl)
    {
        _peekbuf = new char[sizeof(logrec_t)];
    }

    ~partition_t()
    {
        delete _peekbuf;
    }


    /////////////////// DATA
    static int            _artificial_flush_delay;  // in microseconds
private:
    partition_number_t    _num;
    uint32_t     _mask;
    // logical end of partition is _size;
    fileoff_t             _size;
    // physical end of partition
    fileoff_t             _eop;
    log_storage*             _owner;
    lsn_t                 _last_skip_lsn;
    // Read and append file handles
    int                   _fhdl_rd;
    int                   _fhdl_app;
    // buffer to read log records in the _peek function
    // used to be readbuf() of log_core
    char*               _peekbuf;

private:
    /* store end lsn at the beginning of each partition; updated
    * when partition closed
    */
    void             flush(int fd);
    lsn_t            first_lsn(uint32_t pnum) const { return lsn_t( pnum, 0); }

public:
    // exported for unix_log
    void               init(log_storage *owner);
    void               clear();

    fileoff_t          size() const   { return _size; }
    void               set_size(fileoff_t v) { _size =  v; }
    partition_number_t num() const   { return _num; }
    lsn_t              first_lsn() const { return
                                    first_lsn(uint32_t(_num)); }
    void               open_for_append(partition_number_t n, const lsn_t& hint);
    w_rc_t             open_for_read(partition_number_t n, bool err=true);
    void               close_for_append();
    void               close_for_read();
    bool               is_current() const;
    void               peek(partition_number_t n,
                            const lsn_t&    end_hint,
                            bool,
                            int* fd=0);
    w_rc_t             read(char* readbuf,
                            logrec_t *&r, lsn_t &ll,
                            lsn_t* prev_lsn = NULL,
                            int fd = invalid_fhdl);
    void               flush(
#ifdef LOG_DIRECT_IO
                            char* writebuf,
#endif
                            int fd,
                            lsn_t lsn,
                            const char* const buf,
                            long start1,
                            long end1,
                            long start2,
                            long end2);

    /*
     * Methods used by logbuf_core
     */
    // new flush, which can flush multiple segments
    void  flush(
#ifdef LOG_DIRECT_IO
                char* writebuf,
#endif
                int fd,
                lsn_t lsn,
                int64_t size,
                int64_t write_size,
                sdisk_base_t::iovec_t *iov,
                uint32_t seg_cnt);
    // read an entire segment
    w_rc_t read_seg(lsn_t ll, char *buf, uint32_t size, int fd = invalid_fhdl);
    // read one log record
    w_rc_t read_logrec(char* readbuf, logrec_t *&rp, lsn_t &ll, int fd = invalid_fhdl);


    const lsn_t&       last_skip_lsn() const { return _last_skip_lsn; }
#if W_DEBUG_LEVEL > 2
    void               check_fhdl_rd() const ;
    void               check_fhdl_app() const ;
#else
    void               check_fhdl_rd() const { }
    void               check_fhdl_app() const { }
#endif
    int                fhdl_rd() const { check_fhdl_rd(); return _fhdl_rd; }
    int                fhdl_app() const { check_fhdl_app(); return _fhdl_app; }
    bool               exists() const {
                           bool res = (_mask & m_exists) != 0;
#if W_DEBUG_LEVEL > 2
                           if(res) { w_assert3(num() != 0); }
#endif
                           return res;
                       }
    bool               is_open_for_read() const {
                           return ((_mask & m_open_for_read) != 0);
                       }

    bool               is_open_for_append() const {
                           return  ((_mask & m_open_for_append) != 0);
                       }
    void               close(bool both);
    void               close() { this->close(false);  }
    void               destroy();
    void               sanity_check() const;

private:
    char *             _readbuf();
#ifdef LOG_DIRECT_IO
    char *             _writebuf();
#endif

    void               _set_state(uint32_t m) { _mask |= m ; }
    void               _clr_state(uint32_t m) { _mask &= ~m ; }
    void               _skip(const lsn_t &ll, int fd);
    // helper for peek()
    void              _peek(partition_number_t n,
                            fileoff_t startloc,
                            fileoff_t wholesize,
                            bool, int fd);
    void              _set_last_skip_lsn(const lsn_t &l) { _last_skip_lsn = l;}

    // helper for open_for_append
    void              _set_fhdl_app(int fd) {
                           w_assert3(fhdl_app() == invalid_fhdl);
                           _fhdl_app = fd;
                      }
public:
}; // partition_t
#endif
