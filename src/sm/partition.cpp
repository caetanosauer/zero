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

/*<std-header orig-src='shore'>

 $Id: partition.cpp,v 1.11 2010/12/08 17:37:43 nhall Exp $

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

#define SM_SOURCE
#define PARTITION_C

#include "sm_base.h"
#include "logtype_gen.h"
#include "log_storage.h"
#include <sys/stat.h>

// needed for skip_log
#include "logdef_gen.cpp"

partition_t::partition_t(log_storage *owner, partition_number_t num)
    : _num(num), _owner(owner), _size(-1),
      _fhdl_rd(invalid_fhdl), _fhdl_app(invalid_fhdl)
{
#if SM_PAGESIZE < 8192
    _readbuf = new char[log_storage::BLOCK_SIZE*4];
#else
    _readbuf = new char[SM_PAGESIZE*4];
#endif
}

/*
 * open_for_append(num, end_hint)
 * "open" a file  for the given num for append, and
 * make it the current file.
 */
// MUTEX: flush, insert, partition
rc_t partition_t::open_for_append()
{
    w_assert3(!is_open_for_append());

    int fd, flags = smthread_t::OPEN_RDWR | smthread_t::OPEN_CREATE;
    string fname = _owner->make_log_name(_num);
    W_DO(me()->open(fname.c_str(), flags, 0744, fd));
    _fhdl_app = fd;

    return RCOK;
}

long floor2(long offset, long block_size)
{ return offset & -block_size; }
long ceil2(long offset, long block_size)
{ return floor2(offset + block_size - 1, block_size); }

// Block of zeroes : used in next function.
// Initialize on first access:
// block to be cleared upon first use.
class block_of_zeroes {
private:
    char _block[log_storage::BLOCK_SIZE];
public:
    NORET block_of_zeroes() {
        memset(&_block[0], 0, log_storage::BLOCK_SIZE);
    }
    char *block() { return _block; }
};

char *block_of_zeros() {

    static block_of_zeroes z;
    return z.block();
}

/*
 * partition::flush(int fd, bool force)
 * flush to disk whatever's been buffered.
 * Do this with a writev of 4 parts:
 * start->end1 where start is start1 rounded down to the beginning of a BLOCK
 * start2->end2
 * a skip record
 * enough zeroes to make the entire write become a multiple of BLOCK_SIZE
 */
rc_t partition_t::flush(
        lsn_t lsn,  // needed so that we can set the lsn in the skip_log record
        const char* const buf,
        long start1,
        long end1,
        long start2,
        long end2)
{
    w_assert0(end1 >= start1);
    w_assert0(end2 >= start2);
    long size = (end2 - start2) + (end1 - start1);
    long write_size = size;

    { // sync log: Seek the file to the right place.
        DBG5( << "Sync-ing log lsn " << lsn
                << " start1 " << start1
                << " end1 " << end1
                << " start2 " << start2
                << " end2 " << end2 );

        // works because BLOCK_SIZE is always a power of 2
        long file_offset = floor2(lsn.lo(), log_storage::BLOCK_SIZE);
        // offset is rounded down to a block_size

        long delta = lsn.lo() - file_offset;

        // adjust down to the nearest full block
        w_assert1(start1 >= delta); // really offset - delta >= 0,
                                    // but works for unsigned...
        write_size += delta; // account for the extra (clean) bytes
        start1 -= delta;

        /* FRJ: This seek is safe (in theory) because only one thread
           can flush at a time and all other accesses to the file use
           pread/pwrite (which doesn't change the file pointer).
         */
        fileoff_t where = file_offset;
        W_DO(me()->lseek(_fhdl_app, where, sthread_t::SEEK_AT_SET));
    } // end sync log

    { // Copy a skip record to the end of the buffer.
        skip_log* s = _owner->get_skip_log();
        s->set_lsn_ck(lsn+size);

        // Hopefully the OS is smart enough to coalesce the writes
        // before sending them to disk. If not, and it's a problem
        // (e.g. for direct I/O), the alternative is to assemble the last
        // block by copying data out of the buffer so we can append the
        // skiplog without messing up concurrent inserts. However, that
        // could mean copying up to BLOCK_SIZE bytes.
        long total = write_size + s->length();

        // works because BLOCK_SIZE is always a power of 2
        long grand_total = ceil2(total, log_storage::BLOCK_SIZE);
        // take it up to multiple of block size
        w_assert2(grand_total % log_storage::BLOCK_SIZE == 0);

        if(grand_total == log_storage::BLOCK_SIZE) {
            // 1-block flush
            INC_TSTAT(log_short_flush);
        } else {
            // 2-or-more-block flush
            INC_TSTAT(log_long_flush);
        }

        typedef sdisk_base_t::iovec_t iovec_t;

        iovec_t iov[] = {
            // iovec_t expects void* not const void *
            iovec_t((char*)buf+start1,                end1-start1),
            // iovec_t expects void* not const void *
            iovec_t((char*)buf+start2,                end2-start2),
            iovec_t(s,                        s->length()),
            iovec_t(block_of_zeros(),         grand_total-total),
        };

        W_DO(me()->writev(_fhdl_app, iov, sizeof(iov)/sizeof(iovec_t)));

        ADD_TSTAT(log_bytes_written, grand_total);
    } // end copy skip record

    fsync_delayed(_fhdl_app); // fsync
    return RCOK;
}

rc_t partition_t::prime_buffer(char* buffer, lsn_t lsn, size_t& prime_offset)
{
    if (get_size() > 0) {
        logrec_t* lr;
        W_DO(read(lr, lsn, NULL));
        memcpy(buffer, _readbuf, XFERSIZE);
        prime_offset = (char*) lr - _readbuf;
        release_read();
    }
    else { prime_offset = 0; }

    return RCOK;
}

rc_t partition_t::read(logrec_t *&rp, lsn_t &ll, lsn_t* prev_lsn)
{
    _read_mutex.lock();

    w_assert3(is_open_for_read());

    fileoff_t pos = ll.lo();
    fileoff_t lower = pos / XFERSIZE;

    lower *= XFERSIZE;
    fileoff_t off = pos - lower;

    DBG5(<<"seek to lsn " << ll
        << " index=" << _index << " fd=" << _fhdl_rd
        << " pos=" << pos
    );

    /*
     * read & inspect header size and see
     * and see if there's more to read
     */
    int64_t b = 0;
    bool first_time = true;

    rp = (logrec_t *)(_readbuf + off);

    fileoff_t leftover = 0;

    while (first_time || leftover > 0) {

        DBG5(<<"leftover=" << int(leftover) << " b=" << b);

        W_DO(me()->pread(_fhdl_rd, (void *)(_readbuf + b), XFERSIZE, lower + b));

        b += XFERSIZE;

        if (first_time) {
            first_time = false;
            leftover = rp->length() - (b - off);
            DBG5(<<" leftover now=" << leftover);

            // Try to get lsn of previous log record (for backward scan)
            if (prev_lsn) {
                if (off >= (int64_t)sizeof(lsn_t)) {
                    // most common and easy case -- prev_lsn is on the
                    // same block
                    *prev_lsn = *((lsn_t*) (_readbuf + off - sizeof(lsn_t)));
                }
                else {
                    // we were unlucky -- extra IO required to fetch prev_lsn
                    int64_t prev_offset = lower + b - XFERSIZE - sizeof(lsn_t);
                    if (prev_offset < 0) {
                        *prev_lsn = lsn_t::null;
                    }
                    else {
                        W_COERCE(me()->pread(_fhdl_rd, (void*) prev_lsn, sizeof(lsn_t),
                                    prev_offset));
                    }
                }
            }
        } else {
            leftover -= XFERSIZE;
            w_assert3(leftover == (int)rp->length() - (b - off));
            DBG5(<<" leftover now=" << leftover);
        }
    }
    w_assert0(rp != NULL);
    w_assert0(rp->valid_header(ll));
    return RCOK;
}

void partition_t::release_read()
{
    _read_mutex.unlock();
}

rc_t partition_t::open_for_read()
{
    lock_guard<mutex> lck(_read_mutex);

    if(_fhdl_rd == invalid_fhdl) {
        string fname = _owner->make_log_name(_num);
        int fd, flags = smthread_t::OPEN_RDONLY;
        W_DO(me()->open(fname.c_str(), flags, 0, fd));

        w_assert3(_fhdl_rd == invalid_fhdl);
        _fhdl_rd = fd;
    }
    w_assert3(is_open_for_read());

    return RCOK;
}

// CS TODO: why is this definition here?
int partition_t::_artificial_flush_delay = 0;

void partition_t::fsync_delayed(int fd)
{
    static int64_t attempt_flush_delay = 0;
    // We only cound the fsyncs called as
    // a result of flush(), not from peek
    // or start-up
    INC_TSTAT(log_fsync_cnt);

    w_rc_t e = me()->fsync(fd);
    if (e.is_error()) {
        cerr
            << "cannot sync after skip block " << endl;
        W_COERCE(e);
    }

    if (_artificial_flush_delay > 0) {
        if (attempt_flush_delay==0) {
            w_assert1(_artificial_flush_delay < 99999999/1000);
            attempt_flush_delay = _artificial_flush_delay * 1000;
        }
        struct timespec req, rem;
        req.tv_sec = 0;
        req.tv_nsec = attempt_flush_delay;

        struct timeval start;
        gettimeofday(&start,0);

        while(nanosleep(&req, &rem) != 0) {
            if (errno != EINTR)  break;
            req = rem;
        }

        struct timeval stop;
        gettimeofday(&stop,0);
        int64_t diff = stop.tv_sec * 1000000 + stop.tv_usec;
        diff -= start.tv_sec *       1000000 + start.tv_usec;
        //diff is in micros.
        diff *= 1000; // now it is nanos
        attempt_flush_delay += ((_artificial_flush_delay * 1000) - diff)/8;

    }
}

rc_t partition_t::close_for_append()
{
    if (_fhdl_app != invalid_fhdl)  {
        W_DO(me()->close(_fhdl_app));
        _fhdl_app = invalid_fhdl;
    }
    return RCOK;
}

rc_t partition_t::close_for_read()
{
    if (_fhdl_rd != invalid_fhdl)  {
        W_DO(me()->close(_fhdl_rd));
        _fhdl_rd = invalid_fhdl;
    }
    return RCOK;
}

size_t partition_t::get_size(bool must_be_skip)
{
    if (_size < 0) {
        W_COERCE(scan_for_size(must_be_skip));
    }

    w_assert3(_size >= 0);
    return _size;
}

rc_t partition_t::scan_for_size(bool must_be_skip)
{
    // start scanning backwards from end of file until first valid logrec
    // is found; then check for must_be_skip
    W_DO(open_for_read());
    sthread_base_t::filestat_t statbuf;
    W_DO(me()->fstat(_fhdl_rd, statbuf));
    sm_diskaddr_t fsize = statbuf.st_size;

    if (statbuf.st_size == 0) {
        _size = 0;
        return RCOK;
    }

    w_assert3(fsize >= XFERSIZE);
    char buf[2*XFERSIZE];
    size_t bpos = fsize - XFERSIZE;
    int pos = 2*XFERSIZE - sizeof(lsn_t);
    // start reading just the last of 2 blocks, because the file may be just one block
    W_DO(me()->pread(_fhdl_rd, buf + XFERSIZE, XFERSIZE, bpos));

    lsn_t lsn;
    while (pos >= 0) {
        lsn = *((lsn_t*) (buf + pos));
        if (lsn.hi() == _num && lsn.lo() < fsize) {
            // Hi LSN bytes match an lo bytes are below current file
            // position -- good chance we've found the last logrec. Read
            // record header to check validity
            baseLogHeader h;
            W_DO(me()->pread(_fhdl_rd, &h, sizeof(baseLogHeader), lsn.lo()));
            if (h.is_valid()) {
                if (must_be_skip && h._type != logrec_t::t_skip) {
                    W_FATAL_MSG(eINTERNAL,
                            << "Found last log record in partition " << _num
                            << " but it is not a skip");
                }
                _size = lsn.lo();
                break; // Found it!
            }
        }

        if (pos == XFERSIZE) {
            // We've scanned last block and didn't find it -- read second
            // last block
            bpos -= XFERSIZE;
            W_DO(me()->pread(_fhdl_rd, buf, XFERSIZE, bpos));
        }
        pos--;
    }

    if (_size <= 0) {
        W_FATAL_MSG(eINTERNAL, << "Could lot find end of log partition " << _num);
    }

    return RCOK;
}

void partition_t::destroy()
{
    lock_guard<mutex> lck(_read_mutex);

    W_COERCE(close_for_read());
    W_COERCE(close_for_append());

    fs::path f = _owner->make_log_name(_num);
    fs::remove(f);
}
