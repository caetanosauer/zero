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
#include "log.h"
#include "log_storage.h"
#include <sys/stat.h>

// needed for skip_log
#include "logdef_gen.cpp"

partition_t::partition_t(log_storage *owner, partition_number_t num)
    : _num(num), _owner(owner),
      _fhdl_rd(invalid_fhdl), _fhdl_app(invalid_fhdl)
{
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
        long file_offset = log_storage::floor2(lsn.lo(), log_storage::BLOCK_SIZE);
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
        long grand_total = log_storage::ceil2(total, log_storage::BLOCK_SIZE);
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

rc_t partition_t::read(char* readbuf, logrec_t *&rp, lsn_t &ll,
        lsn_t* prev_lsn)
{
    INC_TSTAT(log_fetches);

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

    rp = (logrec_t *)(readbuf + off);

    DBG5(<< "off= " << ((int)off)
        << "readbuf@ " << W_ADDR(readbuf)
        << " rp@ " << W_ADDR(rp)
    );
    fileoff_t leftover = 0;

    while (first_time || leftover > 0) {

        DBG5(<<"leftover=" << int(leftover) << " b=" << b);

        W_DO(me()->pread(_fhdl_rd, (void *)(readbuf + b), XFERSIZE, lower + b));

        b += XFERSIZE;

        //
        // This could be written more simply from
        // a logical standpoint, but using this
        // first_time makes it a wee bit more readable
        //
        if (first_time) {
            if( rp->length() > sizeof(logrec_t) ||
            rp->length() < rp->header_size() ) {
                w_assert0(ll.hi() == 0); // in peek()
                return RC(eEOF);
            }
            first_time = false;
            leftover = rp->length() - (b - off);
            DBG5(<<" leftover now=" << leftover);

            // Try to get lsn of previous log record (for backward scan)
            if (prev_lsn) {
                if (off >= (int64_t)sizeof(lsn_t)) {
                    // most common and easy case -- prev_lsn is on the
                    // same block
                    *prev_lsn = *((lsn_t*) (readbuf + off - sizeof(lsn_t)));
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
    DBG5( << "readbuf@ " << W_ADDR(readbuf)
        << " first 4 chars are: "
        << (int)(*((char *)readbuf))
        << (int)(*((char *)readbuf+1))
        << (int)(*((char *)readbuf+2))
        << (int)(*((char *)readbuf+3))
    );
    w_assert1(rp != NULL);
    return RCOK;
}


rc_t partition_t::open_for_read()
{
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

size_t partition_t::truncate_for_append(partition_number_t pnum,
        const string& fname)
{
    /*
         The goal of this code is to determine where is the last complete
         log record in the log file and truncate the file at the
         end of that record.  It detects this by scanning the file and
         either reaching eof or else detecting an incomplete record.
         If it finds an incomplete record then the end of the preceding
         record is where it will truncate the file.

         The file is scanned by attempting to fread the length of a log
         record header.        If this fread does not read enough bytes, then
         we've reached an incomplete log record.  If it does read enough,
         then the buffer should contain a valid log record header and
         it is checked to determine the complete length of the record.
         Fseek is then called to advance to the end of the record.
         If the fseek fails then it indicates an incomplete record.

         *  NB:
         This is done here rather than in peek() since in the unix-file
         case, we only check the *last* partition opened, not each
         one read.
     */
    /* end of the last valid log record / start of invalid record */
    fileoff_t pos = 0;
    DBGOUT5(<<" truncate last complete log rec ");

    DBGOUT5(<<" checking " << fname);

    FILE *f =  fopen(fname.c_str(), "r");
    DBGOUT5(<<" opened " << fname << " fp " << f << " pos " << pos);

    fileoff_t start_pos = pos;

    // CS TODO: try to open on LSN higher than 0. This used to be where
    // we would get the seek position from the master LSN if the partitions
    // matched.

    // CS TODO: caller should actually check if file exists
    if (!f) { return 0; }

    allocaN<logrec_t::hdr_non_ssx_sz> buf;

    // this is now a bit more complicated because some log record
    // is ssx log, which has a shorter header.
    // (see hdr_non_ssx_sz/hdr_single_sys_xct_sz in logrec_t)
    int n;
    // this might be ssx log, so read only minimal size (hdr_single_sys_xct_sz) first
    const int log_peek_size = logrec_t::hdr_single_sys_xct_sz;
    DBGOUT5(<<"fread " << fname << " log_peek_size= " << log_peek_size);
    while ((n = fread(buf, 1, log_peek_size, f)) == log_peek_size)
    {
        DBGOUT5(<<" pos is now " << pos);
        logrec_t  *l = (logrec_t*) (void*) buf;

        if( l->type() == logrec_t::t_skip) {
            break;
        }

        smsize_t len = l->length();
        DBGOUT5(<<"scanned log rec type=" << int(l->type())
                << " length=" << l->length());

        if(len < l->header_size()) {
            // Must be garbage and we'll have to truncate this
            // partition to size 0
            w_assert1(pos == start_pos);
        } else {
            w_assert1(len >= l->header_size());

            DBGOUT5(<<"hdr_sz " << l->header_size() );
            DBGOUT5(<<"len " << len );
            // seek to lsn_ck at end of record
            // Subtract out log_peek_size because we already
            // read that (thus we have seeked past it)
            // Subtract out lsn_t to find beginning of lsn_ck.
            len -= (log_peek_size + sizeof(lsn_t));

            //NB: this is a RELATIVE seek
            DBGOUT5(<<" pos is now " << pos);
            DBGOUT5(<<"seek additional +" << len << " for lsn_ck");
            if (fseek(f, len, SEEK_CUR))  {
                if (feof(f))  break;
            }
            DBGOUT5(<<"ftell says pos is " << ftell(f));

            lsn_t lsn_ck;
            n = fread(&lsn_ck, 1, sizeof(lsn_ck), f);
            DBGOUT5(<<"read lsn_ck return #bytes=" << n );
            if (n != sizeof(lsn_ck))  {
                w_rc_t        e = RC(eOS);
                // reached eof
                if (! feof(f))  {
                    cerr << "ERROR: unexpected log file inconsistency." << endl;
                    W_COERCE(e);
                }
                break;
            }
            DBGOUT5(<<"pos = " <<  pos
                    << " lsn_ck = " <<lsn_ck);

            // make sure log record's lsn matched its position in file
            if ( (lsn_ck.lo() != pos) ||
                    (lsn_ck.hi() != (uint32_t) pnum ) ) {
                // found partial log record, end of log is previous record
                cerr << "Found unexpected end of log -- probably due to a previous crash."
                    << endl;
                cerr << "   Recovery will continue ..." << endl;
                break;
            }

            pos = ftell(f) ;
        }
    }
    fclose(f);



    DBGOUT5(<<"explicit truncating " << fname << " to " << pos);
    w_assert0(os_truncate(fname.c_str(), pos )==0);

    //
    // but we can't just use truncate() --
    // we have to truncate to a size that's a mpl
    // of the page size. First append a skip record
    DBGOUT5(<<"explicit opening  " << fname );
    f =  fopen(fname.c_str(), "a");
    if (!f) {
        w_rc_t e = RC(fcOS);
        cerr << "fopen(" << fname << "):" << endl << e << endl;
        W_COERCE(e);
    }
    skip_log *s = new skip_log; // deleted below
    s->set_lsn_ck( lsn_t(uint32_t(pnum), sm_diskaddr_t(pos)) );


    DBGOUT5(<<"writing skip_log at pos " << pos << " with lsn "
            << s->get_lsn_ck()
            << "and size " << s->length()
           );
#ifdef W_TRACE
    {
        DBGOUT5(<<"eof is now " << ftell(f));
    }
#endif

    if ( fwrite(s, s->length(), 1, f) != 1)  {
        w_rc_t        e = RC(eOS);
        cerr << "   fwrite: can't write skip rec to log ..." << endl;
        W_COERCE(e);
    }
#ifdef W_TRACE
    {
        DBGTHRD(<<"eof is now " << ftell(f));
    }
#endif
    fileoff_t o = pos;
    o += s->length();
    o = o % XFERSIZE;
    DBGOUT5(<<"BLOCK_SIZE " << int(BLOCK_SIZE));
    if(o > 0) {
        o = XFERSIZE - o;
        char *junk = new char[int(o)]; // delete[] at close scope
        if (!junk)
            W_FATAL(fcOUTOFMEMORY);
#ifdef ZERO_INIT
#if W_DEBUG_LEVEL > 4
        fprintf(stderr, "ZERO_INIT: Clearing before write %d %s\n",
                __LINE__
                , __FILE__);
#endif
        memset(junk,'\0', int(o));
#endif

        DBGOUT5(<<"writing junk of length " << o);
#ifdef W_TRACE
        {
            DBGOUT5(<<"eof is now " << ftell(f));
        }
#endif
        n = fwrite(junk, int(o), 1, f);
        if ( n != 1)  {
            w_rc_t e = RC(eOS);
            cerr << "   fwrite: can't round out log block size ..." << endl;
            W_COERCE(e);
        }

#ifdef W_TRACE
        {
            DBGOUT5(<<"eof is now " << ftell(f));
        }
#endif
        delete[] junk;
        o = 0;
    }
    delete s; // skip_log

    fileoff_t eof = ftell(f);
    w_rc_t e = RC(eOS);        /* collect the error in case it is needed */
    DBGOUT5(<<"eof is now " << eof);


    if(((eof) % XFERSIZE) != 0) {
        cerr <<
            "   ftell: can't write skip rec to log ..." << endl;
        W_COERCE(e);
    }
    W_IGNORE(e);        /* error not used */

    if (os_fsync(fileno(f)) < 0) {
        e = RC(eOS);
        cerr << "   fsync: can't sync fsync truncated log ..." << endl;
        W_COERCE(e);
    }

#if W_DEBUG_LEVEL > 2
    {
        os_stat_t statbuf;
        if (os_fstat(fileno(f), &statbuf) == -1) {
            e = RC(eOS);
        } else {
            e = RCOK;
        }
        if (e.is_error()) {
            cerr << " Cannot stat fd " << fileno(f)
                << ":" << endl << e << endl << endl;
            W_COERCE(e);
        }
        DBGOUT5(<< "size of " << fname << " is " << statbuf.st_size);
    }
#endif
    fclose(f);

    return pos;
}
