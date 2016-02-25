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

// needed for skip_log
#include "logdef_gen.cpp"

#ifdef LOG_DIRECT_IO
// use the log manager's write buffer as a temp buffer to store the last block of
// the unflushed log records and a skip record
//char *             partition_t::writebuf { return _owner->writebuf(); }
#endif

#if W_DEBUG_LEVEL > 2
void
partition_t::check_fhdl_rd() const {
    bool isopen = is_open_for_read();
    if(_fhdl_rd == invalid_fhdl) {
        w_assert3( !isopen );
    } else {
        w_assert3(isopen);
    }
}
void
partition_t::check_fhdl_app() const {
    if(_fhdl_app != invalid_fhdl) {
        w_assert3(is_open_for_append());
    } else {
        w_assert3(! is_open_for_append());
    }
}
#endif

bool
partition_t::is_current()  const
{
    //  rd could be open
    if(index() == _owner->partition_index()) {
        w_assert3(num()>0);
        w_assert3(_owner->partition_num() == num());
        w_assert3(exists());
        w_assert3(_owner->curr_partition() == this);
        w_assert3(_owner->partition_index() == index());
        w_assert3(this->is_open_for_append());

        return true;
    }
#if W_DEBUG_LEVEL > 2
    if(num() == 0) {
        w_assert3(!this->exists());
    }
#endif
    return false;
}


/*
 * open_for_append(num, end_hint)
 * "open" a file  for the given num for append, and
 * make it the current file.
 */
// MUTEX: flush, insert, partition
void
partition_t::open_for_append(partition_number_t __num,
        const lsn_t& end_hint)
{
    // shouldn't be calling this if we're already open
    w_assert3(!is_open_for_append());
    // We'd like to use this assertion, but in the
    // raw case, it's wrong: fhdl_app() is NOT synonymous
    // with is_open_for_append() and the same goes for ...rd()
    // w_assert3(fhdl_app() == 0);

    int         fd;

    DBG(<<"open_for_append num()=" << num()
            << "__num=" << __num
            << "_num=" << _num
            << " about to peek");

    /*
    if(num() == __num) {
        close_for_read();
        close_for_append();
        _num = 0; // so the peeks below
        // will work -- it'll get reset
        // again anyway.
   }
   */
    /* might not yet know its size - discover it now  */
    peek(__num, end_hint, true, &fd); // have to know its size
    w_assert3(fd);
    if(size() == nosize) {
        // we're opening a new partition
        set_size(0);
    }

    _num = __num;
    // size() was set in peek()
    w_assert1(size() != partition_t::nosize);

    _set_fhdl_app(fd);
    _set_state(m_flushed);
    _set_state(m_exists);
    _set_state(m_open_for_append);

    _owner->set_current( index(), num() );
    return ;
}

void
partition_t::clear()
{
    _num=0;
    _size = nosize;
    _mask=0;
    _clr_state(m_open_for_read);
    _clr_state(m_open_for_append);
    DBG5(<<"partition_t::clear num " << num() << " clobbering "
            << _fhdl_rd << " and " << _fhdl_app);
    _fhdl_rd = invalid_fhdl;
    _fhdl_app = invalid_fhdl;
}

void
partition_t::init(log_storage *owner)
{
    _owner = owner;
    _eop = owner->limit(); // always
    DBG5(<< "partition_t::init setting _eop to " << _eop );
    clear();
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

#ifdef LOG_DIRECT_IO
/*
 * partition::flush(int fd, bool force)
 * flush to disk whatever's been buffered.
 * Do this with a writev of 4 parts:
 * start->end1 where start is start1 rounded down to the beginning of a BLOCK
 * start2->end2
 * a skip record
 * enough zeroes to make the entire write become a multiple of BLOCK_SIZE
 */
void
partition_t::flush(
        char* writebuf,
        int fd, // not necessarily fhdl_app() since flush is called from
        // skip, when peeking and this might be in recovery.
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

    DBG5( << "Sync-ing log lsn " << lsn
                << " start1 " << start1
                << " end1 " << end1
                << " start2 " << start2
                << " end2 " << end2 );

    // This change per e-mail from Ippokratis, 16 Jun 09:
    // long file_offset = _owner->floor(lsn.lo(), log_storage::BLOCK_SIZE);
    // works because BLOCK_SIZE is always a power of 2
    long file_offset = log_storage::floor2(lsn.lo(), log_storage::BLOCK_SIZE);
    // offset is rounded down to a block_size
    long delta = lsn.lo() - file_offset;

    bool handle_end1 = false;

    if (start1 == end1) {
        // case 1: no wrap, flush records from start2 to end2
        w_assert1(start2<=end2);  // start2 == end2 if it is called from _skip


        // adjust down to the nearest full block
        w_assert1(start2 >= delta);
        write_size += delta; // account for the extra (clean) bytes
        start2 -= delta;

        // make sure start1 and end1 are both aligned
        start1 = end1 = 0;

        // need to handle end2
        handle_end1 = false;
    }
    else {
        if (start2 != end2) {
            // case 2: wrapped, flush both start1 to end1 and start2 to end2
            w_assert1(start2 == 0);
            w_assert1(end1 % log_storage::BLOCK_SIZE == 0); // already aligned

            // adjust down to the nearest full block
            w_assert1(start1 >= delta);
            write_size += delta; // account for the extra (clean) bytes
            start1 -= delta;

            // need to handle end2
            handle_end1 = false;

        }
        else {
            // case 3: new partition, flush records from start1 to end1
            w_assert1(start2==0 && end2 ==0);

            // adjust down to the nearest full block
            w_assert1(start1 >= delta);
            write_size += delta; // account for the extra (clean) bytes
            start1 -= delta;

            // make sure start2 and end2 are both aligned
            start2 = end2 = 0;


            // need to handle end1
            handle_end1 = true;
        }
    }

    // seek to the correct offset
    fileoff_t where = start() + file_offset;
    w_rc_t e = me()->lseek(fd, where, sthread_t::SEEK_AT_SET);
    if (e.is_error()) {
        W_FATAL_MSG(e.err_num(), << "ERROR: could not seek to "
                    << file_offset
                    << " + " << start()
                    << " to write log record"
                    << endl);
    }

    // prepare a skip record
    skip_log* s = _owner->get_skip_log();
    s->set_lsn_ck(lsn+size);

    long total = write_size + s->length();

    // This change per e-mail from Ippokratis, 16 Jun 09:
    // long grand_total = _owner->ceil(total, log_storage::BLOCK_SIZE);
    // works because BLOCK_SIZE is always a power of 2
    long grand_total = log_storage::ceil2(total, log_storage::BLOCK_SIZE);
    // take it up to multiple of block size
    w_assert2(grand_total % log_storage::BLOCK_SIZE == 0);

    uint64_t zeros = grand_total - total;


    if(grand_total == log_storage::BLOCK_SIZE) {
        // 1-block flush
        INC_TSTAT(log_short_flush);
    } else {
        // 2-or-more-block flush
        INC_TSTAT(log_long_flush);
    }


    // now we deal with end1 or end2
    // to meet the alignment requirement, make a copy of the last block of
    // the log buffer and append a skip log record to it
    int64_t offset = 0;    // offset of end1 or end2 in a block
    int64_t temp_end = 0;  // end offset inside the temp write buffer

    if (handle_end1 == false) {
        // case 1 & 2
        // handle end2
        offset = end2 % log_storage::BLOCK_SIZE;
        end2 -= offset;

        // copy the last unaligned portion from log buffer to the temp write buffer
        memcpy(writebuf, (char*)buf+end2, offset);
        temp_end += offset;

        // copy the skip record to the temp write buffer
        memcpy(writebuf+temp_end, (char*)s, s->length());
        temp_end += s->length();

        // pad zero to make the real end
        memcpy(writebuf+temp_end, block_of_zeros(), zeros);
        temp_end += zeros;
    }
    else {
        // case 3
        w_assert1(start1 != end1);

        // handle end1
        offset = end1 % log_storage::BLOCK_SIZE;
        end1 -= offset;

        // copy the last unaligned portion from log buffer to the temp write buffer
        memcpy(writebuf, (char*)buf+end1, offset);
        temp_end += offset;

        // copy the skip record to the temp write buffer
        memcpy(writebuf+temp_end, (char*)s, s->length());
        temp_end += s->length();

        // pad zero to make the real end
        memcpy(writebuf+temp_end, block_of_zeros(), zeros);
        temp_end += zeros;
    }


    // finally, we can create the io vector
    {
        typedef sdisk_base_t::iovec_t iovec_t;

        iovec_t iov[] = {
            iovec_t((char*)buf+start1,                end1-start1),
            iovec_t((char*)buf+start2,                end2-start2),
            iovec_t((char*)writebuf, temp_end),
        };

        w_assert1((long)(buf+start1)%LOG_DIO_ALIGN == 0);
        w_assert1((long)(buf+start2)%LOG_DIO_ALIGN == 0);
        w_assert1((long)(writebuf)%LOG_DIO_ALIGN == 0);

        w_assert1((end1-start1)%LOG_DIO_ALIGN == 0);
        w_assert1((end2-start2)%LOG_DIO_ALIGN == 0);
        w_assert1((temp_end)%LOG_DIO_ALIGN == 0);



        w_rc_t e = me()->writev(fd, iov, sizeof(iov)/sizeof(iovec_t));
        if (e.is_error()) {
            cerr
                << "ERROR: could not flush log buf:"
                << " fd=" << fd
                << " xfersize=" << log_storage::BLOCK_SIZE
                << log_storage::BLOCK_SIZE
                << " vec parts: "
                << " " << iov[0].iov_len
                << " " << iov[1].iov_len
                << " " << iov[2].iov_len
                << ":" << endl
                << e
                << endl;
            W_COERCE(e);
        }
    }

    ADD_TSTAT(log_bytes_written, grand_total);

    // TODO: not necessary since the file is opened with O_SYNC
    this->flush(fd); // fsync
}
#else
/*
 * partition::flush(int fd, bool force)
 * flush to disk whatever's been buffered.
 * Do this with a writev of 4 parts:
 * start->end1 where start is start1 rounded down to the beginning of a BLOCK
 * start2->end2
 * a skip record
 * enough zeroes to make the entire write become a multiple of BLOCK_SIZE
 */
void
partition_t::flush(
        int fd, // not necessarily fhdl_app() since flush is called from
        // skip, when peeking and this might be in recovery.
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

        // This change per e-mail from Ippokratis, 16 Jun 09:
        // long file_offset = _owner->floor(lsn.lo(), log_storage::BLOCK_SIZE);
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
        w_rc_t e = me()->lseek(fd, where, sthread_t::SEEK_AT_SET);
        if (e.is_error()) {
            W_FATAL_MSG(e.err_num(), << "ERROR: could not seek to "
                                    << file_offset
                                    << " to write log record"
                                    << endl);
        }
    } // end sync log

    /*
       stolen from log_buf::write_to
    */
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

        // This change per e-mail from Ippokratis, 16 Jun 09:
        // long grand_total = _owner->ceil(total, log_storage::BLOCK_SIZE);
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

        w_rc_t e = me()->writev(fd, iov, sizeof(iov)/sizeof(iovec_t));
        if (e.is_error()) {
            cerr
                                    << "ERROR: could not flush log buf:"
                                    << " fd=" << fd
                                << " xfersize="
                                << log_storage::BLOCK_SIZE
                                << " vec parts: "
                                << " " << iov[0].iov_len
                                << " " << iov[1].iov_len
                                << " " << iov[2].iov_len
                                << " " << iov[3].iov_len
                                    << ":" << endl
                                    << e
                                    << endl;
            W_COERCE(e);
        }

        ADD_TSTAT(log_bytes_written, grand_total);
    } // end copy skip record

    this->flush(fd); // fsync
}
#endif // LOG_DIRECT_IO

/*
 *  partition_t::_peek(num, peek_loc, whole_size,
        recovery, fd) -- used by both -- contains
 *   the guts
 *
 *  Peek at a partition num() -- see what num it represents and
 *  if it's got anything other than a skip record in it.
 *
 *  If recovery==true,
 *  determine its size, if it already exists (has something
 *  other than a skip record in it). In this case its num
 *  had better match num().
 *
 *  If it's just a skip record, consider it not to exist, and
 *  set _num to 0, leave it "closed"
 *
 *********************************************************************/
void
partition_t::_peek(
    partition_number_t num_wanted,
    fileoff_t        peek_loc,
    fileoff_t        whole_size,
    bool recovery,
    int fd
)
{
    DBG5("_peek: num_wanted=" << num_wanted << " peek_loc=" << peek_loc
            << " whole_size=" << whole_size << " recovery=" << recovery
            << " fd=" << fd);
    w_assert3(num() == 0 || num() == num_wanted);
    clear();

    _clr_state(m_exists);
    _clr_state(m_flushed);
    _clr_state(m_open_for_read);

    w_assert3(fd);

    logrec_t        *l = NULL;

    // seek to start of partition or to the location given
    // in peek_loc -- that's a location we suspect might
    // be the end of-the-log skip record.
    //
    // the lsn passed to read(rec,lsn) is not
    // inspected for its hi() value
    //
    bool  peeked_high = false;
    if(    (peek_loc != partition_t::nosize)
        && (peek_loc <= this->_eop)
        && (peek_loc < whole_size) ) {
        peeked_high = true;
    } else {
        peek_loc = 0;
        peeked_high = false;
    }
    DBG5(
            << " peek_loc " << peek_loc
            << " nosize " << nosize
            << " _eop " << this->_eop
            << " peeked_high " << peeked_high);

    // We should never have written a partition larger than
    // that determined to be the maximum. If we hit this assert,
    // it should be because we changed log size between writing the
    // log and recovering.
    w_assert1(whole_size <= this->_eop);
again:
    lsn_t pos = lsn_t(uint32_t(num()), sm_diskaddr_t(peek_loc));
    DBG5("peek_loc " << peek_loc << " yields sm_diskaddr/pos " << pos);

    lsn_t lsn_ck = pos ;
    w_rc_t rc;

    while(pos.lo() < this->_eop) {
        DBG5("pos.lo() = " << pos.lo()
                << " and eop=" << this->_eop);
        if(recovery) {
            // increase the starting point as much as possible.
            // to decrease the time for recovery
            if(pos.hi() == _owner->master_lsn().hi() &&
               pos.lo() < _owner->master_lsn().lo())  {
                      pos = _owner->master_lsn();
            }
        }
        DBG5( <<"reading pos=" << pos <<" eop=" << this->_eop);

        rc = read(_peekbuf, l, pos, NULL, fd);
        DBG5(<<"POS " << pos << ": tx." << *l);

        if(rc.err_num() == eEOF) {
            // eof or record -- wipe it out
            DBG5(<<"EOF--Skipping!");
            _skip(pos, fd);
            break;
        }
        if (rc.err_num() == stSHORTIO) {
            // Crash interrupted a log page write (of XFERSIZE) and
            // we are now reading the last incomplete block. Simply keep
            // parsing like nothing happened -- the end of log will be
            // detected at the first inconsistent log record.
        }

        w_assert1(l != NULL);

        DBG5(<<"peek index " << _index
            << " l->length " << l->length()
            << " l->type " << int(l->type()));

        w_assert1(l->length() >= l->header_size());
        {
            // check lsn
            lsn_ck = l->get_lsn_ck();
            int err = 0;

            DBG5( <<"lsnck=" << lsn_ck << " pos=" << pos
                <<" l.length=" << l->length() );


            if( ( l->length() <l->header_size() )
                ||
                ( l->length() > sizeof(logrec_t) )
                ||
                ( lsn_ck.lo() !=  pos.lo() )
                ||
                (num_wanted  && (lsn_ck.hi() != num_wanted) )
                ) {
                err++;
            }

            if( num_wanted  && (lsn_ck.hi() != num_wanted) ) {
                // Wrong partition - break out/return
                DBG5(<<"NOSTASH because num_wanted="
                        << num_wanted
                        << " lsn_ck="
                        << lsn_ck
                    );
                return;
            }

            DBG5( <<"type()=" << int(l->type())
                << " index()=" << this->index()
                << " lsn_ck=" << lsn_ck
                << " err=" << err );

            /*
            // if it's a skip record, and it's the first record
            // in the partition, its lsn might be null.
            //
            // A skip record that's NOT the first in the partiton
            // will have a correct lsn.
            */

#if W_DEBUG_LEVEL > 3
            if( l->type() == logrec_t::t_skip ) {
                cerr << "Found skip record " << " at " << pos << endl;
            }
#endif
            if( l->type() == logrec_t::t_skip   &&
                pos == first_lsn()) {
                // it's a skip record and it's the first rec in partition
                if( lsn_ck != lsn_t::null )  {
                    DBG5( <<" first rec is skip and has lsn " << lsn_ck );
                    err = 1;
                }
            } else {
                // ! skip record or ! first in the partition
                if ( (lsn_ck.hi()-1) % PARTITION_COUNT != (uint32_t)this->index()) {
                    DBG5( <<"unexpected end of log");
                    err = 2;
                }
            }
            if(err > 0) {
                // bogus log record,
                // consider end of log to be previous record

                if(err > 1) {
                    cerr << "Found unexpected end of log --"
                        << " pos " << pos
                        << " with lsn_ck " << lsn_ck
                        << " probably due to a previous crash."
                        << endl;
                }

                if(peeked_high) {
                    // set pos to 0 and start this loop all over
                    DBG5( <<"Peek high failed at loc " << pos);
                    peek_loc = 0;
                    peeked_high = false;
                    goto again;
                }

                /*
                // Incomplete record -- wipe it out
                */
#if W_DEBUG_LEVEL > 2
                if(pos.hi() != 0) {
                   w_assert3(pos.hi() == num_wanted);
                }
#endif

                // assign to lsn_ck so that the when
                // we drop out the loop, below, pos is set
                // correctly.
                lsn_ck = lsn_t(num_wanted, pos.lo());
                DBG5(<<"truncating partition fd=" << fd << " at " << lsn_ck);
                _skip(lsn_ck, fd);
                w_assert0(lsn_ck.lo()==0); // for debugging
                break;
            }
        }
        // DBG5(<<" changing pos from " << pos << " to " << lsn_ck );
        pos = lsn_ck;

        DBG5(<< " recovery=" << recovery
            << " master=" << _owner->master_lsn()
        );
        if( l->type() == logrec_t::t_skip
            || !recovery) {
            /*
             * IF
             *  we hit a skip record
             * or
             *  if we're not in recovery (i.e.,
             *  we aren't trying to find the last skip log record
             *  or check each record's legitimacy)
             * THEN
             *  we've seen enough
             */
            DBG5(<<" BREAK EARLY ");
            break;
        }
        pos.advance(l->length());
    }

    // pos == 0 if the first record
    // was a skip or if we don't care about the recovery checks.

    w_assert1(l != NULL);
    DBG5(<<"pos= " << pos << "l->type()=" << int(l->type()));

#if W_DEBUG_LEVEL > 2
    if(pos.lo() > first_lsn().lo()) {
        w_assert3(l!=0);
    }
#endif

    if( pos.lo() > first_lsn().lo() || l->type() != logrec_t::t_skip ) {
        // we care and the first record was not a skip record
        _num = pos.hi();

        // let the size *not* reflect the skip record
        // and let us *not* set it to 0 (had we not read
        // past the first record, which is the case when
        // we're peeking at a partition that's earlier than
        // that containing the master checkpoint
        //
        if(pos.lo()> first_lsn().lo()) set_size(pos.lo());

        // OR first rec was a skip so we know
        // size already
        // Still have to figure out if file exists

        _set_state(m_exists);

        DBG5(<<"STASHED num()=" << num()
                << " size()=" << size()
            );
    } else {
        w_assert3(num() == 0);
        w_assert3(size() == nosize || size() == 0);
        // size can be 0 if the partition is exactly
        // a skip record
        DBG5(<<"SIZE NOT STASHED ");
    }
}


// Helper for _peek
void
partition_t::_skip(const lsn_t &ll, int fd)
{
    // Current partition should flush(), not skip()
    w_assert1(_num == 0 || _num != _owner->partition_num());

    DBG5(<<"skip at " << ll);

#ifdef LOG_DIRECT_IO
    char * _skipbuf = NULL;
    posix_memalign((void**)&_skipbuf, LOG_DIO_ALIGN, log_storage::BLOCK_SIZE*2);
#else
    char* _skipbuf = new char[log_storage::BLOCK_SIZE*2];
#endif
    // FRJ: We always need to prime() partition ops (peek, open, etc)
    // always use a different buffer than log inserts.
    long offset = _owner->prime(_skipbuf, ll, log_storage::BLOCK_SIZE);

    // Make sure that flush writes a skip record
    this->flush(
#ifdef LOG_DIRECT_IO
            _skipbuf,
#endif
            fd, ll, _skipbuf, offset, offset, offset, offset);

#ifdef LOG_DIRECT_IO
    free(_skipbuf);
#else
    delete [] _skipbuf;
#endif

    DBG5(<<"wrote and flushed skip record at " << ll);

    _set_last_skip_lsn(ll);
}

/*
 * partition_t::read(logrec_t *&rp, lsn_t &ll, int fd)
 *
 * expect ll to be correct for this partition.
 * if we're reading this for the first time,
 * for the sake of peek(), we expect ll to be
 * lsn_t(0,0), since we have no idea what
 * its lsn is supposed to be, but in fact, we're
 * trying to find that out.
 *
 * If a non-zero fd is given, the read is to be done
 * on that fd. Otherwise it is assumed that the
 * read will be done on the fhdl_rd().
 */
// MUTEX: partition
w_rc_t
partition_t::read(char* readbuf, logrec_t *&rp, lsn_t &ll,
        lsn_t* prev_lsn, int fd)
{
    INC_TSTAT(log_fetches);

    if(fd == invalid_fhdl) fd = fhdl_rd();

#if W_DEBUG_LEVEL > 2
    w_assert3(fd);
    if(exists()) {
        if(fd) w_assert3(is_open_for_read());
        w_assert3(num() == ll.hi());
    }
#endif

    fileoff_t pos = ll.lo();
    fileoff_t lower = pos / XFERSIZE;

    lower *= XFERSIZE;
    fileoff_t off = pos - lower;

    DBG5(<<"seek to lsn " << ll
        << " index=" << _index << " fd=" << fd
        << " pos=" << pos
        //<< " lower=" << lower  << " + " << start()
        << " fd=" << fd
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

        W_DO(me()->pread(fd, (void *)(readbuf + b), XFERSIZE, lower + b));

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
                        W_COERCE(me()->pread(fd, (void*) prev_lsn, sizeof(lsn_t),
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


w_rc_t
partition_t::open_for_read(
    partition_number_t  __num,
    bool err // = true.  if true, it's an error for the partition not to exist
)
{
    // protected w_assert2(_owner->_partition_lock.is_mine()==true);
    // asserted before call in srv_log.cpp

    DBG5(<<"start open for part " << __num << " err=" << err);

    w_assert1(__num != 0);

    // do the equiv of opening existing file
    // if not already in the list and opened
    //
    if(fhdl_rd() == invalid_fhdl) {
        char *fname = new char[smlevel_0::max_devname];
        if (!fname)
                W_FATAL(fcOUTOFMEMORY);

        _owner->make_log_name(__num, fname, smlevel_0::max_devname);

        int fd;
        w_rc_t e;
        DBG5(<< "partition " << __num
                << "open_for_read OPEN " << fname);

#ifdef LOG_DIRECT_IO
        int flags = smthread_t::OPEN_RDONLY | smthread_t::OPEN_DIRECT;
        //int flags = smthread_t::OPEN_RDONLY;
#else
        int flags = smthread_t::OPEN_RDONLY;
#endif
        e = me()->open(fname, flags, 0, fd);

        DBG5(<< " OPEN " << fname << " returned " << fd);

        if (e.is_error()) {
            if(err) {
                cerr
                    << "Cannot open log file: partition number "
                    << __num  << " fd" << fd << endl;
                // fatal
                W_DO(e);
            } else {
                w_assert3(! exists());
                w_assert3(_fhdl_rd == invalid_fhdl);
                // _fhdl_rd = invalid_fhdl;
                _clr_state(m_open_for_read);
                DBG5(<<"fhdl_app() is " << _fhdl_app);
                return RCOK;
            }
        }

        w_assert3(_fhdl_rd == invalid_fhdl);
        _fhdl_rd = fd;

        DBG5(<<"size is " << size());
        // size might not be known, might be anything
        // if this is an old partition

        _set_state(m_exists);
        _set_state(m_open_for_read);

        delete[] fname;
    }
    _num = __num;
    w_assert3(exists());
    w_assert3(is_open_for_read());
    // might not be flushed, but if
    // it isn't, surely it's flushed up to
    // the offset we're reading
    //w_assert3(flushed());

    w_assert3(_fhdl_rd != invalid_fhdl);
    DBG5(<<"_fhdl_rd = " <<_fhdl_rd );
    return RCOK;
}

/*
 * close for append, or if both==true, close
 * the read-file also
 */
void
partition_t::close(bool both)
{
    bool err_encountered=false;
    w_rc_t e;

    // protected member: w_assert2(_owner->_partition_lock.is_mine()==true);
    // assert is done by callers
    if(is_current()) {
        // This assertion is bad -- the log flusher is probably trying
        // to update dlsn right now!
        //        w_assert1(dlsn.hi() > num());
        //        _owner->_flush(_owner->curr_lsn());
        //w_assert3(flushed());
        _owner->unset_current();
    }
    if (both) {
        if (fhdl_rd() != invalid_fhdl) {
            DBG5(<< " CLOSE " << fhdl_rd());
            e = me()->close(fhdl_rd());
            if (e.is_error()) {
                cerr
                        << "ERROR: could not close the log file."
                        << e << endl << endl;
                err_encountered = true;
            }
        }
        _fhdl_rd = invalid_fhdl;
        _clr_state(m_open_for_read);
    }

    if (is_open_for_append()) {
        DBG5(<< " CLOSE " << fhdl_rd());
        e = me()->close(fhdl_app());
        if (e.is_error()) {
            cerr
            << "ERROR: could not close the log file."
            << endl << e << endl << endl;
            err_encountered = true;
        }
        _fhdl_app = invalid_fhdl;
        _clr_state(m_open_for_append);
        DBG5(<<"fhdl_app() is " << _fhdl_app);
    }

    _clr_state(m_flushed);
    if (err_encountered) {
        W_COERCE(e);
    }
}


void
partition_t::sanity_check() const
{
    if(num() == 0) {
       // initial state
       w_assert3(size() == nosize);
       w_assert3(!is_open_for_read());
       w_assert3(!is_open_for_append());
       w_assert3(!exists());
       // don't even ask about flushed
    } else {
       w_assert3(exists());
       (void) is_open_for_read();
       (void) is_open_for_append();
    }
    if(is_current()) {
       w_assert3(is_open_for_append());
    }
}



/**********************************************************************
 *
 *  partition_t::destroy()
 *
 *  Destroy a log file.
 *
 *********************************************************************/
void
partition_t::destroy()
{
    w_assert3(num() < _owner->global_min_lsn().hi());

    if(num()>0) {
        w_assert3(exists());
        w_assert3(! is_current() );
        w_assert3(! is_open_for_read() );
        w_assert3(! is_open_for_append() );

        _owner->destroy_file(num(), true);
        _clr_state(m_exists);
        // _num = 0;
        DBG(<< " calling clear");
        clear();
    }
    w_assert3( !exists());
    sanity_check();
}


void
partition_t::peek(
    partition_number_t  __num,
    const lsn_t&        end_hint,
    bool                 recovery,
    int *                fdp
)
{
    // this is a static func so we cannot assert this:
    // w_assert2(_owner->_partition_lock.is_mine()==true);
    int fd;

    // Either we have nothing opened or we are peeking at something
    // already opened.
    w_assert2(num() == 0 || num() == __num);
    w_assert3(__num != 0);

    if( num() ) {
        close_for_read();
        close_for_append();
        DBG(<< " calling clear");
        clear();
    }

    _clr_state(m_exists);
    _clr_state(m_flushed);

    char *fname = new char[smlevel_0::max_devname];
    if (!fname)
        W_FATAL(fcOUTOFMEMORY);
    _owner->make_log_name(__num, fname, smlevel_0::max_devname);

    smlevel_0::fileoff_t part_size = fileoff_t(0);

    DBG5(<<"partition " << __num << " peek opening " << fname);

    // first create it if necessary.
#ifdef LOG_DIRECT_IO
    int flags = smthread_t::OPEN_RDWR | smthread_t::OPEN_SYNC | smthread_t::OPEN_CREATE | smthread_t::OPEN_DIRECT;
#else
    int flags = smthread_t::OPEN_RDWR | smthread_t::OPEN_SYNC | smthread_t::OPEN_CREATE;
#endif // LOG_DIRECT_IO

    w_rc_t e;
    e = me()->open(fname, flags, 0744, fd);
    if (e.is_error()) {
        cerr
            << "ERROR: cannot open log file: " << endl << e << endl;
        W_COERCE(e);
    }
    DBG5(<<"partition " << __num << " peek  opened " << fname);
    {
         sthread_base_t::filestat_t statbuf;
         e = me()->fstat(fd, statbuf);
         if (e.is_error()) {
             cerr
                << " Cannot stat fd " << fd << ":"
                << endl << e  << endl;
                W_COERCE(e);
         }
         part_size = statbuf.st_size;
         DBG5(<< "partition " << __num << " peek "
             << "size of " << fname << " is " << part_size);
    }


    // We will eventually want to write a record with the durable
    // lsn.  But if this is start-up and we've initialized
    // with a partial partition, we have to prime the
    // buf with the last block in the partition.
    //
    // If this was a pre-existing partition, we have to scan it
    // to find the *real* end of the file.

    if( part_size > 0 ) {
        w_assert3(__num == end_hint.hi() || end_hint.hi() == 0);
        _peek(__num, end_hint.lo(), part_size, recovery, fd);
    } else {
        // write a skip record so that prime() can
        // cope with it.
        // Have to do this carefully -- since using
        // the standard insert()/write code causes a
        // prime() to occur and that doesn't solve anything.

        DBG5(<<" peek DESTROYING PARTITION " << __num << "  on fd " << fd);

        // First: write any-old junk
        e = me()->ftruncate(fd,  log_storage::BLOCK_SIZE );
        if (e.is_error())        {
            cerr
                << "cannot write garbage block " << endl;
            W_COERCE(e);
        }
        /* write the lsn of the up-coming skip record */

        // Now write the skip record and flush it to the disk:
        _skip(first_lsn(__num), fd);

        // First: write any-old junk
        e = me()->fsync(fd);
        if (e.is_error()) {
            cerr
                << "cannot sync after skip block " << endl;
            W_COERCE(e);
        }

        // Size is 0
        set_size(0);
    }

    if (fdp) {
        DBG5(<< "partition " << __num << " SAVED, NOT CLOSED fd " << fd); *fdp = fd;
    } else {
        DBG5(<< " CLOSE " << fd);
        e = me()->close(fd);
        if (e.is_error()) {
            cerr
            << "ERROR: could not close the log file." << endl;
            W_COERCE(e);
        }

    }

    delete[] fname;
}

void
partition_t::flush(int fd)
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
int partition_t::_artificial_flush_delay = 0;


void
partition_t::close_for_append()
{
    int f = fhdl_app();
    if (f != invalid_fhdl)  {
        w_rc_t e;
        DBG5(<< " CLOSE " << f);
        e = me()->close(f);
        if (e.is_error()) {
            cerr
                << "warning: error in unix log on close(app):"
                    << endl <<  e << endl;
        }
        _fhdl_app = invalid_fhdl;
    }
}

void
partition_t::close_for_read()
{
    int f = fhdl_rd();
    if (f != invalid_fhdl)  {
        w_rc_t e;
        DBG5(<< " CLOSE " << f);
        e = me()->close(f);
        if (e.is_error()) {
            cerr
                << "warning: error in unix partition on close(rd):"
                << endl <<  e << endl;
        }
        _fhdl_rd = invalid_fhdl;
    }
}

/*********************************************************************
 *
 *  void partition_t::flush(int fd, lsn_t lsn, int64_t size, int64_t write_size,
 *                          sdisk_base_t::iovec_t *iov, uint32_t seg_cnt)
 *
 *  Flush log records in the io vectors to physical log partition
 *
 *********************************************************************/
#ifdef LOG_DIRECT_IO
void
partition_t::flush(
                char* writebuf,
                   int fd, // IN: the file descriptor of the current log partition
                   lsn_t lsn, // IN: the lsn of the first log record we want to flush
                   int64_t size, // IN: the total size of log records
                   int64_t write_size,  // IN: the aligned total size
                   sdisk_base_t::iovec_t *iov, // IN: io vector array; each vector corresponds to a segment
                   uint32_t seg_cnt // IN: number of io vectors/segments
)
{

    DBG5( << "Sync-ing log lsn " << lsn
             << " write_size " << write_size
             << " seg_cnt " << seg_cnt );

    // This change per e-mail from Ippokratis, 16 Jun 09:
    // long file_offset = _owner->floor(lsn.lo(), log_storage::BLOCK_SIZE);
    // works because BLOCK_SIZE is always a power of 2
    long file_offset = log_storage::floor2(lsn.lo(), log_storage::BLOCK_SIZE);
    // offset is rounded down to a block_size


    // seek to the correct offset
    fileoff_t where = start() + file_offset;
    w_rc_t e = me()->lseek(fd, where, sthread_t::SEEK_AT_SET);
    if (e.is_error()) {
        W_FATAL_MSG(e.err_num(), << "ERROR: could not seek to "
                    << file_offset
                    << " + " << start()
                    << " to write log record"
                    << endl);
    }

    // prepare a skip record
    skip_log* s = _owner->get_skip_log();
    s->set_lsn_ck(lsn+size);


    long total = write_size + s->length();

    // This change per e-mail from Ippokratis, 16 Jun 09:
    // long grand_total = _owner->ceil(total, log_storage::BLOCK_SIZE);
    // works because BLOCK_SIZE is always a power of 2
    long grand_total = log_storage::ceil2(total, log_storage::BLOCK_SIZE);
    // take it up to multiple of block size
    w_assert2(grand_total % log_storage::BLOCK_SIZE == 0);

    uint64_t zeros = grand_total - total;


    if(grand_total == log_storage::BLOCK_SIZE) {
        // 1-block flush
        INC_TSTAT(log_short_flush);
    } else {
        // 2-or-more-block flush
        INC_TSTAT(log_long_flush);
    }


    // now we deal with the skip log record
    // to meet the alignment requirement, make a copy of the last block of
    // the log buffer and append a skip log record to it
    char *buf = (char*)iov[seg_cnt-1].iov_base;
    size_t &end = iov[seg_cnt-1].iov_len;
    int64_t offset = 0;    // offset of end into a block
    int64_t temp_end = 0;  // end offset inside the temp write buffer

    offset = end % log_storage::BLOCK_SIZE;
    end -= offset;

    // copy the last unaligned portion from log buffer to the temp write buffer
    memcpy(writebuf, buf+end, offset);
    temp_end += offset;

    // copy the skip record to the temp write buffer
    memcpy(writebuf+temp_end, (char*)s, s->length());
    temp_end += s->length();

    // pad zero to make the real end aligned to blocksize
    memcpy(writebuf+temp_end, block_of_zeros(), zeros);
    temp_end += zeros;

    typedef sdisk_base_t::iovec_t iovec_t;

    // new iovec: skip record + zeros
    iov[seg_cnt] = iovec_t((char*)writebuf, temp_end);


    // finally, we write the iovecs
    {
        w_rc_t e = me()->writev(fd, iov, seg_cnt+1);
        //w_rc_t e;

        if (e.is_error()) {
            cerr
                                    << "ERROR: could not flush log buf:"
                                    << " fd=" << fd
                                    << " xfersize=" << log_storage::BLOCK_SIZE
                                << log_storage::BLOCK_SIZE
                                    << ":" << endl
                                    << e
                                    << endl;
            W_COERCE(e);
        }

        ADD_TSTAT(log_bytes_written, grand_total);
    }

    // TODO: not necessary since the file is opened with O_SYNC
    this->flush(fd); // fsync


    // read the log records back to verify that they were indeed written to disk
#ifdef LOG_VERIFY_FLUSHES
    char *read_buf = NULL;

    // make sure the read buffer is properly aligned
    posix_memalign((void**)&read_buf, LOG_DIO_ALIGN, _owner->segsize());

    int max_seg_cnt = seg_cnt+1;

    uint64_t read_offset = file_offset;
    for(int i=0; i<max_seg_cnt; i++) {
        size_t iov_len = iov[i].iov_len;
        void *iov_buf = iov[i].iov_base;
        w_rc_t e = me()->pread(fd, read_buf, iov_len, start() + read_offset);
        if (e.is_error()) {
            cerr
                                    << "FLUSH VERIFICATION: read failed" << endl;
            W_COERCE(e);
            delete read_buf;
            return;
        }
        if (memcmp(iov_buf, read_buf, iov_len)!=0) {
            cerr
                                    << "FLUSH VERIFICATION FAILED" << endl;
            W_FATAL(eINTERNAL);
            delete read_buf;
            return;
        }
        read_offset+=iov_len;
    }

    free(read_buf);

#endif // LOG_VERIFY_FLUSHES

}
#else // LOG_DIRECT_IO
void
partition_t::flush(int fd, lsn_t lsn, int64_t size, int64_t write_size,
                   sdisk_base_t::iovec_t *iov, uint32_t seg_cnt)
{
    { // sync log: Seek the file to the right place.
        DBG5( << "Sync-ing log lsn " << lsn
                 << " write_size " << write_size
                 << " seg_cnt " << seg_cnt );

        // This change per e-mail from Ippokratis, 16 Jun 09:
        // long file_offset = _owner->floor(lsn.lo(), log_storage::BLOCK_SIZE);
        // works because BLOCK_SIZE is always a power of 2
        long file_offset = log_storage::floor2(lsn.lo(), log_storage::BLOCK_SIZE);
        // offset is rounded down to a block_size


        /* FRJ: This seek is safe (in theory) because only one thread
           can flush at a time and all other accesses to the file use
           pread/pwrite (which doesn't change the file pointer).
         */
        fileoff_t where = file_offset;
        w_rc_t e = me()->lseek(fd, where, sthread_t::SEEK_AT_SET);
        if (e.is_error()) {
            W_FATAL_MSG(e.err_num(), << "ERROR: could not seek to "
                                    << file_offset
                                    << " to write log record"
                                    << endl);
        }
    } // end sync log

    /*
       stolen from log_buf::write_to
    */
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

        // This change per e-mail from Ippokratis, 16 Jun 09:
        // long grand_total = _owner->ceil(total, log_storage::BLOCK_SIZE);
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


        // skip record
        iov[seg_cnt] = iovec_t(s, s->length());

        // padding 0's
        iov[seg_cnt+1] = iovec_t(block_of_zeros(), grand_total-total);

        //DBGOUT3(<< "p->flush:  count " << sizeof(iov)/sizeof(iovec_t));

        w_rc_t e = me()->writev(fd, iov, seg_cnt+2);
        //w_rc_t e;

        if (e.is_error()) {
            cerr
                                    << "ERROR: could not flush log buf:"
                                    << " fd=" << fd
                                    << " xfersize=" << log_storage::BLOCK_SIZE
                                << log_storage::BLOCK_SIZE
                                    << ":" << endl
                                    << e
                                    << endl;
            W_COERCE(e);
        }

        ADD_TSTAT(log_bytes_written, grand_total);
    } // end copy skip record

    this->flush(fd); // fsync
}
#endif // LOG_DIRECT_IO


/*********************************************************************
 *
 *  w_rc_t partition_t::read_seg(lsn_t ll, char *buf, uint32_t size, int fd)
 *
 *  Read an entire segment from a log partition
 *
 *********************************************************************/
w_rc_t
partition_t::read_seg(
                      lsn_t ll, // IN: the base lsn of the segment
                      char *buf, // IN: the in-mem buffer in the segment descriptor
                      uint32_t size,  // IN: size of the read (segment + tail blocks)
                      int fd // IN: the file descriptor of the partition we are going to read from
)
{

    if(fd == invalid_fhdl) fd = fhdl_rd();

#if W_DEBUG_LEVEL > 2
    w_assert3(fd);
    if(exists()) {
        if(fd) w_assert3(is_open_for_read());
        w_assert3(num() == ll.hi());
    }
#endif


    fileoff_t pos = ll.lo();

    w_assert1(pos%XFERSIZE == 0);
    w_assert1(size%XFERSIZE == 0);

    DBGOUT3(
        << " read_seg: lsn "
        << ll
        << " pos "
        << pos
        << " size "
        << size
    );

    w_rc_t e = me()->pread(fd, buf, size, pos);

    if (e.is_error()) {
        /* accept the short I/O error for now */
        // ignore short I/O error
        if(e.err_num() != stSHORTIO) {
            cerr
                                    << "read(" << int(XFERSIZE) << ")" << endl;
            W_COERCE(e);
        }
    }

    return RCOK;
}

/*********************************************************************
 *
 *  w_rc_t partition_t::read_logrec(logrec_t *&rp, lsn_t &ll, int fd)
 *
 *  Read a single log record
 *
 *  Same as partition_t::read
 *  Used when there is no locality in a segment (for hints)
 *
 *********************************************************************/
w_rc_t
partition_t::read_logrec(char* readbuf, logrec_t *&rp, lsn_t &ll, int fd)
{

//     // not aligned

//     if(fd == invalid_fhdl) fd = fhdl_rd();

// #if W_DEBUG_LEVEL > 2
//     w_assert3(fd);
//     if(exists()) {
//         if(fd) w_assert3(is_open_for_read());
//         w_assert3(num() == ll.hi());
//     }
// #endif

//     fileoff_t pos = ll.lo();

//     DBGOUT3(
//         << " read_logrec: lsn "
//         << ll
//     );


//     // w_rc_t e = me()->pread(fd, rp, sizeof(logrec_t), start() + pos);

//     // if (e.is_error()) {
//     //     /* accept the short I/O error for now */
//     //     // ignore short I/O error
//     //     if(e.err_num() != stSHORTIO) {
//     //         smlevel_0::errlog->clog << fatal_prio
//     //                                 << "read(" << int(XFERSIZE) << ")" << endl;
//     //         W_COERCE(e);
//     //     }
//     // }


//     /*
//      * read & inspect header size and see
//      * and see if there's more to read
//      */
//     int b = 0;
//     bool first_time = true;

//     fileoff_t leftover = 0;

//     while (first_time || leftover > 0) {

//         //DBG5(<<"leftover=" << int(leftover) << " b=" << b);
//         DBGOUT3(<<"leftover=" << int(leftover) << " b=" << b);


//         w_rc_t e = me()->pread(fd, ((char*)rp)+b, XFERSIZE, start() + pos + b);
//         DBG5(<<"after me()->read() size= " << int(XFERSIZE));


//         if (e.is_error()) {
//             /* accept the short I/O error for now */
//             // ignore short I/O error
//             if(e.err_num() != stSHORTIO) {
//                 smlevel_0::errlog->clog << fatal_prio
//                                         << "read(" << int(XFERSIZE) << ")" << endl;
//                 W_COERCE(e);
//             }
//         }

//         b += XFERSIZE;

//         //
//         // This could be written more simply from
//         // a logical standpoint, but using this
//         // first_time makes it a wee bit more readable
//         //
//         if (first_time) {
//             if( rp->length() > sizeof(logrec_t) ||
//             rp->length() < rp->header_size() ) {
//                 w_assert1(ll.hi() == 0); // in peek()
//                 return RC(eEOF);
//             }
//             first_time = false;
//             DBGOUT3(<<"length" << rp->length());

//             leftover = (int)rp->length() - (b);
//             //DBG5(<<" leftover now=" << leftover);
//             DBGOUT3(<<" leftover now=" << leftover);
//         } else {
//             leftover -= XFERSIZE;
//             w_assert3(leftover == (int)rp->length() - (b));
//             //DBG5(<<" leftover now=" << leftover);
//             DBGOUT3(<<" leftover now=" << leftover);

//         }
//     }
//     DBG5( << "readbuf@ " << W_ADDR(readbuf)
//         << " first 4 chars are: "
//         << (int)(*((char *)readbuf))
//         << (int)(*((char *)readbuf+1))
//         << (int)(*((char *)readbuf+2))
//         << (int)(*((char *)readbuf+3))
//     );
//     w_assert1(rp != NULL);

//     return RCOK;


    // aligned
    // copied from the  existing partition_t::read

// MUTEX: partition

    INC_TSTAT(log_fetches);

    if(fd == invalid_fhdl) fd = fhdl_rd();

#if W_DEBUG_LEVEL > 2
    w_assert3(fd);
    if(exists()) {
        if(fd) w_assert3(is_open_for_read());
        w_assert3(num() == ll.hi());
    }
#endif

    fileoff_t pos = ll.lo();
    fileoff_t lower = pos / XFERSIZE;

    lower *= XFERSIZE;
    fileoff_t off = pos - lower;

    DBG5(<<"seek to lsn " << ll
        << " index=" << _index << " fd=" << fd
        << " pos=" << pos
        //<< " lower=" << lower  << " + " << start()
        << " fd=" << fd
    );

    /*
     * read & inspect header size and see
     * and see if there's more to read
     */
    int b = 0;
    bool first_time = true;

    rp = (logrec_t *)(readbuf + off);

    DBG5(<< "off= " << ((int)off)
        << "readbuf@ " << W_ADDR(readbuf)
        << " rp@ " << W_ADDR(rp)
    );
    fileoff_t leftover = 0;

    while (first_time || leftover > 0) {

        DBG5(<<"leftover=" << int(leftover) << " b=" << b);
        w_rc_t e = me()->pread(fd, (void *)(readbuf + b), XFERSIZE, lower + b);
        DBG5(<<"after me()->read() size= " << int(XFERSIZE));


        if (e.is_error()) {
                /* accept the short I/O error for now */
            cerr
                        << "read(" << int(XFERSIZE) << ")" << endl;
                W_COERCE(e);
        }
        b += XFERSIZE;

        //
        // This could be written more simply from
        // a logical standpoint, but using this
        // first_time makes it a wee bit more readable
        //
        if (first_time) {
            if( rp->length() > sizeof(logrec_t) ||
            rp->length() < rp->header_size() ) {
                w_assert1(ll.hi() == 0); // in peek()
                return RC(eEOF);
            }
            first_time = false;
            leftover = rp->length() - (b - off);
            DBG5(<<" leftover now=" << leftover);
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
