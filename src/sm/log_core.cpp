/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
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

 $Id: log_core.cpp,v 1.20 2010/12/08 17:37:42 nhall Exp $

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
#define LOG_CORE_C

#include <cstdio>        /* XXX for log recovery */
#include <sys/types.h>
#include <sys/stat.h>
#include <os_interface.h>
#include <largefile_aware.h>

#include "sm_int_1.h"
#include "logtype_gen.h"
#include "log.h"
#include "logrec.h"
#include "log_core.h"
#include "log_carray.h"

// chkpt.h needed to kick checkpoint thread
#include "chkpt.h"
#include "bf_tree.h"

#include "fixable_page_h.h"

#include <sstream>
#include <w_strstream.h>

// needed for skip_log
#include "logdef_gen.cpp"


// LOG_BUFFER switch
#include "logbuf_common.h"


bool       log_core::_initialized = false;

// Once the log is created, this points to it. This is the
// implementation of log_m.
log_core *log_core::THE_LOG(NULL); // me

long
log_core::partition_size(long psize) {
     long p = psize - BLOCK_SIZE;
     return _floor(p, SEGMENT_SIZE) + BLOCK_SIZE;
}

long
log_core::min_partition_size() {
     return _floor(SEGMENT_SIZE, SEGMENT_SIZE) + BLOCK_SIZE;
}
/*********************************************************************
 *
 *  log_core *log_core::new_log_m(logdir,segid,reformat,carray_active_slot_count)
 *
 *  CONSTRUCTOR.  Returns one or the other log types.
 *
 *********************************************************************/

w_rc_t
log_core::new_log_m(
    log_m        *&log_p,
    int          wrbufsize,
    bool         reformat,
    int          carray_active_slot_count
#ifdef LOG_BUFFER
                 ,
                 int logbuf_seg_count,
                 int logbuf_flush_trigger,
                 int logbuf_block_size
#endif
)
{
    rc_t        rc;

    if(THE_LOG != NULL) {
        // not mt-safe
        smlevel_0::errlog->clog << error_prio  << "Log already created. "
                << endl << flushl;
        return RC(eINTERNAL); // server programming error
    }

    if(rc.is_error()) {
        // not mt-safe, but this is not going to happen in concurrency scenario
        smlevel_0::errlog->clog << error_prio
                << "Error: cannot open the log file(s) " << dir_name()
                << ":" << endl << rc << flushl;
        return rc;
    }

    /* The log created here is deleted by the ss_m. */
    log_core *l = 0;
    {
        DBGTHRD(<<" log is unix file" );
        if (max_logsz == 0)  {
            // not mt-safe, but this is not going to happen in
            // concurrency scenario
            smlevel_0::errlog->clog << fatal_prio
                << "Error: log size must be non-zero for log devices"
                << flushl;
            /* XXX should genertae invalid log size of something instead? */
            return RC(eOUTOFLOGSPACE);
        }

        l = new log_core(wrbufsize, reformat, carray_active_slot_count
#ifdef LOG_BUFFER
                                  ,
                                  logbuf_seg_count,
                                  logbuf_flush_trigger,
                                  logbuf_block_size
#endif
                         );
    }
    if (rc.is_error())
        return rc;

    log_p = l;
    THE_LOG = l;
    return RCOK;
}

void
log_core::_acquire()
{
    _partition_lock.acquire(&me()->get_log_me_node());
}
void
log_core::release()
{
    _partition_lock.release(me()->get_log_me_node());
}


partition_index_t
log_core::_get_index(uint32_t n) const
{
    const partition_t        *p;
    for(int i=0; i<PARTITION_COUNT; i++) {
        p = _partition(i);
        if(p->num()==n) return i;
    }
    return -1;
}

partition_t *
log_core::_n_partition(partition_number_t n) const
{
    partition_index_t i = _get_index(n);
    return (i<0)? (partition_t *)0 : _partition(i);
}


partition_t *
log_core::curr_partition() const
{
    w_assert3(partition_index() >= 0);
    return _partition(partition_index());
}

/*********************************************************************
 *
 *  log_core::scavenge(min_rec_lsn, min_xct_lsn)
 *
 *  Scavenge (free, reclaim) unused log files.
 *  We can scavenge all log files with index less
 *  than the minimum of the three lsns:
 *  the two arguments
 *  min_rec_lsn,  : minimum recovery lsn computed by checkpoint
 *  min_xct_lsn,  : first log record written by any uncommitted xct
 *  and
 *  global_min_lsn: the smaller of :
 *     min chkpt rec lsn: min_rec_lsn computed by the last checkpoint
 *     master_lsn: lsn of the last completed checkpoint-begin
 * (so the min chkpt rec lsn is in here twice - that's ok)
 *
 *********************************************************************/
rc_t
log_core::scavenge(const lsn_t &min_rec_lsn, const lsn_t& min_xct_lsn)
{
    FUNC(log_core::scavenge);
    CRITICAL_SECTION(cs, _partition_lock);
    DO_PTHREAD(pthread_mutex_lock(&_scavenge_lock));

#if W_DEBUG_LEVEL > 2
    _sanity_check();
#endif
    partition_t        *p;

    lsn_t lsn = global_min_lsn(min_rec_lsn,min_xct_lsn);
    partition_number_t min_num;
    {
        /*
         *  find min_num -- the lowest of all the partitions
         */
        min_num = partition_num();
        for (uint i = 0; i < PARTITION_COUNT; i++)  {
            p = _partition(i);
            if( p->num() > 0 &&  p->num() < min_num )
                min_num = p->num();
        }
    }

    DBGTHRD( << "scavenge until lsn " << lsn << ", min_num is "
         << min_num << endl );

    /*
     *  recycle all partitions  whose num is less than
     *  lsn.hi().
     */
    int count=0;
    for ( ; min_num < lsn.hi(); ++min_num)  {
        p = _n_partition(min_num);
        w_assert3(p);
        if (durable_lsn() < p->first_lsn() )  {
            W_FATAL(fcINTERNAL); // why would this ever happen?
            //            set_durable(first_lsn(p->num() + 1));
        }
        w_assert3(durable_lsn() >= p->first_lsn());
        DBGTHRD( << "scavenging log " << p->num() << endl );
        count++;
        p->close(true);
        p->destroy();
    }
    if(count > 0) {
        /* LOG_RESERVATIONS

           reinstate the log space from the reclaimed partitions. We
           can put back the entire partition size because every log
           insert which finishes off a partition will consume whatever
           unused space was left at the end.

           Skim off the top of the released space whatever it takes to
           top up the log checkpoint reservation.
         */
        fileoff_t reclaimed = recoverable_space(count);
        fileoff_t max_chkpt = max_chkpt_size();
        while(!verify_chkpt_reservation() && reclaimed > 0) {
            long skimmed = std::min(max_chkpt, reclaimed);
            lintel::unsafe::atomic_fetch_add(const_cast<int64_t*>(&_space_rsvd_for_chkpt), skimmed);
            reclaimed -= skimmed;
        }
        release_space(reclaimed);
        DO_PTHREAD(pthread_cond_signal(&_scavenge_cond));
    }
    DO_PTHREAD(pthread_mutex_unlock(&_scavenge_lock));

    return RCOK;
}

#ifdef LOG_BUFFER
// this is just the first portion of the original _flushX
// called from logbuf_core::_flushX
// It's hacky, but if we want to merge this piece of code into logbuf_core::_flushX(), we have to
// move a lot of variables/functions from log_core to logbuf_core,
// all of which are not that essential for the log buffer logic.
partition_t *
log_core::_flushX_get_partition(lsn_t start_lsn,
        long start1, long end1, long start2, long end2)
{
    w_assert1(end1 >= start1);
    w_assert1(end2 >= start2);
    // time to open a new partition? (used to be in log_core::insert,
    // now called by log flush daemon)
    // This will open a new file when the given start_lsn has a
    // different file() portion from the current partition()'s
    // partition number, so the start_lsn is the clue.
    partition_t* p = curr_partition();
    if(start_lsn.file() != p->num()) {
        partition_number_t n = p->num();
        w_assert3(start_lsn.file() == n+1);
        w_assert3(n != 0);

        {
            /* FRJ: before starting into the CS below we have to be
               sure an empty partition waits for us (otherwise we
               deadlock because partition scavenging is protected by
               the _partition_lock as well).
             */
            DO_PTHREAD(pthread_mutex_lock(&_scavenge_lock));
        retry:
            // need predicates, lest we be in shutdown()
            if(bf) bf->wakeup_cleaners();
            DBGOUT3(<< "chkpt 1");
            if(smlevel_1::chkpt != NULL) smlevel_1::chkpt->wakeup_and_take();
            u_int oldest = global_min_lsn().hi();
            if(oldest + PARTITION_COUNT == start_lsn.file()) {
                fprintf(stderr,
                "Cannot open partition %d until partition %d is reclaimed\n",
                    start_lsn.file(), oldest);
                fprintf(stderr,
                "Waiting for reclamation.\n");
                DO_PTHREAD(pthread_cond_wait(&_scavenge_cond, &_scavenge_lock));
                goto retry;
            }
            DO_PTHREAD(pthread_mutex_unlock(&_scavenge_lock));

            // grab the lock -- we're about to mess with partitions
            CRITICAL_SECTION(cs, _partition_lock);
            p->close();
            unset_current();
            DBG(<<" about to open " << n+1);
            //                                  end_hint, existing, recovery
            p = _open_partition_for_append(n+1, lsn_t::null, false, false);
        }

        // it's a new partition -- size is now 0
        w_assert3(curr_partition()->size()== 0);
        w_assert3(partition_num() != 0);
    }

    return p;
}
#else
/*********************************************************************
 *
 *  log_core::_flush(start_lsn, start1, end1, start2, end2)
 *  @param[in] start_lsn    starting lsn: tells us destination file
 *  @param[in] start1
 *  @param[in] end1
 *  @param[in] start2
 *  @param[in] end2
 *
 *  helper for flush_daemon_work
 *
 *
 *********************************************************************/
void
log_core::_flushX(lsn_t start_lsn,
        long start1, long end1, long start2, long end2)
{
    w_assert1(end1 >= start1);
    w_assert1(end2 >= start2);
    // time to open a new partition? (used to be in log_core::insert,
    // now called by log flush daemon)
    // This will open a new file when the given start_lsn has a
    // different file() portion from the current partition()'s
    // partition number, so the start_lsn is the clue.
    partition_t* p = curr_partition();
    if(start_lsn.file() != p->num()) {
        partition_number_t n = p->num();
        w_assert3(start_lsn.file() == n+1);
        w_assert3(n != 0);

        {
            /* FRJ: before starting into the CS below we have to be
               sure an empty partition waits for us (otherwise we
               deadlock because partition scavenging is protected by
               the _partition_lock as well).
             */
            DO_PTHREAD(pthread_mutex_lock(&_scavenge_lock));
        retry:
            // need predicates, lest we be in shutdown()
            if(bf) bf->wakeup_cleaners();
            if(smlevel_1::chkpt != NULL) smlevel_1::chkpt->wakeup_and_take();
            u_int oldest = global_min_lsn().hi();
            if(oldest + PARTITION_COUNT == start_lsn.file()) {
                fprintf(stderr,
                "Cannot open partition %d until partition %d is reclaimed\n",
                    start_lsn.file(), oldest);
                fprintf(stderr,
                "Waiting for reclamation.\n");
                DO_PTHREAD(pthread_cond_wait(&_scavenge_cond, &_scavenge_lock));
                goto retry;
            }
            DO_PTHREAD(pthread_mutex_unlock(&_scavenge_lock));

            // grab the lock -- we're about to mess with partitions
            CRITICAL_SECTION(cs, _partition_lock);
            p->close();
            unset_current();
            DBG(<<" about to open " << n+1);
            //                                  end_hint, existing, recovery
            p = _open_partition_for_append(n+1, lsn_t::null, false, false);
        }

        // it's a new partition -- size is now 0
        w_assert3(curr_partition()->size()== 0);
        w_assert3(partition_num() != 0);
    }

    // Flush the log buffer
    p->flush(p->fhdl_app(), start_lsn, _buf, start1, end1, start2, end2);
    long written = (end2 - start2) + (end1 - start1);
    p->set_size(start_lsn.lo()+written);

#if W_DEBUG_LEVEL > 2
    _sanity_check();
#endif
}
#endif // LOG_BUFFER

// See that the log buffer contains whatever partial log record
// might have been written to the tail of the file fd.
// Used when recovery finds a not-full partition file.
#ifdef LOG_BUFFER
void
log_core::_prime(int fd, fileoff_t start, lsn_t next)
{
    // initilize the log buffer
    _log_buffer->_prime(fd, start, next);
}
#else
void
log_core::_prime(int fd, fileoff_t start, lsn_t next)
{

    DBGOUT3(<< "_prime @ lsn " << next);

    w_assert1(_durable_lsn == _curr_lsn); // better be startup/recovery!
    long boffset = prime(_buf, fd, start, next);


    _durable_lsn = _flush_lsn = _curr_lsn = next;

    /* FRJ: the new code assumes that the buffer is always aligned
       with some buffer-sized multiple of the partition, so we need to
       return how far into the current segment we are.
     */
    long offset = next.lo() % segsize();
    long base = next.lo() - offset;
    lsn_t start_lsn(next.hi(), base);

    // This should happend only in recovery/startup case.  So let's assert
    // that there is no log daemon running yet. If we ever fire this
    // assert, we'd better see why and it means we might have to protect
    // _cur_epoch and _start/_end with a critical section on _insert_lock.
    w_assert1(_flush_daemon_running == false);
    _buf_epoch = _cur_epoch = epoch(start_lsn, base, offset, offset);
    _end = _start = next.lo();

    // move the primed data where it belongs (watch out, it might overlap)
    memmove(_buf+offset-boffset, _buf, boffset);
}
#endif // LOG_BUFFER

// Prime buf with the partial block ending at 'next';
// return the size of that partial block (possibly 0)
//
// We are about to write a record for a certain lsn(next).
// If we haven't been appending to this file (e.g., it's
// startup), we need to make sure the first part of the buffer
// contains the last partial block in the file, so that when
// we append that block to the file, we aren't clobbering the
// tail of the file (partition).
//
// This reads from the given file descriptor, the necessary
// block to cover the lsn.
//
// The start argument (offset from beginning of file (fd) of
// start of partition) is for support on raw devices; for unix
// files, it's always zero, since the beginning of the partition
// is the beginning of the file (fd).
//
// This method is public to allow calling from partition_t, which
// uses this to prime its own buffer for writing a skip record.
// It is called from the private _prime to prime the segment-sized
// log buffer _buf.
long
log_core::prime(char* buf, int fd, fileoff_t start, lsn_t next)
{
    FUNC(log_core::prime);

    w_assert1(start == 0); // unless we are on a raw device, which is
    // no longer supported for the log.

    fileoff_t b = _floor(next.lo(), BLOCK_SIZE);
    // get the first lsn in the block to which "next" belongs.
    lsn_t first = lsn_t(uint32_t(next.hi()), sm_diskaddr_t(b));

    // if the "next" lsn is in the middle of a block...
    if(first != next) {
        w_assert3(first.lo() < next.lo());
        fileoff_t offset = start + first.lo();

        DBG(<<" reading " << int(BLOCK_SIZE) << " on fd " << fd );
        int n = 0;
        w_rc_t e = me()->pread(fd, buf, BLOCK_SIZE, offset);
        if (e.is_error()) {
            // Not mt-safe, but it's a fatal error anyway
            W_FATAL_MSG(e.err_num(),
                        << "cannot read log: lsn " << first
                        << "pread(): " << e
                        << "pread() returns " << n << endl);
        }
    }
    return next.lo() - first.lo();
}

void
log_core::_sanity_check() const
{
    if(!_initialized) return;

#if W_DEBUG_LEVEL > 1
    partition_index_t   i;
    const partition_t*  p;
    bool                found_current=false;
    bool                found_min_lsn=false;

    // we should not be calling this when
    // we're in any intermediate state, i.e.,
    // while there's no current index

    if( _curr_index >= 0 ) {
        w_assert1(_curr_num > 0);
    } else {
        // initial state: _curr_num == 1
        w_assert1(_curr_num == 1);
    }
    w_assert1(durable_lsn() <= curr_lsn());
    w_assert1(durable_lsn() >= first_lsn(1));

    for(i=0; i<PARTITION_COUNT; i++) {
        p = _partition(i);
        p->sanity_check();

        w_assert1(i ==  p->index());

        // at most one open for append at any time
        if(p->num()>0) {
            w_assert1(p->exists());
            w_assert1(i ==  _get_index(p->num()));
            w_assert1(p ==  _n_partition(p->num()));

            if(p->is_current()) {
                w_assert1(!found_current);
                found_current = true;

                w_assert1(p ==  curr_partition());
                w_assert1(p->num() ==  partition_num());
                w_assert1(i ==  partition_index());

                w_assert1(p->is_open_for_append());
            } else if(p->is_open_for_append()) {
                // FRJ: not always true with concurrent inserts
                //w_assert1(p->flushed());
            }

            // look for global_min_lsn
            if(global_min_lsn().hi() == p->num()) {
                //w_assert1(!found_min_lsn);
                // don't die in case global_min_lsn() is null lsn
                found_min_lsn = true;
            }
        } else {
            w_assert1(!p->is_current());
            w_assert1(!p->exists());
        }
    }
    w_assert1(found_min_lsn || (global_min_lsn()== lsn_t::null));
#endif
}

#ifdef LOG_BUFFER
rc_t
log_core::fetch(lsn_t& ll, logrec_t*& rp, lsn_t* nxt, const bool forward)
{
    FUNC(log_core::fetch);

    return _log_buffer->fetch(ll, rp, nxt, forward);
}

rc_t
log_core::fetch(lsn_t& ll, logrec_t* &rp, lsn_t* nxt, hints_op op)
{
    FUNC(log_core::fetch);

    return _log_buffer->fetch(ll, rp, nxt, op);
}
#else
/*********************************************************************
 *
 *  log_core::fetch(lsn, rec, nxt, forward)
 *
 *  used in rollback and log_i
 *
 *  Fetch a record at lsn, and return it in rec. Optionally, return
 *  the lsn of the next/previous record in nxt.  The lsn parameter also returns
 *  the lsn of the log record actually fetched.  This is necessary
 *  since it is possible while scanning to specify an lsn
 *  that points to the end of a log file and therefore is actually
 *  the first log record in the next file.
 *
 * NOTE: caller must call release()
 *********************************************************************/

rc_t
log_core::fetch(lsn_t& ll, logrec_t*& rp, lsn_t* nxt, const bool forward)
{
    FUNC(log_core::fetch);

    DBGTHRD(<<"fetching lsn " << ll
        << " , _curr_lsn = " << curr_lsn()
        << " , _durable_lsn = " << durable_lsn());

#if W_DEBUG_LEVEL > 0
    _sanity_check();
#endif

    // it's not sufficient to flush to ll, since
    // ll is at the *beginning* of what we want
    // to read...
    lsn_t must_be_durable = ll + sizeof(logrec_t);
    if(must_be_durable > _durable_lsn) {
        W_DO(flush(must_be_durable));
    }

    // protect against double-acquire
    _acquire(); // caller must release the _partition_lock mutex

    /*
     *  Find and open the partition
     */

    partition_t        *p = 0;
    uint32_t        last_hi=0;
    while (!p) {
        if(last_hi == ll.hi()) {
            // can happen on the 2nd or subsequent round
            // but not first
            DBGTHRD(<<"no such partition " << ll  );
            return RC(eEOF);
        }
        if (ll >= curr_lsn())  {
            /*
             *  This would constitute a
             *  read beyond the end of the log
             */
            DBGTHRD(<<"fetch at lsn " << ll  << " returns eof -- _curr_lsn="
                    << curr_lsn());
            return RC(eEOF);
        }
        last_hi = ll.hi();

        DBG(<<" about to open " << ll.hi());
        //                                 part#, end_hint, existing, recovery
        if ((p = _open_partition_for_read(ll.hi(), lsn_t::null, true, false))) {

            // opened one... is it the right one?
            DBGTHRD(<<"opened... p->size()=" << p->size());

            if ( ll.lo() >= p->size() ||
                (p->size() == partition_t::nosize && ll.lo() >= limit()))  {
                DBGTHRD(<<"seeking to " << ll.lo() << ";  beyond p->size() ... OR ...");
                DBGTHRD(<<"limit()=" << limit() << " & p->size()=="
                        << int(partition_t::nosize));

                ll = first_lsn(ll.hi() + 1);
                DBGTHRD(<<"getting next partition: " << ll);
                p = 0; continue;
            }
        }
    }

    bool first_record = false;  // True if target record is the first record in a partition

    DBGOUT3(<< "fetch @ lsn: " << ll);
    W_COERCE(p->read(rp, ll));
    {
        logrec_t        &r = *rp;

        if (r.type() == logrec_t::t_skip && r.get_lsn_ck() == ll) {

            // The log record we want to read is at the end of one partition
            // therefore the actual log record is in the next partition
            // Everything is good except if caller is asking for a backward scan
            // then the 'nxt' is in the current partition, not the next partition which
            // we are about to go to

            if ((false == forward) && (nxt))
            {
                // If backward scan, save the 'nxt' before moving to the next partition
                // Note the parameter for 'advance' is a signed int, so we are using
                // negative number to get the lsn from previous log record
                lsn_t tmp = ll;
                int distance = 0 - (int)(r.length());
                *nxt = tmp.advance(distance);

                // The target record is the first record in the next partition
                // we recorded the lsn for 'nxt' before we move to the next partition
                first_record = true;
            }

            DBGTHRD(<<"seeked to skip" << ll );
            DBGTHRD(<<"getting next partition.");
            ll = first_lsn(ll.hi() + 1);
            // FRJ: BUG? Why are we so certain this partition is even
            // open, let alone open for read?
            p = _n_partition(ll.hi());
            if(!p)
                p = _open_partition_for_read(ll.hi(), lsn_t::null, false, false);

            // re-read
            DBGOUT3(<< "fetch @ lsn: " << ll);
            W_COERCE(p->read(rp, ll));
        }
    }
    logrec_t        &r = *rp;

    if (r.lsn_ck().hi() != ll.hi()) {
        W_FATAL_MSG(fcINTERNAL,
            << "Fatal error: log record " << ll
            << " is corrupt in lsn_ck().hi() "
            << r.get_lsn_ck()
            << endl);
    } else if (r.lsn_ck().lo() != ll.lo()) {
        W_FATAL_MSG(fcINTERNAL,
            << "Fatal error: log record " << ll
            << "is corrupt in lsn_ck().lo()"
            << r.get_lsn_ck()
            << endl);
    }

    if ((nxt) && (false == first_record))
    {
        // Get the lsn for next/previous log record
        // If backward scan, the target record might be the first one in the partition
        // so the previous record would be in a different partition
        // we don't need to worry about this special case because:
        // The logic would go to the previous partition first, realized the actual log record
        // is the first record of the next partition, record the 'nxt' and then move
        // to the next partition, so the 'nxt' has been taken care of if the target is the first
        // record in a partition (true == first_record)

        lsn_t tmp = ll;
        int distance;
        if (true == forward)
            distance = (int)(r.length());
        else
            distance = 0 - (int)(r.length());
        *nxt = tmp.advance(distance);
    }

    DBGTHRD(<<"fetch at lsn " << ll  << " returns " << r);
#if W_DEBUG_LEVEL > 2
    _sanity_check();
#endif

    // caller must release the _partition_lock mutex
    return RCOK;
}

#endif

/*********************************************************************
 *
 *  log_core::close_min(n)
 *
 *  Close the partition with the smallest index(num) or an unused
 *  partition, and
 *  return a ptr to the partition
 *
 *  The argument n is the partition number for which we are going
 *  to use the free partition.
 *
 *********************************************************************/
// MUTEX: partition
partition_t        *
log_core::_close_min(partition_number_t n)
{
    // kick the cleaner thread(s)
    if(bf) bf->wakeup_cleaners();

    FUNC(log_core::close_min);

    /*
     *  If a free partition exists, return it.
     */

    /*
     * first try the slot that is n % PARTITION_COUNT
     * That one should be free.
     */
    int tries=0;
 again:
    partition_index_t    i =  (int)((n-1) % PARTITION_COUNT);
    partition_number_t   min = min_chkpt_rec_lsn().hi();
    partition_t         *victim;

    victim = _partition(i);
    if((victim->num() == 0)  ||
        (victim->num() < min)) {
        // found one -- doesn't matter if it's the "lowest"
        // but it should be
    } else {
        victim = 0;
    }

    if (victim)  {
        w_assert3( victim->index() == (partition_index_t)((n-1) % PARTITION_COUNT));
    }
    /*
     *  victim is the chosen victim partition.
     */
    if(!victim) {
        /*
         * uh-oh, no space left. Kick the page cleaners, wait a bit, and
         * try again. Do this no more than 8 times.
         *
         */
        {
            w_ostrstream msg;
            msg << error_prio
            << "Thread " << me()->id << " "
            << "Out of log space  ("
            << space_left()
            << "); No empty partitions."
            << endl;
            fprintf(stderr, "%s\n", msg.c_str());
        }

        if(tries++ > 8) W_FATAL(eOUTOFLOGSPACE);
        if(bf) bf->wakeup_cleaners();
        me()->sleep(1000);
        goto again;
    }
    w_assert1(victim);
    // num could be 0

    /*
     *  Close it.
     */
    if(victim->exists()) {
        /*
         * Cannot close it if we need it for recovery.
         */
        if(victim->num() >= min_chkpt_rec_lsn().hi()) {
            w_ostrstream msg;
            msg << " Cannot close min partition -- still in use!" << endl;
            // not mt-safe
            smlevel_0::errlog->clog << error_prio  << msg.c_str() << flushl;
        }
        w_assert1(victim->num() < min_chkpt_rec_lsn().hi());

        victim->close(true);
        victim->destroy();

    } else {
        w_assert3(! victim->is_open_for_append());
        w_assert3(! victim->is_open_for_read());
    }
    w_assert1(! victim->is_current() );

    victim->clear();

    return victim;
}

/*********************************************************************
 *
 *  log_core::_open_partition_for_append() calls _open_partition with
 *                            forappend=true)
 *  log_core::_open_partition_for_read() calls _open_partition with
 *                            forappend=false)
 *
 *  log_core::_open_partition(num, end_hint, existing,
 *                           forappend, during_recovery)
 *
 *  This partition structure is free and usable.
 *  Open it as partition num.
 *
 *  if existing==true, the partition "num" had better already exist,
 *  else it had better not already exist.
 *
 *  if forappend==true, making this the new current partition.
 *    and open it for appending was well as for reading
 *
 *  if during_recovery==true, make sure the entire partition is
 *   checked and its size is recorded accurately.
 *
 *  end_hint is used iff during_recovery is true.
 *
 *********************************************************************/

// MUTEX: partition
partition_t        *
log_core::_open_partition(partition_number_t  __num,
        const lsn_t&  end_hint,
        bool existing,
        bool forappend,
        bool during_recovery
)
{
    w_assert3(__num > 0);

#if W_DEBUG_LEVEL > 2
    // sanity checks for arguments:
    {
        // bool case1 = (existing  && forappend && during_recovery);
        bool case2 = (existing  && forappend && !during_recovery);
        // bool case3 = (existing  && !forappend && during_recovery);
        // bool case4 = (existing  && !forappend && !during_recovery);
        // bool case5 = (!existing  && forappend && during_recovery);
        // bool case6 = (!existing  && forappend && !during_recovery);
        bool case7 = (!existing  && !forappend && during_recovery);
        bool case8 = (!existing  && !forappend && !during_recovery);

        w_assert3( ! case2);
        w_assert3( ! case7);
        w_assert3( ! case8);
    }

#endif

    // see if one's already opened with the given __num
    partition_t *p = _n_partition(__num);

#if W_DEBUG_LEVEL > 2
    if(forappend) {
        w_assert3(partition_index() == -1);
        // there should now be *no open partition*
        partition_t *c;
        int i;
        for (i = 0; i < PARTITION_COUNT; i++)  {
            c = _partition(i);
            w_assert3(! c->is_current());
        }
    }
#endif

    if(!p) {
        /*
         * find an empty partition to use
         */
        DBG(<<"find a new partition structure  to use " );
        p = _close_min(__num);
        w_assert1(p);
        p->peek(__num, end_hint, during_recovery);
    }


    if(existing && !forappend) {
        DBG(<<"about to open for read");
        w_rc_t err = p->open_for_read(__num);
        if(err.is_error()) {
            // Try callback to recover this file
            if(smlevel_0::log_archived_callback) {
                static char buf[max_devname];
                make_log_name(__num, buf, max_devname);
                err = (*smlevel_0::log_archived_callback)(
                        buf,
                        __num
                        );
                if(!err.is_error()) {
                    // Try again, just once.
                    err = p->open_for_read(__num);
                }
            }
        }
        if(err.is_error()) {
            fprintf(stderr,
                    "Could not open partition %d for reading.\n",
                    __num);
            W_FATAL(eINTERNAL);
        }


        w_assert3(p->is_open_for_read());
        w_assert3(p->num() == __num);
        w_assert3(p->exists());
    }


    if(forappend) {
        /*
         *  This becomes the current partition.
         */
        p->open_for_append(__num, end_hint);
        if(during_recovery) {
          // We will eventually want to write a record with the durable
          // lsn.  But if this is start-up and we've initialized
          // with a partial partition, we have to prime the
          // buf with the last block in the partition.
          w_assert1(durable_lsn() == curr_lsn());
          _prime(p->fhdl_app(), p->start(), durable_lsn());
        }
        w_assert3(p->exists());
        w_assert3(p->is_open_for_append());

        // The idea here is to checkpoint at the beginning of every
        // new partition because it seems we aren't taking enough
        // checkpoints; then we were making the user threads do an emergency
        // checkpoint to scavenge log space.  Short-tx workloads should never
        // encounter this.    Don't do this if shutting down or starting
        // up because in those 2 cases, the chkpt_m might not exist yet/anymore
        DBGOUT3(<< "chkpt 2");
        if(smlevel_1::chkpt != NULL) smlevel_1::chkpt->wakeup_and_take();
    }
    return p;
}

void
log_core::unset_current()
{
    _curr_index = -1;
    _curr_num = 0;
}

void
log_core::set_current(
        partition_index_t i,
        partition_number_t num
)
{
    w_assert3(_curr_index == -1);
    w_assert3(_curr_num  == 0 || _curr_num == 1);
    _curr_index = i;
    _curr_num = num;
}

#ifdef LOG_BUFFER
void log_core::start_flush_daemon()
{
    _log_buffer->start_flush_daemon();
}

void log_core::shutdown()
{
    _log_buffer->shutdown();
}
#else
class flush_daemon_thread_t : public smthread_t {
    log_core* _log;
public:
    flush_daemon_thread_t(log_core* log) :
         smthread_t(t_regular, "flush_daemon", WAIT_NOT_USED), _log(log) { }

    virtual void run() { _log->flush_daemon(); }
};

// Does not get called until after the
// log is fully constructed:
void log_core::start_flush_daemon()
{
    _flush_daemon_running = true;
    _flush_daemon->fork();
}

void
log_core::shutdown()
{
    // gnats 52:  RACE: We set _shutting_down and signal between the time
    // the daemon checks _shutting_down (false) and waits.
    //
    // So we need to notice if the daemon got the message.
    // It tells us it did by resetting the flag after noticing
    // that the flag is true.
    // There should be no interference with these two settings
    // of the flag since they happen strictly in that sequence.
    //
    _shutting_down = true;
    while (*&_shutting_down) {
        CRITICAL_SECTION(cs, _wait_flush_lock);
        // The only thread that should be waiting
        // on the _flush_cond is the log flush daemon.
        // Yet somehow we wedge here.
        DO_PTHREAD(pthread_cond_broadcast(&_flush_cond));
    }
    _flush_daemon->join();
    _flush_daemon_running = false;
    delete _flush_daemon;
    _flush_daemon=NULL;
}
#endif // LOG_BUFFER

/*********************************************************************
 *
 *  log_core::log_core(bufsize, reformat)
 *
 *  Hidden constructor.
 *  Create log flush daemon thread.
 *
 *  Open and scan logdir for master lsn and last log file.
 *  Truncate last incomplete log record (if there is any)
 *  from the last log file.
 *
 *********************************************************************/

#ifdef LOG_BUFFER
NORET
log_core::log_core(
                   long bsize, // segment size for the log buffer, set through "sm_logbufsize"
                   bool reformat,
                   int carray_active_slot_count,
                   int logbuf_seg_count,
                   int logbuf_flush_trigger,
                   int logbuf_block_size
                   )

    :
      _reservations_active(false),
      _segsize(_ceil(bsize, SEGMENT_SIZE)), // actual segment size for the log buffer,
      _curr_index(-1),
      _curr_num(1),
      _readbuf(NULL),
#ifdef LOG_DIRECT_IO
      _writebuf(NULL),
#endif
      _skip_log(NULL)
{
    FUNC(log_core::log_core);

    DO_PTHREAD(pthread_mutex_init(&_scavenge_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_scavenge_cond, NULL));

    // create the log buffer
    // _log_buffer->_partition_data_size is not set at this moment
    _log_buffer = new logbuf_core(logbuf_seg_count, logbuf_flush_trigger, logbuf_block_size, _segsize,
                              0, carray_active_slot_count);
    _log_buffer->logbuf_set_owner(this);


    // NOTE: GROT must make this a function of page size, and of xfer size,
    // since xfer size is fixed (8K).
    // It has to big enough to read the maximum-sized log record, clearly
    // more than a page.
#ifdef LOG_DIRECT_IO
#if SM_PAGESIZE < 8192
    posix_memalign((void**)&_readbuf, LOG_DIO_ALIGN, BLOCK_SIZE*4);
    //_readbuf = new char[BLOCK_SIZE*4];
    posix_memalign((void**)&_writebuf, LOG_DIO_ALIGN, BLOCK_SIZE*2);
#else
    posix_memalign((void**)&_readbuf, LOG_DIO_ALIGN, SM_PAGESIZE*4);
    //_readbuf = new char[SM_PAGESIZE*4];
    // we need two blocks for the write buffer because the skip log record may span two blocks
    posix_memalign((void**)&_writebuf, LOG_DIO_ALIGN, SM_PAGESIZE*2);
#endif
#else
#if SM_PAGESIZE < 8192
    _readbuf = new char[BLOCK_SIZE*4];
#else
    _readbuf = new char[SM_PAGESIZE*4];
#endif
#endif // LOG_DIRECT_IO

    _skip_log = new skip_log;

    w_assert1(is_aligned(_readbuf));

    // this function calculates _partition_data_size
    // the total log size, max_logsz, is set through this option "sm_logsize"
    // the default value of sm_logsize is increased from the original 128KB to 128MB
    W_COERCE(_set_size(max_logsz));

    // the log buffer (the epochs) is designed to hold log records from at most two partitions
    // so its capacity cannot exceed the partition size
    // otherwise, there could be log records from three parttitions in the buffer
    if(LOGBUF_SEG_COUNT*_segsize > _partition_data_size) {
        errlog->clog << error_prio
                     << "Log buf seg count too big or total log size (sm_logsize) too small: "
                     << "LOGBUF_SEG_COUNT " <<  LOGBUF_SEG_COUNT
                     << "_segsize " << _segsize
                     << "_partition_data_size " << _partition_data_size
                     << "max_logsz" << max_logsz
                     << endl;
        errlog->clog << error_prio << endl;
        fprintf(stderr, "Log buf seg count too big or total log size (sm_logsize) too small ");
        W_FATAL(eINTERNAL);
    }

    // set partition_data_size
    _log_buffer->set_partition_data_size(_partition_data_size);

    DBGOUT3(<< "SEG SIZE " << _segsize << " PARTITION DATA SIZE " << _partition_data_size);


    // FRJ: we don't actually *need* this (no trx around yet), but we
    // don't want to trip the assertions that watch for it.
    CRITICAL_SECTION(cs, _partition_lock);

    partition_number_t  last_partition = partition_num();
    bool                last_partition_exists = false;
    /*
     * make sure there's room for the log names
     */
    fileoff_t eof= fileoff_t(0);

    os_dirent_t *dd=0;
    os_dir_t ldir = os_opendir(dir_name());
    if (! ldir)
    {
        w_rc_t e = RC(eOS);
        smlevel_0::errlog->clog << fatal_prio
            << "Error: could not open the log directory " << dir_name() <<flushl;
        fprintf(stderr, "Error: could not open the log directory %s\n",
                    dir_name());

        smlevel_0::errlog->clog << fatal_prio
            << "\tNote: the log directory is specified using\n"
            "\t      the sm_logdir option." << flushl;

        smlevel_0::errlog->clog << flushl;

        W_COERCE(e);
    }
    DBGTHRD(<<"opendir " << dir_name() << " succeeded");

    /*
     *  scan directory for master lsn and last log file
     */

    _master_lsn = lsn_t::null;

    uint32_t min_index = max_uint4;

    char *fname = new char [smlevel_0::max_devname];
    if (!fname)
        W_FATAL(fcOUTOFMEMORY);
    w_auto_delete_array_t<char> ad_fname(fname);

    /* Create a list of lsns for the partitions - this
     * will be used to store any hints about the last
     * lsns of the partitions (stored with checkpoint meta-info
     */
    lsn_t lsnlist[PARTITION_COUNT];
    int   listlength=0;
    {
        /*
         *  initialize partition table
         */
        partition_index_t i;
        for (i = 0; i < PARTITION_COUNT; i++)  {
            _part[i].init_index(i);
            _part[i].init(this);
        }
    }

    DBGTHRD(<<"reformat= " << reformat
            << " last_partition "  << last_partition
            << " last_partition_exists "  << last_partition_exists
            );
    if (reformat)
    {
        smlevel_0::errlog->clog << emerg_prio
            << "Reformatting logs..." << endl;

        while ((dd = os_readdir(ldir)))
        {
            DBGTHRD(<<"master_prefix= " << master_prefix());

            unsigned int namelen = strlen(log_prefix());
            namelen = namelen > strlen(master_prefix())? namelen :
                                        strlen(master_prefix());

            const char *d = dd->d_name;
            unsigned int orig_namelen = strlen(d);
            namelen = namelen > orig_namelen ? namelen : orig_namelen;

            char *name = new char [namelen+1];
            w_auto_delete_array_t<char>  cleanup(name);

            memset(name, '\0', namelen+1);
            strncpy(name, d, orig_namelen);
            DBGTHRD(<<"name= " << name);

            bool parse_ok = (strncmp(name,master_prefix(),strlen(master_prefix()))==0);
            if(!parse_ok) {
                parse_ok = (strncmp(name,log_prefix(),strlen(log_prefix()))==0);
            }
            if(parse_ok) {
                smlevel_0::errlog->clog << debug_prio
                    << "\t" << name << "..." << endl;

                {
                    w_ostrstream s(fname, (int) smlevel_0::max_devname);
                    s << dir_name() << _SLASH << name << ends;
                    w_assert1(s);
                    if( unlink(fname) < 0) {
                        w_rc_t e = RC(fcOS);
                        smlevel_0::errlog->clog << debug_prio
                            << "unlink(" << fname << "):"
                            << endl << e << endl;
                    }
                }
            }
        }

        //  os_closedir(ldir);
        w_assert3(!last_partition_exists);
    }

    DBGOUT5(<<"about to readdir"
            << " last_partition "  << last_partition
            << " last_partition_exists "  << last_partition_exists
            );

    while ((dd = os_readdir(ldir)))
    {
        DBGOUT5(<<"dd->d_name=" << dd->d_name);

        // XXX should abort on name too long earlier, or size buffer to fit
        const unsigned int prefix_len = strlen(master_prefix());
        w_assert3(prefix_len < smlevel_0::max_devname);

        char *buf = new char[smlevel_0::max_devname+1];
        if (!buf)
                W_FATAL(fcOUTOFMEMORY);
        w_auto_delete_array_t<char>  ad_buf(buf);

        unsigned int         namelen = prefix_len;
        const char *         dn = dd->d_name;
        unsigned int         orig_namelen = strlen(dn);

        namelen = namelen > orig_namelen ? namelen : orig_namelen;
        char *                name = new char [namelen+1];
        w_auto_delete_array_t<char>  cleanup(name);

        memset(name, '\0', namelen+1);
        strncpy(name, dn, orig_namelen);

        strncpy(buf, name, prefix_len);
        buf[prefix_len] = '\0';

        DBGOUT5(<<"name= " << name);

        bool parse_ok = ((strlen(buf)) == prefix_len);

        DBGOUT5(<<"parse_ok  = " << parse_ok
                << " buf = " << buf
                << " prefix_len = " << prefix_len
                << " strlen(buf) = " << strlen(buf));
        if (parse_ok) {
            lsn_t tmp;
            if (strcmp(buf, master_prefix()) == 0)
            {
                DBGOUT5(<<"found log file " << buf);
                /*
                 *  File name matches master prefix.
                 *  Extract master lsn & lsns of skip-records
                 */
                lsn_t tmp1;
                bool old_style=false;
                rc_t rc = _read_master(name, prefix_len,
                        tmp, tmp1, lsnlist, listlength,
                        old_style);
                W_COERCE(rc);

                if (tmp < master_lsn())  {
                    /*
                     *  Swap tmp <-> _master_lsn, tmp1 <-> _min_chkpt_rec_lsn
                     */
                    std::swap(_master_lsn, tmp);
                    std::swap(_min_chkpt_rec_lsn, tmp1);
                }
                /*
                 *  Remove the older master record.
                 */
                if (_master_lsn != lsn_t::null) {
                    _make_master_name(_master_lsn,
                                      _min_chkpt_rec_lsn,
                                      fname,
                                      smlevel_0::max_devname);
                    (void) unlink(fname);
                }
                /*
                 *  Save the new master record
                 */
                _master_lsn = tmp;
                _min_chkpt_rec_lsn = tmp1;
                DBGOUT5(<<" _master_lsn=" << _master_lsn
                 <<" _min_chkpt_rec_lsn=" << _min_chkpt_rec_lsn);

                DBGOUT5(<<"parse_ok = " << parse_ok);

            } else if (strcmp(buf, log_prefix()) == 0)  {
                DBGOUT5(<<"found log file " << buf);
                /*
                 *  File name matches log prefix
                 */

                w_istrstream s(name + prefix_len);
                uint32_t curr;
                if (! (s >> curr))  {
                    smlevel_0::errlog->clog << fatal_prio
                    << "bad log file \"" << name << "\"" << flushl;
                    W_FATAL(eINTERNAL);
                }

                DBGOUT5(<<"curr " << curr
                        << " partition_num()==" << partition_num()
                        << " last_partition_exists " << last_partition_exists
                        );

                if (curr >= last_partition) {
                    last_partition = curr;
                    last_partition_exists = true;
                    DBGOUT5(<<"new last_partition " << curr
                        << " exits=true" );
                }
                if (curr < min_index) {
                    min_index = curr;
                }
            } else {
                DBGOUT5(<<"NO MATCH");
                DBGOUT5(<<"_master_prefix= " << master_prefix());
                DBGOUT5(<<"_log_prefix= " << log_prefix());
                DBGOUT5(<<"buf= " << buf);
                parse_ok = false;
            }
        }

        /*
         *  if we couldn't parse the file name and it was not "." or ..
         *  then print an error message
         */
        if (!parse_ok && ! (strcmp(name, ".") == 0 ||
                                strcmp(name, "..") == 0)) {
            smlevel_0::errlog->clog << fatal_prio
                                    << "log_core: cannot parse filename \""
                                    << name << "\".  Maybe a data volume in the logging directory?"
                                    << flushl;
            W_FATAL(fcINTERNAL);
        }
    }
    os_closedir(ldir);

    DBGOUT5(<<"after closedir  "
            << " last_partition "  << last_partition
            << " last_partition_exists "  << last_partition_exists
            );

#if W_DEBUG_LEVEL > 2
    if(reformat) {
        w_assert3(partition_num() == 1);
        w_assert3(_min_chkpt_rec_lsn.hi() == 1);
        w_assert3(_min_chkpt_rec_lsn.lo() == first_lsn(1).lo());
    } else {
       // ??
    }
    w_assert3(partition_index() == -1);
#endif

    DBGOUT5(<<"Last partition is " << last_partition
        << " existing = " << last_partition_exists
     );

    /*
     *  Destroy all partitions less than _min_chkpt_rec_lsn
     *  Open the rest and close them.
     *  There might not be an existing last_partition,
     *  regardless of the value of "reformat"
     */
    {
        partition_number_t n;
        partition_t        *p;

        DBGOUT5(<<" min_chkpt_rec_lsn " << min_chkpt_rec_lsn()
                << " last_partition " << last_partition);
        w_assert3(min_chkpt_rec_lsn().hi() <= last_partition);

        for (n = min_index; n < min_chkpt_rec_lsn().hi(); n++)  {
            // not an error if we can't unlink (probably doesn't exist)
            DBGOUT5(<<" destroy_file " << n << "false");
            destroy_file(n, false);
        }
        for (n = _min_chkpt_rec_lsn.hi(); n <= last_partition; n++)  {
            // Find out if there's a hint about the length of the
            // partition (from the checkpoint).  This lsn serves as a
            // starting point from which to search for the skip_log record
            // in the file.  It's a performance thing...
            lsn_t lasthint;
            for(int q=0; q<listlength; q++) {
                if(lsnlist[q].hi() == n) {
                    lasthint = lsnlist[q];
                }
            }

            // open and check each file (get its size)
            DBGOUT5(<<" open " << n << "true, false, true");

            // last argument indicates "in_recovery" more accurately,
            // we should say "at-startup"
            p = _open_partition_for_read(n, lasthint, true, true);
            w_assert3(p == _n_partition(n));
            p->close();
            unset_current();
            DBGOUT5(<<" done w/ open " << n );
        }
    }

    /* XXXX :  Don't have a static method on
     * partition_t for start()
    */
    /* end of the last valid log record / start of invalid record */
    fileoff_t pos = 0;

    { // Truncate at last complete log rec
    DBGOUT5(<<" truncate last complete log rec ");

    /*
     *
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
     *
     */
    make_log_name(last_partition, fname, smlevel_0::max_devname);
    DBGOUT5(<<" checking " << fname);

    FILE *f =  fopen(fname, "r");
    DBGOUT5(<<" opened " << fname << " fp " << f << " pos " << pos);

    fileoff_t start_pos = pos;

    /* If the master checkpoint is in the current partition, seek
       to its position immediately, instead of scanning from the
       beginning of the log.   If the current partition doesn't have
       a checkpoint, must read entire paritition until the skip
       record is found. */

    const lsn_t &seek_lsn = _master_lsn;

    if (f && seek_lsn.hi() == last_partition) {
            start_pos = seek_lsn.lo();

            DBGOUT5(<<" seeking to start_pos " << start_pos);
            if (fseek(f, start_pos, SEEK_SET)) {
                smlevel_0::errlog->clog  << error_prio
                    << "log read: can't seek to " << start_pos
                     << " starting log scan at origin"
                     << endl;
                start_pos = pos;
            }
            else
                pos = start_pos;
    }
    DBGOUT5(<<" pos is now " << pos);



    if (f)  {
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
                        smlevel_0::errlog->clog << fatal_prio
                        << "ERROR: unexpected log file inconsistency." << flushl;
                        W_COERCE(e);
                    }
                    break;
                }
                DBGOUT5(<<"pos = " <<  pos
                    << " lsn_ck = " <<lsn_ck);

                // make sure log record's lsn matched its position in file
                if ( (lsn_ck.lo() != pos) ||
                    (lsn_ck.hi() != (uint32_t) last_partition ) ) {
                    // found partial log record, end of log is previous record
                    smlevel_0::errlog->clog << error_prio <<
        "Found unexpected end of log -- probably due to a previous crash."
                    << flushl;
                    smlevel_0::errlog->clog << error_prio <<
                    "   Recovery will continue ..." << flushl;
                    break;
                }

                pos = ftell(f) ;
            }
        }
        fclose(f);



        {
            DBGOUT5(<<"explicit truncating " << fname << " to " << pos);
            w_assert0(os_truncate(fname, pos )==0);

            //
            // but we can't just use truncate() --
            // we have to truncate to a size that's a mpl
            // of the page size. First append a skip record
            DBGOUT5(<<"explicit opening  " << fname );
            f =  fopen(fname, "a");
            if (!f) {
                w_rc_t e = RC(fcOS);
                smlevel_0::errlog->clog  << fatal_prio
                    << "fopen(" << fname << "):" << endl << e << endl;
                W_COERCE(e);
            }
            skip_log *s = new skip_log; // deleted below
            s->set_lsn_ck( lsn_t(uint32_t(last_partition), sm_diskaddr_t(pos)) );


            DBGOUT5(<<"writing skip_log at pos " << pos << " with lsn "
                << s->get_lsn_ck()
                << "and size " << s->length()
                );
#ifdef W_TRACE
            {
                fileoff_t eof2 = ftell(f);
                DBGOUT5(<<"eof is now " << eof2);
            }
#endif

            if ( fwrite(s, s->length(), 1, f) != 1)  {
                w_rc_t        e = RC(eOS);
                smlevel_0::errlog->clog << fatal_prio <<
                    "   fwrite: can't write skip rec to log ..." << flushl;
                W_COERCE(e);
            }
#ifdef W_TRACE
            {
                fileoff_t eof2 = ftell(f);
                DBGTHRD(<<"eof is now " << eof2);
            }
#endif
            fileoff_t o = pos;
            o += s->length();
            o = o % BLOCK_SIZE;
            DBGOUT5(<<"BLOCK_SIZE " << int(BLOCK_SIZE));
            if(o > 0) {
                o = BLOCK_SIZE - o;
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
                    fileoff_t eof2 = ftell(f);
                    DBGOUT5(<<"eof is now " << eof2);
                }
#endif
                n = fwrite(junk, int(o), 1, f);
                if ( n != 1)  {
                    w_rc_t e = RC(eOS);
                    smlevel_0::errlog->clog << fatal_prio <<
                    "   fwrite: can't round out log block size ..." << flushl;
                    W_COERCE(e);
                }

#ifdef W_TRACE
                {
                    fileoff_t eof2 = ftell(f);
                    DBGOUT5(<<"eof is now " << eof2);
                }
#endif
                delete[] junk;
                o = 0;
            }
            delete s; // skip_log

            eof = ftell(f);
            w_rc_t e = RC(eOS);        /* collect the error in case it is needed */
            DBGOUT5(<<"eof is now " << eof);


            if(((eof) % BLOCK_SIZE) != 0) {
                smlevel_0::errlog->clog << fatal_prio <<
                    "   ftell: can't write skip rec to log ..." << flushl;
                W_COERCE(e);
            }
            W_IGNORE(e);        /* error not used */

            if (os_fsync(fileno(f)) < 0) {
                e = RC(eOS);
                smlevel_0::errlog->clog << fatal_prio <<
                    "   fsync: can't sync fsync truncated log ..." << flushl;
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
                    smlevel_0::errlog->clog << fatal_prio
                            << " Cannot stat fd " << fileno(f)
                            << ":" << endl << e << endl << flushl;
                    W_COERCE(e);
                }
                DBGOUT5(<< "size of " << fname << " is " << statbuf.st_size);
            }
#endif
            fclose(f);
        }

    } else {
        w_assert3(!last_partition_exists);
    }
    } // End truncate at last complete log rec

    /*
     *  initialize current and durable lsn for
     *  the purpose of sanity checks in open*()
     *  and elsewhere
     */
    DBGOUT5( << "partition num = " << partition_num()
        <<" current_lsn " << curr_lsn()
        <<" durable_lsn " << durable_lsn());

    lsn_t new_lsn(last_partition, pos);


    _curr_lsn = _durable_lsn = _flush_lsn = new_lsn;



    DBGOUT2( << "partition num = " << partition_num()
            <<" current_lsn " << curr_lsn()
            <<" durable_lsn " << durable_lsn());

    {
        /*
         *  create/open the "current" partition
         *  "current" could be new or existing
         *  Check its size and all the records in it
         *  by passing "true" for the last argument to open()
         */

        // Find out if there's a hint about the length of the
        // partition (from the checkpoint).  This lsn serves as a
        // starting point from which to search for the skip_log record
        // in the file.  It's a performance thing...
        lsn_t lasthint;
        for(int q=0; q<listlength; q++) {
            if(lsnlist[q].hi() == last_partition) {
                lasthint = lsnlist[q];
            }
        }
        partition_t *p = _open_partition_for_append(last_partition, lasthint,
                last_partition_exists, true);

        /* XXX error info lost */
        if(!p) {
            smlevel_0::errlog->clog << fatal_prio
            << "ERROR: could not open log file for partition "
            << last_partition << flushl;
            W_FATAL(eINTERNAL);
        }

        w_assert3(p->num() == last_partition);
        w_assert3(partition_num() == last_partition);
        w_assert3(partition_index() == p->index());

    }
    DBGOUT2( << "partition num = " << partition_num()
            <<" current_lsn " << curr_lsn()
            <<" durable_lsn " << durable_lsn());

    cs.exit();
    if(1){
        // Print various interesting info to the log:
        errlog->clog << debug_prio
            << "Log max_partition_size (based on OS max file size)"
            << max_partition_size() << endl
            << "Log max_partition_size * PARTITION_COUNT "
                    << max_partition_size() * PARTITION_COUNT << endl
            << "Log min_partition_size (based on fixed segment size and fixed block size) "
                    << min_partition_size() << endl
            << "Log min_partition_size*PARTITION_COUNT "
                    << min_partition_size() * PARTITION_COUNT << endl;

        errlog->clog << debug_prio
            << "Log BLOCK_SIZE (log write size) " << BLOCK_SIZE
            << endl
            << "Log segsize() (log buffer size) " << segsize()
            << endl
            << "Log segsize()/BLOCK_SIZE " << double(segsize())/double(BLOCK_SIZE)
            << endl;

        errlog->clog << debug_prio
            << "User-option smlevel_0::max_logsz " << max_logsz << endl
            << "Log _partition_data_size " << _partition_data_size
            << endl
            << "Log _partition_data_size/segsize() "
                << double(_partition_data_size)/double(segsize())
            << endl
            << "Log _partition_data_size/segsize()+BLOCK_SIZE "
                << _partition_data_size + BLOCK_SIZE
            << endl;

        errlog->clog << debug_prio
            << "Log _start " << start_byte() << " end_byte() " << end_byte()
            << endl
                     << "Log _curr_lsn " << curr_lsn()
                     << " _durable_lsn " << durable_lsn()
            << endl;
        errlog->clog << debug_prio
            << "Curr epoch  base_lsn " << _log_buffer->_cur_epoch.base_lsn
            << endl
            << "Curr epoch  base " << _log_buffer->_cur_epoch.base
            << endl
            << "Curr epoch  start " << _log_buffer->_cur_epoch.start
            << endl
            << "Curr epoch  end " << _log_buffer->_cur_epoch.end
            << endl;
        errlog->clog << debug_prio
            << "Old epoch  base_lsn " << _log_buffer->_old_epoch.base_lsn
            << endl
            << "Old epoch  base " << _log_buffer->_old_epoch.base
            << endl
            << "Old epoch  start " << _log_buffer->_old_epoch.start
            << endl
            << "Old epoch  end " << _log_buffer->_old_epoch.end
            << endl;
    }
}
#else
NORET
log_core::log_core(
    long bsize,
    bool reformat,
    int carray_active_slot_count)

    :
      _reservations_active(false),
      _waiting_for_space(false),
      _waiting_for_flush(false),
      _start(0),
      _end(0),
      _segsize(_ceil(bsize, SEGMENT_SIZE)),
      // _blocksize(BLOCK_SIZE),
      _buf(NULL),
      _shutting_down(false),
      _flush_daemon_running(false),
      _carray(new ConsolidationArray(carray_active_slot_count)),
      _curr_index(-1),
      _curr_num(1),
      _readbuf(NULL),
#ifdef LOG_DIRECT_IO
      _writebuf(NULL),
#endif
      _skip_log(NULL)
{
    FUNC(log_core::log_core);
    DO_PTHREAD(pthread_mutex_init(&_wait_flush_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_wait_cond, NULL));
    DO_PTHREAD(pthread_cond_init(&_flush_cond, NULL));
    DO_PTHREAD(pthread_mutex_init(&_scavenge_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_scavenge_cond, NULL));

#ifdef LOG_DIRECT_IO
    posix_memalign((void**)&_buf, LOG_DIO_ALIGN, _segsize);
    //_buf = new char[_segsize];
#else
    _buf = new char[_segsize];
#endif

    // NOTE: GROT must make this a function of page size, and of xfer size,
    // since xfer size is fixed (8K).
    // It has to big enough to read the maximum-sized log record, clearly
    // more than a page.
#ifdef LOG_DIRECT_IO
#if SM_PAGESIZE < 8192
    posix_memalign((void**)&_readbuf, LOG_DIO_ALIGN, BLOCK_SIZE*4);
    //_readbuf = new char[BLOCK_SIZE*4];
    // we need two blocks for the write buffer because the skip log record may span two blocks
    posix_memalign((void**)&_writebuf, LOG_DIO_ALIGN, BLOCK_SIZE*2);
#else
    posix_memalign((void**)&_readbuf, LOG_DIO_ALIGN, SM_PAGESIZE*4);
    //_readbuf = new char[SM_PAGESIZE*4];
    posix_memalign((void**)&_writebuf, LOG_DIO_ALIGN, SM_PAGESIZE*2);
#endif
#else
#if SM_PAGESIZE < 8192
    _readbuf = new char[BLOCK_SIZE*4];
#else
    _readbuf = new char[SM_PAGESIZE*4];
#endif
#endif // LOG_DIRECT_IO

    _skip_log = new skip_log;

    /* Create thread o flush the log */
    _flush_daemon = new flush_daemon_thread_t(this);

    if (bsize < 64 * 1024) {
        // not mt-safe, but this is not going to happen in
        // concurrency scenario
        errlog->clog << error_prio
        << "Log buf size (sm_logbufsize) too small: "
        << bsize << ", require at least " << 64 * 1024
        << endl;
        errlog->clog << error_prio << endl;
        fprintf(stderr,
            "Log buf size (sm_logbufsize) too small: %ld, need %d\n",
            bsize, 64*1024);
        W_FATAL(eINTERNAL);
    }

    w_assert1(is_aligned(_readbuf));

    // By the time we get here, the max_logsize should already have been
    // adjusted by the sm options-handling code, so it should be
    // a legitimate value now.
    W_COERCE(_set_size(max_logsz));

    DBGOUT3(<< "SEG SIZE " << _segsize << " PARTITION DATA SIZE " << _partition_data_size);


    // FRJ: we don't actually *need* this (no trx around yet), but we
    // don't want to trip the assertions that watch for it.
    CRITICAL_SECTION(cs, _partition_lock);

    partition_number_t  last_partition = partition_num();
    bool                last_partition_exists = false;
    /*
     * make sure there's room for the log names
     */
    fileoff_t eof= fileoff_t(0);

    os_dirent_t *dd=0;
    os_dir_t ldir = os_opendir(dir_name());
    if (! ldir)
    {
        w_rc_t e = RC(eOS);
        smlevel_0::errlog->clog << fatal_prio
            << "Error: could not open the log directory " << dir_name() <<flushl;
        fprintf(stderr, "Error: could not open the log directory %s\n",
                    dir_name());

        smlevel_0::errlog->clog << fatal_prio
            << "\tNote: the log directory is specified using\n"
            "\t      the sm_logdir option." << flushl;

        smlevel_0::errlog->clog << flushl;

        W_COERCE(e);
    }
    DBGTHRD(<<"opendir " << dir_name() << " succeeded");

    /*
     *  scan directory for master lsn and last log file
     */

    _master_lsn = lsn_t::null;

    uint32_t min_index = max_uint4;

    char *fname = new char [smlevel_0::max_devname];
    if (!fname)
        W_FATAL(fcOUTOFMEMORY);
    w_auto_delete_array_t<char> ad_fname(fname);

    /* Create a list of lsns for the partitions - this
     * will be used to store any hints about the last
     * lsns of the partitions (stored with checkpoint meta-info
     */
    lsn_t lsnlist[PARTITION_COUNT];
    int   listlength=0;
    {
        /*
         *  initialize partition table
         */
        partition_index_t i;
        for (i = 0; i < PARTITION_COUNT; i++)  {
            _part[i].init_index(i);
            _part[i].init(this);
        }
    }

    DBGTHRD(<<"reformat= " << reformat
            << " last_partition "  << last_partition
            << " last_partition_exists "  << last_partition_exists
            );
    if (reformat)
    {
        smlevel_0::errlog->clog << emerg_prio
            << "Reformatting logs..." << endl;

        while ((dd = os_readdir(ldir)))
        {
            DBGTHRD(<<"master_prefix= " << master_prefix());

            unsigned int namelen = strlen(log_prefix());
            namelen = namelen > strlen(master_prefix())? namelen :
                                        strlen(master_prefix());

            const char *d = dd->d_name;
            unsigned int orig_namelen = strlen(d);
            namelen = namelen > orig_namelen ? namelen : orig_namelen;

            char *name = new char [namelen+1];
            w_auto_delete_array_t<char>  cleanup(name);

            memset(name, '\0', namelen+1);
            strncpy(name, d, orig_namelen);
            DBGTHRD(<<"name= " << name);

            bool parse_ok = (strncmp(name,master_prefix(),strlen(master_prefix()))==0);
            if(!parse_ok) {
                parse_ok = (strncmp(name,log_prefix(),strlen(log_prefix()))==0);
            }
            if(parse_ok) {
                smlevel_0::errlog->clog << debug_prio
                    << "\t" << name << "..." << endl;

                {
                    w_ostrstream s(fname, (int) smlevel_0::max_devname);
                    s << dir_name() << _SLASH << name << ends;
                    w_assert1(s);
                    if( unlink(fname) < 0) {
                        w_rc_t e = RC(fcOS);
                        smlevel_0::errlog->clog << debug_prio
                            << "unlink(" << fname << "):"
                            << endl << e << endl;
                    }
                }
            }
        }

        //  os_closedir(ldir);
        w_assert3(!last_partition_exists);
    }

    DBGOUT5(<<"about to readdir"
            << " last_partition "  << last_partition
            << " last_partition_exists "  << last_partition_exists
            );

    while ((dd = os_readdir(ldir)))
    {
        DBGOUT5(<<"dd->d_name=" << dd->d_name);

        // XXX should abort on name too long earlier, or size buffer to fit
        const unsigned int prefix_len = strlen(master_prefix());
        w_assert3(prefix_len < smlevel_0::max_devname);

        char *buf = new char[smlevel_0::max_devname+1];
        if (!buf)
                W_FATAL(fcOUTOFMEMORY);
        w_auto_delete_array_t<char>  ad_buf(buf);

        unsigned int         namelen = prefix_len;
        const char *         dn = dd->d_name;
        unsigned int         orig_namelen = strlen(dn);

        namelen = namelen > orig_namelen ? namelen : orig_namelen;
        char *                name = new char [namelen+1];
        w_auto_delete_array_t<char>  cleanup(name);

        memset(name, '\0', namelen+1);
        strncpy(name, dn, orig_namelen);

        strncpy(buf, name, prefix_len);
        buf[prefix_len] = '\0';

        DBGOUT5(<<"name= " << name);

        bool parse_ok = ((strlen(buf)) == prefix_len);

        DBGOUT5(<<"parse_ok  = " << parse_ok
                << " buf = " << buf
                << " prefix_len = " << prefix_len
                << " strlen(buf) = " << strlen(buf));
        if (parse_ok) {
            lsn_t tmp;
            if (strcmp(buf, master_prefix()) == 0)
            {
                DBGOUT5(<<"found log file " << buf);
                /*
                 *  File name matches master prefix.
                 *  Extract master lsn & lsns of skip-records
                 */
                lsn_t tmp1;
                bool old_style=false;
                rc_t rc = _read_master(name, prefix_len,
                        tmp, tmp1, lsnlist, listlength,
                        old_style);
                W_COERCE(rc);

                if (tmp < master_lsn())  {
                    /*
                     *  Swap tmp <-> _master_lsn, tmp1 <-> _min_chkpt_rec_lsn
                     */
                    std::swap(_master_lsn, tmp);
                    std::swap(_min_chkpt_rec_lsn, tmp1);
                }
                /*
                 *  Remove the older master record.
                 */
                if (_master_lsn != lsn_t::null) {
                    _make_master_name(_master_lsn,
                                      _min_chkpt_rec_lsn,
                                      fname,
                                      smlevel_0::max_devname);
                    (void) unlink(fname);
                }
                /*
                 *  Save the new master record
                 */
                _master_lsn = tmp;
                _min_chkpt_rec_lsn = tmp1;
                DBGOUT5(<<" _master_lsn=" << _master_lsn
                 <<" _min_chkpt_rec_lsn=" << _min_chkpt_rec_lsn);

                DBGOUT5(<<"parse_ok = " << parse_ok);

            } else if (strcmp(buf, log_prefix()) == 0)  {
                DBGOUT5(<<"found log file " << buf);
                /*
                 *  File name matches log prefix
                 */

                w_istrstream s(name + prefix_len);
                uint32_t curr;
                if (! (s >> curr))  {
                    smlevel_0::errlog->clog << fatal_prio
                    << "bad log file \"" << name << "\"" << flushl;
                    W_FATAL(eINTERNAL);
                }

                DBGOUT5(<<"curr " << curr
                        << " partition_num()==" << partition_num()
                        << " last_partition_exists " << last_partition_exists
                        );

                if (curr >= last_partition) {
                    last_partition = curr;
                    last_partition_exists = true;
                    DBGOUT5(<<"new last_partition " << curr
                        << " exits=true" );
                }
                if (curr < min_index) {
                    min_index = curr;
                }
            } else {
                DBGOUT5(<<"NO MATCH");
                DBGOUT5(<<"_master_prefix= " << master_prefix());
                DBGOUT5(<<"_log_prefix= " << log_prefix());
                DBGOUT5(<<"buf= " << buf);
                parse_ok = false;
            }
        }

        /*
         *  if we couldn't parse the file name and it was not "." or ..
         *  then print an error message
         */
        if (!parse_ok && ! (strcmp(name, ".") == 0 ||
                                strcmp(name, "..") == 0)) {
            smlevel_0::errlog->clog << fatal_prio
                                    << "log_core: cannot parse filename \""
                                    << name << "\".  Maybe a data volume in the logging directory?"
                                    << flushl;
            W_FATAL(fcINTERNAL);
        }
    }
    os_closedir(ldir);

    DBGOUT5(<<"after closedir  "
            << " last_partition "  << last_partition
            << " last_partition_exists "  << last_partition_exists
            );

#if W_DEBUG_LEVEL > 2
    if(reformat) {
        w_assert3(partition_num() == 1);
        w_assert3(_min_chkpt_rec_lsn.hi() == 1);
        w_assert3(_min_chkpt_rec_lsn.lo() == first_lsn(1).lo());
    } else {
       // ??
    }
    w_assert3(partition_index() == -1);
#endif

    DBGOUT5(<<"Last partition is " << last_partition
        << " existing = " << last_partition_exists
     );

    /*
     *  Destroy all partitions less than _min_chkpt_rec_lsn
     *  Open the rest and close them.
     *  There might not be an existing last_partition,
     *  regardless of the value of "reformat"
     */
    {
        partition_number_t n;
        partition_t        *p;

        DBGOUT5(<<" min_chkpt_rec_lsn " << min_chkpt_rec_lsn()
                << " last_partition " << last_partition);
        w_assert3(min_chkpt_rec_lsn().hi() <= last_partition);

        for (n = min_index; n < min_chkpt_rec_lsn().hi(); n++)  {
            // not an error if we can't unlink (probably doesn't exist)
            DBGOUT5(<<" destroy_file " << n << "false");
            destroy_file(n, false);
        }
        for (n = _min_chkpt_rec_lsn.hi(); n <= last_partition; n++)  {
            // Find out if there's a hint about the length of the
            // partition (from the checkpoint).  This lsn serves as a
            // starting point from which to search for the skip_log record
            // in the file.  It's a performance thing...
            lsn_t lasthint;
            for(int q=0; q<listlength; q++) {
                if(lsnlist[q].hi() == n) {
                    lasthint = lsnlist[q];
                }
            }

            // open and check each file (get its size)
            DBGOUT5(<<" open " << n << "true, false, true");

            // last argument indicates "in_recovery" more accurately,
            // we should say "at-startup"
            p = _open_partition_for_read(n, lasthint, true, true);
            w_assert3(p == _n_partition(n));
            p->close();
            unset_current();
            DBGOUT5(<<" done w/ open " << n );
        }
    }

    /* XXXX :  Don't have a static method on
     * partition_t for start()
    */
    /* end of the last valid log record / start of invalid record */
    fileoff_t pos = 0;

    { // Truncate at last complete log rec
    DBGOUT5(<<" truncate last complete log rec ");

    /*
     *
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
     *
     */
    make_log_name(last_partition, fname, smlevel_0::max_devname);
    DBGOUT5(<<" checking " << fname);

    FILE *f =  fopen(fname, "r");
    DBGOUT5(<<" opened " << fname << " fp " << f << " pos " << pos);

    fileoff_t start_pos = pos;

    /* If the master checkpoint is in the current partition, seek
       to its position immediately, instead of scanning from the
       beginning of the log.   If the current partition doesn't have
       a checkpoint, must read entire paritition until the skip
       record is found. */

    const lsn_t &seek_lsn = _master_lsn;

    if (f && seek_lsn.hi() == last_partition) {
            start_pos = seek_lsn.lo();

            DBGOUT5(<<" seeking to start_pos " << start_pos);
            if (fseek(f, start_pos, SEEK_SET)) {
                smlevel_0::errlog->clog  << error_prio
                    << "log read: can't seek to " << start_pos
                     << " starting log scan at origin"
                     << endl;
                start_pos = pos;
            }
            else
                pos = start_pos;
    }
    DBGOUT5(<<" pos is now " << pos);

    if (f)  {
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
                        smlevel_0::errlog->clog << fatal_prio
                        << "ERROR: unexpected log file inconsistency." << flushl;
                        W_COERCE(e);
                    }
                    break;
                }
                DBGOUT5(<<"pos = " <<  pos
                    << " lsn_ck = " <<lsn_ck);

                // make sure log record's lsn matched its position in file
                if ( (lsn_ck.lo() != pos) ||
                    (lsn_ck.hi() != (uint32_t) last_partition ) ) {
                    // found partial log record, end of log is previous record
                    smlevel_0::errlog->clog << error_prio <<
        "Found unexpected end of log -- probably due to a previous crash."
                    << flushl;
                    smlevel_0::errlog->clog << error_prio <<
                    "   Recovery will continue ..." << flushl;
                    break;
                }

                // remember current position
                pos = ftell(f) ;
            }
        }
        fclose(f);

        {
            DBGOUT5(<<"explicit truncating " << fname << " to " << pos);
            w_assert0(os_truncate(fname, pos )==0);

            //
            // but we can't just use truncate() --
            // we have to truncate to a size that's a mpl
            // of the page size. First append a skip record
            DBGOUT5(<<"explicit opening  " << fname );
            f =  fopen(fname, "a");
            if (!f) {
                w_rc_t e = RC(fcOS);
                smlevel_0::errlog->clog  << fatal_prio
                    << "fopen(" << fname << "):" << endl << e << endl;
                W_COERCE(e);
            }
            skip_log *s = new skip_log; // deleted below
            s->set_lsn_ck( lsn_t(uint32_t(last_partition), sm_diskaddr_t(pos)) );

            DBGOUT5(<<"writing skip_log at pos " << pos << " with lsn "
                << s->get_lsn_ck()
                << "and size " << s->length()
                );
#ifdef W_TRACE
            {
                fileoff_t eof2 = ftell(f);
                DBGOUT5(<<"eof is now " << eof2);
            }
#endif

            if ( fwrite(s, s->length(), 1, f) != 1)  {
                w_rc_t        e = RC(eOS);
                smlevel_0::errlog->clog << fatal_prio <<
                    "   fwrite: can't write skip rec to log ..." << flushl;
                W_COERCE(e);
            }
#ifdef W_TRACE
            {
                fileoff_t eof2 = ftell(f);
                DBGTHRD(<<"eof is now " << eof2);
            }
#endif
            fileoff_t o = pos;
            o += s->length();
            o = o % BLOCK_SIZE;
            DBGOUT5(<<"BLOCK_SIZE " << int(BLOCK_SIZE));
            if(o > 0) {
                o = BLOCK_SIZE - o;
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
                    fileoff_t eof2 = ftell(f);
                    DBGOUT5(<<"eof is now " << eof2);
                }
#endif
                n = fwrite(junk, int(o), 1, f);
                if ( n != 1)  {
                    w_rc_t e = RC(eOS);
                    smlevel_0::errlog->clog << fatal_prio <<
                    "   fwrite: can't round out log block size ..." << flushl;
                    W_COERCE(e);
                }
#ifdef W_TRACE
                {
                    fileoff_t eof2 = ftell(f);
                    DBGOUT5(<<"eof is now " << eof2);
                }
#endif
                delete[] junk;
                o = 0;
            }
            delete s; // skip_log

            eof = ftell(f);
            w_rc_t e = RC(eOS);        /* collect the error in case it is needed */
            DBGOUT5(<<"eof is now " << eof);


            if(((eof) % BLOCK_SIZE) != 0) {
                smlevel_0::errlog->clog << fatal_prio <<
                    "   ftell: can't write skip rec to log ..." << flushl;
                W_COERCE(e);
            }
            W_IGNORE(e);        /* error not used */

            if (os_fsync(fileno(f)) < 0) {
                e = RC(eOS);
                smlevel_0::errlog->clog << fatal_prio <<
                    "   fsync: can't sync fsync truncated log ..." << flushl;
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
                    smlevel_0::errlog->clog << fatal_prio
                            << " Cannot stat fd " << fileno(f)
                            << ":" << endl << e << endl << flushl;
                    W_COERCE(e);
                }
                DBGOUT5(<< "size of " << fname << " is " << statbuf.st_size);
            }
#endif
            fclose(f);
        }

    } else {
        w_assert3(!last_partition_exists);
    }
    } // End truncate at last complete log rec

    /*
     *  initialize current and durable lsn for
     *  the purpose of sanity checks in open*()
     *  and elsewhere
     */
    DBGOUT5( << "partition num = " << partition_num()
        <<" current_lsn " << curr_lsn()
        <<" durable_lsn " << durable_lsn());

    lsn_t new_lsn(last_partition, pos);
    _curr_lsn = _durable_lsn = _flush_lsn = new_lsn;

    DBGOUT2( << "partition num = " << partition_num()
            <<" current_lsn " << curr_lsn()
            <<" durable_lsn " << durable_lsn());

    {
        /*
         *  create/open the "current" partition
         *  "current" could be new or existing
         *  Check its size and all the records in it
         *  by passing "true" for the last argument to open()
         */

        // Find out if there's a hint about the length of the
        // partition (from the checkpoint).  This lsn serves as a
        // starting point from which to search for the skip_log record
        // in the file.  It's a performance thing...
        lsn_t lasthint;
        for(int q=0; q<listlength; q++) {
            if(lsnlist[q].hi() == last_partition) {
                lasthint = lsnlist[q];
            }
        }
        partition_t *p = _open_partition_for_append(last_partition, lasthint,
                last_partition_exists, true);

        /* XXX error info lost */
        if(!p) {
            smlevel_0::errlog->clog << fatal_prio
            << "ERROR: could not open log file for partition "
            << last_partition << flushl;
            W_FATAL(eINTERNAL);
        }

        w_assert3(p->num() == last_partition);
        w_assert3(partition_num() == last_partition);
        w_assert3(partition_index() == p->index());

    }
    DBGOUT2( << "partition num = " << partition_num()
            <<" current_lsn " << curr_lsn()
            <<" durable_lsn " << durable_lsn());

    cs.exit();
    if(1){
        // Print various interesting info to the log:
        errlog->clog << debug_prio
            << "Log max_partition_size (based on OS max file size)"
            << max_partition_size() << endl
            << "Log max_partition_size * PARTITION_COUNT "
                    << max_partition_size() * PARTITION_COUNT << endl
            << "Log min_partition_size (based on fixed segment size and fixed block size) "
                    << min_partition_size() << endl
            << "Log min_partition_size*PARTITION_COUNT "
                    << min_partition_size() * PARTITION_COUNT << endl;

        errlog->clog << debug_prio
            << "Log BLOCK_SIZE (log write size) " << BLOCK_SIZE
            << endl
            << "Log segsize() (log buffer size) " << segsize()
            << endl
            << "Log segsize()/BLOCK_SIZE " << double(segsize())/double(BLOCK_SIZE)
            << endl;

        errlog->clog << debug_prio
            << "User-option smlevel_0::max_logsz " << max_logsz << endl
            << "Log _partition_data_size " << _partition_data_size
            << endl
            << "Log _partition_data_size/segsize() "
                << double(_partition_data_size)/double(segsize())
            << endl
            << "Log _partition_data_size/segsize()+BLOCK_SIZE "
                << _partition_data_size + BLOCK_SIZE
            << endl;

        errlog->clog << debug_prio
            << "Log _start " << start_byte() << " end_byte() " << end_byte()
            << endl
            << "Log _curr_lsn " << _curr_lsn
            << " _durable_lsn " << _durable_lsn
            << endl;
        errlog->clog << debug_prio
            << "Curr epoch  base_lsn " << _cur_epoch.base_lsn
            << endl
            << "Curr epoch  base " << _cur_epoch.base
            << endl
            << "Curr epoch  start " << _cur_epoch.start
            << endl
            << "Curr epoch  end " << _cur_epoch.end
            << endl;
        errlog->clog << debug_prio
            << "Old epoch  base_lsn " << _old_epoch.base_lsn
            << endl
            << "Old epoch  base " << _old_epoch.base
            << endl
            << "Old epoch  start " << _old_epoch.start
            << endl
            << "Old epoch  end " << _old_epoch.end
            << endl;
    }
}
#endif // LOG_BUFFER


#ifdef LOG_BUFFER
log_core::~log_core()
{
    if(THE_LOG != NULL)
    {
        partition_t        *p;
        for (uint i = 0; i < PARTITION_COUNT; i++) {
            p = _partition(i);
            p->close_for_read();
            p->close_for_append();
            DBG(<< " calling clear");
            p->clear();
        }
        w_assert1(_durable_lsn == _curr_lsn);

#ifdef LOG_DIRECT_IO
        free(_readbuf);
        free(_writebuf);
        _writebuf = NULL;
#else
        delete [] _readbuf;
#endif
        _readbuf = NULL;
        delete _skip_log;
        _skip_log = NULL;


        delete _log_buffer;

        DO_PTHREAD(pthread_mutex_destroy(&_scavenge_lock));
        DO_PTHREAD(pthread_cond_destroy(&_scavenge_cond));
        THE_LOG = NULL;
    }
}
#else
log_core::~log_core()
{
    if(THE_LOG != NULL)
    {
        partition_t        *p;
        for (uint i = 0; i < PARTITION_COUNT; i++) {
            p = _partition(i);
            p->close_for_read();
            p->close_for_append();
            DBG(<< " calling clear");
            p->clear();
        }
        w_assert1(_durable_lsn == _curr_lsn);

        delete _carray;

#ifdef LOG_DIRECT_IO
        free(_readbuf);
        free(_writebuf);
        _writebuf = NULL;
#else
        delete [] _readbuf;
#endif
        _readbuf = NULL;
        delete _skip_log;
        _skip_log = NULL;

#ifdef LOG_DIRECT_IO
        free(_buf);
#else
        delete [] _buf;
#endif
        _buf = NULL;

        DO_PTHREAD(pthread_mutex_destroy(&_wait_flush_lock));
        DO_PTHREAD(pthread_cond_destroy(&_wait_cond));
        DO_PTHREAD(pthread_cond_destroy(&_flush_cond));
        DO_PTHREAD(pthread_mutex_destroy(&_scavenge_lock));
        DO_PTHREAD(pthread_cond_destroy(&_scavenge_cond));
        THE_LOG = NULL;
    }
}
#endif // LOG_BUFFER

partition_t *
log_core::_partition(partition_index_t i) const
{
    return i<0 ? (partition_t *)0: (partition_t *) &_part[i];
}


void
log_core::destroy_file(partition_number_t n, bool pmsg)
{
    char        *fname = new char[smlevel_0::max_devname];
    if (!fname)
        W_FATAL(fcOUTOFMEMORY);
    w_auto_delete_array_t<char> ad_fname(fname);
    make_log_name(n, fname, smlevel_0::max_devname);
    if (unlink(fname) == -1)  {
        w_rc_t e = RC(eOS);
        smlevel_0::errlog->clog  << error_prio
            << "destroy_file " << n << " " << fname << ":" <<endl
             << e << endl;
        if(pmsg) {
            smlevel_0::errlog->clog << error_prio
            << "warning : cannot free log file \""
            << fname << '\"' << flushl;
            smlevel_0::errlog->clog << error_prio
            << "          " << e << flushl;
        }
    }
}

/**\brief compute size of partition from given max-open-log-bytes size
 * \details
 * PARTITION_COUNT == smlevel_0::max_openlog is fixed.
 * SEGMENT_SIZE  is fixed.
 * BLOCK_SIZE  is fixed.
 * Only the partition size is determinable by the user; it's the
 * size of a partition file and PARTITION_COUNT*partition-size is
 * therefore the maximum amount of log space openable at one time.
 */
w_rc_t log_core::_set_size(fileoff_t size)
{
    /* The log consists of at most PARTITION_COUNT open files,
     * each with space for some integer number of segments (log buffers)
     * plus one extra block for writing skip records.
     *
     * Each segment is an integer number of blocks (BLOCK_SIZE), which
     * is the size of an I/O.  An I/O is padded, if necessary, to BLOCK_SIZE.
     */
    fileoff_t usable_psize = size/PARTITION_COUNT - BLOCK_SIZE;

    // partition must hold at least one buffer...
    if (usable_psize < _segsize) {
        W_FATAL(eOUTOFLOGSPACE);
    }

    // largest integral multiple of segsize() not greater than usable_psize:
    _partition_data_size = _floor(usable_psize, (segsize()));

    if(_partition_data_size == 0)
    {
        cerr << "log size is too small: size "<<size<<" usable_psize "<<usable_psize
        <<", segsize() "<<segsize()<<", blocksize "<<BLOCK_SIZE<< endl
        <<"need at least "<<_get_min_size()<<" ("<<(_get_min_size()/1024)<<" * 1024 = "<<(1024 *(_get_min_size()/1024))<<") "<< endl;
        W_FATAL(eOUTOFLOGSPACE);
    }
    _partition_size = _partition_data_size + BLOCK_SIZE;
    DBGTHRD(<< "log_core::_set_size setting _partition_size (limit LIMIT) "
            << _partition_size);
    /*
    fprintf(stderr,
"size %ld usable_psize %ld segsize() %ld _part_data_size %ld _part_size %ld\n",
            size,
            usable_psize,
            segsize(),
            _partition_data_size,
            _partition_size
           );
    */
    // initial free space estimate... refined once log recovery is complete
    // release_space(PARTITION_COUNT*_partition_data_size);
    release_space(recoverable_space(PARTITION_COUNT));
    if(!verify_chkpt_reservation()
            || _space_rsvd_for_chkpt > _partition_data_size) {
        cerr<<
        "log partitions too small compared to buffer pool:"<<endl
        <<"    "<<_partition_data_size<<" bytes per partition available"<<endl
        <<"    "<<_space_rsvd_for_chkpt<<" bytes needed for checkpointing dirty pages"<<endl;
        return RC(eOUTOFLOGSPACE);
    }
    return RCOK;
}

#ifdef LOG_BUFFER
// see logbuf_core::_acquire_buffer_space(CArraySlot* info, long recsize)
#else
void log_core::_acquire_buffer_space(CArraySlot* info, long recsize)
{
    w_assert2(recsize > 0);


  /* Copy our data into the log buffer and update/create epochs.
   * Re: Racing flush daemon over
   * epochs, _start, _end, _curr_lsn, _durable_lsn :
   *
   * _start is set by  _prime at startup (no log flush daemon yet)
   *                   and flush_daemon_work, not by insert
   * _end is set by  _prime at startup (no log flush daemon yet)
   *                   and insert, not by log flush daemon
   * _old_epoch is  protected by _flush_lock
   * _cur_epoch is  protected by _flush_lock EXCEPT
   *                when insert does not wrap, it sets _cur_epoch.end,
   *                which is only read by log flush daemon
   *                The _end is only set after the memcopy is done,
   *                so this should be safe.
   * _curr_lsn is set by  insert (and at startup)
   * _durable_lsn is set by flush daemon
   *
   * NOTE: _end, _start, epochs updated in
   * _prime(), but that is called only for
   * opening a partition for append in startup/recovery case,
   * in which case there is no race to be had.
   *
   * It is also updated below.
   */

    /*
    * Make sure there's actually space available in the
    * log buffer,
    * accounting for the fact that flushes (by the daemon)
    * always work with full blocks. (they round down start of
    * flush to beginning of block and round up/pad end of flush to
    * end of block).
    *
    * If not, kick the flush daemon to make space.
    */
    while(*&_waiting_for_space ||
            end_byte() - start_byte() + recsize > segsize() - 2*BLOCK_SIZE)
    {
        _insert_lock.release(&info->me);
        {
            CRITICAL_SECTION(cs, _wait_flush_lock);
            while(end_byte() - start_byte() + recsize > segsize() - 2*BLOCK_SIZE)
            {
                _waiting_for_space = true;
                // Use signal since the only thread that should be waiting
                // on the _flush_cond is the log flush daemon.
                DO_PTHREAD(pthread_cond_signal(&_flush_cond));
                DO_PTHREAD(pthread_cond_wait(&_wait_cond, &_wait_flush_lock));
            }
        }
        _insert_lock.acquire(&info->me);
    }
    // lfence because someone else might have entered and left during above release/acquire.
    lintel::atomic_thread_fence(lintel::memory_order_consume);
    // Having ics now should mean that even if another insert snuck in here,
    // we're ok since we recheck the condition. However, we *could* starve here.


  /* _curr_lsn, _cur_epoch.end, and end_byte() are all strongly related.
   *
   * _curr_lsn is the lsn of first byte past the tail of the log.
   *    Tail of the log is the last byte of the last record inserted (does not
   *    include the skip_log record).  Inserted records go to _curr_lsn,
   *    _curr_lsn moves with records inserted.
   *
   * _cur_epoch.end and end_byte() are convenience variables to avoid doing
   * expensive operations like modulus and division on the _curr_lsn
   * (lsn of next record to be inserted):
   *
   * _cur_epoch.end is the position of the current lsn relative to the
   *    segsize() log buffer.
   *    It is relative to the log buffer, and it wraps (modulo the
   *    segment size, which is the log buffer size). ("spill")
   *
   * end_byte()/_end is the non-wrapping version of _cur_epoch.end:
   *     at log init time it is set to a value in the range [0, segsize())
   *     and is  incremented by every log insert for the lifetime of
   *     the database.
   *     It is a byte-offset from the beginning of the log, considering that
   *     partitions are not "contiguous" in this sense.  Each partition has
   *     a lot of byte-offsets that aren't really in the log because
   *     we limit the size of a partition.
   *
   * _durable_lsn is the lsn of the first byte past the last
   *     log record written to the file (not counting the skip log record).
   *
   * _cur_epoch.start and start_byte() are convenience variables to avoid doing
   * expensive operations like modulus and division on the _durable_lsn
   *
   * _cur_epoch.start is the position of the durable lsn relative to the
   *     segsize() log buffer.   Because the _cur_epoch.end wraps, start
   *     could become > end and this would create a mess.  For this
   *     reason, when the log buffer wraps, we create a new
   *     epoch. The old epoch represents the entire unflushed
   *     portion of the old log buffer (including a portion of the
   *     presently-inserted log record) and the new epoch represents
   *     the next segment (logically, log buffer), containing
   *     the wrapped portion of the presently-inserted log record.
   *
   *     If, however, by starting a new segment to handle the wrap, we
   *     would exceed the partition size, we create a new epoch and
   *     new segment to hold the entire log record -- log records do not
   *     span partitions.   So we make the old epoch represent
   *     the unflushed portion of the old log buffer (not including any
   *     part of this record) and the new epoch represents the first segment
   *     in the new partition, and contains the entire presently-inserted
   *     log record.  We write the inserted log record at the beginning
   *     of the log buffer.
   *
   * start_byte()/_start is the non-wrapping version of _cur_epoch.start:
   *     At log init time it is set to 0
   *     and is bumped to match the durable lsn by every log flush.
   *     It is a byte-offset from the beginning of the log, considering that
   *     partitions are not "contiguous" in this sense.  Each partition has
   *     a lot of byte-offsets that aren't really in the log because
   *     we limit the size of a partition.
   *
   * start_byte() through end_byte() tell us the unflushed portion of the
   * log.
   */

  /* An epoch fits within a segment */
    w_assert2(_buf_epoch.end >= 0 && _buf_epoch.end <= segsize());

  /* end_byte() is the byte-offset-from-start-of-log-file
   * version of _cur_epoch.end */
    w_assert2(_buf_epoch.end % segsize() == end_byte() % segsize());

  /* _curr_lsn is the lsn of the next-to-be-inserted log record, i.e.,
   * the next byte of the log to be written
   */
  /* _cur_epoch.end is the offset into the log buffer of the _curr_lsn */
    w_assert2(_buf_epoch.end % segsize() == _curr_lsn.lo() % segsize());
  /* _curr_epoch.end should never be > segsize at this point;
   * that would indicate a wraparound is in progress when we entered this
   */
    w_assert2(end_byte() >= start_byte());

    // The following should be true since we waited on a condition
    // variable while
    // end_byte() - start_byte() + recsize > segsize() - 2*BLOCK_SIZE
    w_assert2(end_byte() - start_byte() <= segsize() - 2*BLOCK_SIZE);


    long end = _buf_epoch.end;
    long old_end = _buf_epoch.base + end;
    long new_end = end + recsize;
    // set spillsize to the portion of the new record that
    // wraps around to the beginning of the log buffer(segment)
    long spillsize = new_end - segsize();
    lsn_t curr_lsn = _curr_lsn;
    lsn_t next_lsn = _buf_epoch.base_lsn + new_end;
    long new_base = -1;
    long start_pos = end;

    if(spillsize <= 0) {
        // update epoch for next log insert
        _buf_epoch.end = new_end;
    }
    // next_lsn is first byte after the tail of the log.
    else if(next_lsn.lo() <= _partition_data_size) {
        // wrap within a partition
        _buf_epoch.base_lsn += _segsize;
        _buf_epoch.base += _segsize;
        _buf_epoch.start = 0;
        _buf_epoch.end = new_end = spillsize;
    }
    else {
        // new partition! need to update next_lsn/new_end to reflect this
        long leftovers = _partition_data_size - curr_lsn.lo();
        w_assert2(leftovers >= 0);
        if(leftovers && !reserve_space(leftovers)) {
            std::cerr << "WARNING WARNING: OUTOFLOGSPACE in update_epochs" << std::endl;
            info->error = eOUTOFLOGSPACE;
            _insert_lock.release(&info->me);
            return;
        }

        curr_lsn = first_lsn(next_lsn.hi()+1);
        next_lsn = curr_lsn + recsize;
        new_base = _buf_epoch.base + _segsize;
        start_pos = 0;
        _buf_epoch = epoch(curr_lsn, new_base, 0, new_end=recsize);
    }

    // let the world know
    _curr_lsn = next_lsn;
    _end = _buf_epoch.base + new_end;

    _carray->join_expose(info);
    _insert_lock.release(&info->me);

    info->lsn = curr_lsn; // where will we end up on disk?
    info->old_end = old_end; // lets us serialize with our predecessor after memcpy
    info->start_pos = start_pos; // != old_end when partitions wrap
    info->pos = start_pos + recsize; // coordinates groups of threads sharing a log allocation
    info->new_end = new_end; // eventually assigned to _cur_epoch
    info->new_base = new_base; // positive if we started a new partition
    info->error = w_error_ok;
}
#endif // LOG_BUFFER


#ifdef LOG_BUFFER
// see logbuf_core::_copy_to_buffer(logrec_t &rec, long pos, long recsize, CArraySlot* info)
#else
lsn_t log_core::_copy_to_buffer(logrec_t &rec, long pos, long recsize, CArraySlot* info)
{
    /*
      do the memcpy (or two)
    */
    lsn_t rlsn = info->lsn + pos;
    rec.set_lsn_ck(rlsn);

    // are we the ones that actually wrap? (do this *after* computing the lsn!)
    pos += info->start_pos;
    if(pos >= _segsize)
        pos -= _segsize;

    char const* data = (char const*) &rec;
    long spillsize = pos + recsize - _segsize;
    if(spillsize <= 0) {
        // normal insert
        memcpy(_buf+pos, data, recsize);
    }
    else {
        // spillsize > 0 so we are wrapping.
        // The wrap is within a partition.
        // next_lsn is still valid but not new_end
        //
        // Old epoch becomes valid for the flush daemon to
        // flush and "close".  It contains the first part of
        // this log record that we're trying to insert.
        // New epoch holds the rest of the log record that we're trying
        // to insert.
        //
        // spillsize is the portion that wraps around
        // partsize is the portion that doesn't wrap.
        long partsize = recsize - spillsize;

        // Copy log record to buffer
        // memcpy : areas do not overlap
        memcpy(_buf+pos, data, partsize);
        memcpy(_buf, data+partsize, spillsize);
    }

    return rlsn;
}
#endif // LOG_BUFFER


#ifdef LOG_BUFFER
// see logbuf_core::_update_epochs(CArraySlot* info)
#else
bool log_core::_update_epochs(CArraySlot* info) {
    w_assert1(info->vthis()->count == ConsolidationArray::SLOT_FINISHED);
    // Wait for our predecessor to catch up if we're ahead.
    // Even though the end pointer we're checking wraps regularly, we
    // already have to limit each address in the buffer to one active
    // writer or data corruption will result.
    if (CARRAY_RELEASE_DELEGATION) {
        if(_carray->wait_for_expose(info)) {
            return true; // we delegated!
        }
    } else {
        // If delegated-buffer-release is off, we simply spin until predecessors complete.
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
        while(*&_cur_epoch.vthis()->end + *&_cur_epoch.vthis()->base != info->old_end);
    }

    // now update the epoch(s)
    while (info != NULL) {
        w_assert1(*&_cur_epoch.vthis()->end + *&_cur_epoch.vthis()->base == info->old_end);
        if(info->new_base > 0) {
            // new partition! update epochs to reflect this

            // I just wrote part of the log record to the beginning of the
            // log buffer. How do I know that it didn't interfere with what
            // the flush daemon is writing? Because at the beginning of
            // this method, I waited until the log flush daemon ensured that
            // I had room to insert this entire record (with a fudge factor
            // of 2*BLOCK_SIZE)

            // update epochs
            CRITICAL_SECTION(cs, _flush_lock);
            w_assert3(_old_epoch.start == _old_epoch.end);
            _old_epoch = _cur_epoch;
            _cur_epoch = epoch(info->lsn, info->new_base, 0, info->new_end);
        }
        else if(info->pos > _segsize) {
            // wrapped buffer! update epochs
            CRITICAL_SECTION(cs, _flush_lock);
            w_assert3(_old_epoch.start == _old_epoch.end);
            _old_epoch = epoch(_cur_epoch.base_lsn, _cur_epoch.base,
                        _cur_epoch.start, segsize());
            _cur_epoch.base_lsn += segsize();
            _cur_epoch.base += segsize();
            _cur_epoch.start = 0;
            _cur_epoch.end = info->new_end;
        }
        else {
            // normal update -- no need for a lock if we just increment its end
            w_assert1(_cur_epoch.start < info->new_end);
            _cur_epoch.end = info->new_end;
        }

        // we might have to also release delegated buffer(s).
        info = _carray->grab_delegated_expose(info);
    }

    return false;
}
#endif // LOG_BUFFER

#ifdef LOG_BUFFER
rc_t log_core::insert(logrec_t &rec, lsn_t* rlsn) {
    return _log_buffer->insert(rec, rlsn);
}
#else
rc_t log_core::insert(logrec_t &rec, lsn_t* rlsn) {
    w_assert1(rec.length() <= sizeof(logrec_t));
    int32_t size = rec.length();

    /* Copy our data into the buffer and update/create epochs. Note
       that, while we may race the flush daemon to update the epoch
       record, it will not touch the buffer until after we succeed so
       there is no race with memcpy(). If we do lose an epoch update
       race, it is only because the flush changed old_epoch.begin to
       equal old_epoch.end. The mutex ensures we don't race with any
       other inserts.
    */
    lsn_t rec_lsn;
    CArraySlot* info = 0;
    long pos = 0;

    // consolidate
    carray_slotid_t idx;
    carray_status_t old_count;
    info = _carray->join_slot(size, idx, old_count);

    pos = ConsolidationArray::extract_carray_log_size(old_count);
    if(old_count == ConsolidationArray::SLOT_AVAILABLE) {
        /* First to arrive. Acquire the lock on behalf of the whole
        * group, claim the first 'size' bytes, then make the rest
        * visible to waiting threads.
        */
        _insert_lock.acquire(&info->me);

        w_assert1(info->vthis()->count > ConsolidationArray::SLOT_AVAILABLE);

        // swap out this slot and mark it busy
        _carray->replace_active_slot(idx);

        // negate the count to signal waiting threads and mark the slot busy
        old_count = lintel::unsafe::atomic_exchange<carray_status_t>(
            &info->count, ConsolidationArray::SLOT_PENDING);
        long combined_size = ConsolidationArray::extract_carray_log_size(old_count);

        // grab space for everyone in one go (releases the lock)
        _acquire_buffer_space(info, combined_size);

        // now let everyone else see it
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
        info->count = ConsolidationArray::SLOT_FINISHED - combined_size;
    }
    else {
        // Not first. Wait for the owner to tell us what's going on.
        w_assert1(old_count > ConsolidationArray::SLOT_AVAILABLE);
        _carray->wait_for_leader(info);
    }

    // insert my value
    if(!info->error) {
        rec_lsn = _copy_to_buffer(rec, pos, size, info);
    }

    // last one to leave cleans up
    carray_status_t end_count = lintel::unsafe::atomic_fetch_add<carray_status_t>(
        &info->count, size);
    end_count += size; // NOTE lintel::unsafe::atomic_fetch_add returns the value before the
    // addition. So, we need to add it here again. atomic_add_fetch desired..
    w_assert3(end_count <= ConsolidationArray::SLOT_FINISHED);
    if(end_count == ConsolidationArray::SLOT_FINISHED) {
        if(!info->error) {
            _update_epochs(info);
        }
    }

    if(info->error) {
        return RC(info->error);
    }

    if(rlsn) {
        *rlsn = rec_lsn;
    }
    DBGOUT3(<< " insert @ lsn: " << rec_lsn << " type " << rec.type() << " length " << rec.length() );

    ADD_TSTAT(log_bytes_generated,size);
    return RCOK;
}
#endif // LOG_BUFFER


#ifdef LOG_BUFFER
rc_t log_core::flush(const lsn_t &to_lsn, bool block, bool signal, bool *ret_flushed)
{
    return _log_buffer->flush(to_lsn, block, signal, ret_flushed);
}
#else
// Return when we know that the given lsn is durable. Wait for the
// log flush daemon to ensure that it's durable.
rc_t log_core::flush(const lsn_t &to_lsn, bool block, bool signal, bool *ret_flushed)
{
    DBGOUT3(<< " flush @ to_lsn: " << to_lsn);



    w_assert1(signal || !block); // signal=false can be used only when block=false
    ASSERT_FITS_IN_POINTER(lsn_t);
    // else our reads to _durable_lsn would be unsafe

    // don't try to flush past end of log -- we might wait forever...
    lsn_t lsn = std::min(to_lsn, (*&_curr_lsn)+ -1);

    // already durable?
    if(lsn >= *&_durable_lsn) {
        if (!block) {
            *&_waiting_for_flush = true;
            if (signal) {
                DO_PTHREAD(pthread_cond_signal(&_flush_cond));
            }
            if (ret_flushed) *ret_flushed = false; // not yet flushed
        }  else {
            CRITICAL_SECTION(cs, _wait_flush_lock);
            while(lsn >= *&_durable_lsn) {
                *&_waiting_for_flush = true;
                // Use signal since the only thread that should be waiting
                // on the _flush_cond is the log flush daemon.
                DO_PTHREAD(pthread_cond_signal(&_flush_cond));
                DO_PTHREAD(pthread_cond_wait(&_wait_cond, &_wait_flush_lock));
            }
            if (ret_flushed) *ret_flushed = true;// now flushed!
        }
    } else {
        INC_TSTAT(log_dup_sync_cnt);
        if (ret_flushed) *ret_flushed = true; // already flushed
    }
    return RCOK;
}
#endif // LOG_BUFFER

#ifdef LOG_BUFFER
// see logbuf_core::flush_daemon()
#else
/**\brief Log-flush daemon driver.
 * \details
 * This method handles the wait/block of the daemon thread,
 * and when awake, calls its main-work method, flush_daemon_work.
 */
void log_core::flush_daemon()
{
    /* Algorithm: attempt to flush non-durable portion of the buffer.
     * If we empty out the buffer, block until either enough
       bytes get written or a thread specifically requests a flush.
     */
    lsn_t last_completed_flush_lsn;
    bool success = false;
    while(1) {

        // wait for a kick. Kicks come at regular intervals from
        // inserts, but also at arbitrary times when threads request a
        // flush.
        {
            CRITICAL_SECTION(cs, _wait_flush_lock);
            if(success && (*&_waiting_for_space || *&_waiting_for_flush)) {
                _waiting_for_flush = _waiting_for_space = false;
                DO_PTHREAD(pthread_cond_broadcast(&_wait_cond));
                // wake up anyone waiting on log flush
            }
            if(_shutting_down) {
                _shutting_down = false;
                break;
            }

            // sleep. We don't care if we get a spurious wakeup
            if(!success && !*&_waiting_for_space && !*&_waiting_for_flush) {
                // Use signal since the only thread that should be waiting
                // on the _flush_cond is the log flush daemon.
                DO_PTHREAD(pthread_cond_wait(&_flush_cond, &_wait_flush_lock));
            }
        }

        // flush all records later than last_completed_flush_lsn
        // and return the resulting last durable lsn
        lsn_t lsn = flush_daemon_work(last_completed_flush_lsn);

        // success=true if we wrote anything
        success = (lsn != last_completed_flush_lsn);
        last_completed_flush_lsn = lsn;
    }

    // make sure the buffer is completely empty before leaving...
    for(lsn_t lsn;
        (lsn=flush_daemon_work(last_completed_flush_lsn)) !=
                last_completed_flush_lsn;
        last_completed_flush_lsn=lsn) ;
}

/**\brief Flush unflushed-portion of log buffer.
 * @param[in] old_mark Durable lsn from last flush. Flush records later than this.
 * \details
 * This is the guts of the log daemon.
 *
 * Flush the log buffer of any log records later than \a old_mark. The
 * argument indicates what is already durable and these log records must
 * not be duplicated on the disk.
 *
 * Called by the log flush daemon.
 * Protection from duplicate flushing is handled by the fact that we have
 * only one log flush daemon.
 * \return Latest durable lsn resulting from this flush
 *
 */
lsn_t log_core::flush_daemon_work(lsn_t old_mark)
{
    lsn_t base_lsn_before, base_lsn_after;
    long base, start1, end1, start2, end2;
    {
        CRITICAL_SECTION(cs, _flush_lock);
        base_lsn_before = _old_epoch.base_lsn;
        base_lsn_after = _cur_epoch.base_lsn;
        base = _cur_epoch.base;

        // The old_epoch is valid (needs flushing) iff its end > start.
        // The old_epoch is valid id two cases, both when
        // insert wrapped thelog buffer
        // 1) by doing so reached the end of the partition,
        //     In this case, the old epoch might not be an entire
        //     even segment size
        // 2) still room in the partition
        //     In this case, the old epoch is exactly 1 segment in size.

        if(_old_epoch.start == _old_epoch.end) {
            // no wrap -- flush only the new
            w_assert1(_cur_epoch.end >= _cur_epoch.start);
            start2 = _cur_epoch.start;
            end2 = _cur_epoch.end;
            w_assert1(end2 >= start2);
            // false alarm?
            if(start2 == end2) {
                return old_mark;
            }
            _cur_epoch.start = end2;

            start1 = start2; // fake start1 so the start_lsn calc below works
            end1 = start2;

            base_lsn_before = base_lsn_after;
        }
        else if(base_lsn_before.file() == base_lsn_after.file()) {
            // wrapped within partition -- flush both
            start2 = _cur_epoch.start;
            // race here with insert setting _curr_epoch.end, but
            // it won't matter. Since insert already did the memcpy,
            // we are safe and can flush the entire amount.
            end2 = _cur_epoch.end;
            _cur_epoch.start = end2;

            start1 = _old_epoch.start;
            end1 = _old_epoch.end;
            _old_epoch.start = end1;

            w_assert1(base_lsn_before + segsize() == base_lsn_after);
        }
        else {
            // new partition -- flushing only the old since the
            // two epochs target different files. Let the next
            // flush handle the new epoch.
            start2 = 0;
            end2 = 0; // don't fake end2 because end_lsn needs to see '0'

            start1 = _old_epoch.start;
            end1 = _old_epoch.end;

            // Mark the old epoch has no longer valid.
            _old_epoch.start = end1;

            w_assert1(base_lsn_before.file()+1 == base_lsn_after.file());
        }
    } // end critical section

    lsn_t start_lsn = base_lsn_before + start1;
    lsn_t end_lsn   = base_lsn_after + end2;
    long  new_start = base + end2;
    {
        // Avoid interference with compensations.
        CRITICAL_SECTION(cs, _comp_lock);
        _flush_lsn = end_lsn;
    }

    w_assert1(end1 >= start1);
    w_assert1(end2 >= start2);
    w_assert1(end_lsn == first_lsn(start_lsn.hi()+1)
          || end_lsn.lo() - start_lsn.lo() == (end1-start1) + (end2-start2));

    // start_lsn.file() determines partition # and whether _flushX
    // will open a new partition into which to flush.
    // That, in turn, is determined by whether the _old_epoch.base_lsn.file()
    // matches the _cur_epoch.base_lsn.file()
    _flushX(start_lsn, start1, end1, start2, end2);

    _durable_lsn = end_lsn;
    _start = new_start;

    return end_lsn;
}
#endif // LOG_BUFFER


#ifdef LOG_BUFFER
rc_t log_core::compensate(const lsn_t& orig_lsn, const lsn_t& undo_lsn)
{
    return _log_buffer->compensate(orig_lsn, undo_lsn);
}
#else
// Find the log record at orig_lsn and turn it into a compensation
// back to undo_lsn
rc_t log_core::compensate(const lsn_t& orig_lsn, const lsn_t& undo_lsn)
{
    // somewhere in the calling code, we didn't actually log anything.
    // so this would be a compensate to myself.  i.e. a no-op
    if(orig_lsn == undo_lsn)
        return RCOK;

    // FRJ: this assertion wasn't there originally, but I don't see
    // how the situation could possibly be correct
    w_assert1(orig_lsn <= _curr_lsn);

    // no need to grab a mutex if it's too late
    if(orig_lsn < _flush_lsn)
    {
        DBGOUT3( << "log_core::compensate - orig_lsn: " << orig_lsn
                 << ", flush_lsn: " << _flush_lsn << ", undo_lsn: " << undo_lsn);
        return RC(eBADCOMPENSATION);
    }

    CRITICAL_SECTION(cs, _comp_lock);
    // check again; did we just miss it?
    lsn_t flsn = _flush_lsn;
    if(orig_lsn < flsn)
        return RC(eBADCOMPENSATION);

    /* where does it live? the buffer is always aligned with a
       buffer-sized chunk of the partition, so all we need to do is
       take a modulus of the lsn to get its buffer position. It may be
       wrapped but we know it's valid because we're less than a buffer
       size from the current _flush_lsn -- nothing newer can be
       overwritten until we release the mutex.
     */
    long pos = orig_lsn.lo() % segsize();
    if(pos >= segsize() - logrec_t::hdr_non_ssx_sz)
        return RC(eBADCOMPENSATION); // split record. forget it.

    // aligned?
    w_assert1((pos & 0x7) == 0);

    // grab the record and make sure it's valid
    logrec_t* s = (logrec_t*) &_buf[pos];

    // valid length?
    w_assert1((s->length() & 0x7) == 0);

    // split after the log header? don't mess with it
    if(pos + long(s->length()) > segsize())
        return RC(eBADCOMPENSATION);

    lsn_t lsn_ck = s->get_lsn_ck();
    if(lsn_ck != orig_lsn) {
        // this is a pretty rare occurrence, and I haven't been able
        // to figure out whether it's actually a bug
        cerr << endl << "lsn_ck = "<<lsn_ck.hi()<<"."<<lsn_ck.lo()<<", orig_lsn = "<<orig_lsn.hi()<<"."<<orig_lsn.lo()<<endl
            << __LINE__ << " " __FILE__ << " "
            << "log rec is  " << *s << endl;
                return RC(eBADCOMPENSATION);
    }
    if (!s->is_single_sys_xct()) {
        w_assert1(s->xid_prev() == lsn_t::null || s->xid_prev() >= undo_lsn);

        if(s->is_undoable_clr())
            return RC(eBADCOMPENSATION);

        // success!
        DBGTHRD(<<"COMPENSATING LOG RECORD " << undo_lsn << " : " << *s);
        s->set_clr(undo_lsn);
    }
    return RCOK;
}
#endif // LOG_BUFFER


int
log_core::get_last_lsns(lsn_t *array)
{
    int j=0;
    for(int i=0; i < PARTITION_COUNT; i++) {
        const partition_t *p = this->_partition(i);
        DBGTHRD(<<"last skip lsn for " << p->num()
                                       << " " << p->last_skip_lsn());
        if(p->num() > 0 && (p->last_skip_lsn().hi() == p->num())) {
            array[j++] = p->last_skip_lsn();
        }
    }
    return j;
}


std::deque<log_core::waiting_xct*> log_core::_log_space_waiters;

rc_t log_core::wait_for_space(fileoff_t &amt, timeout_in_ms timeout)
{
    DBG(<<"log_core::wait_for_space " << amt);
    // if they're asking too much don't even bother
    if(amt > _partition_data_size) {
        return RC(eOUTOFLOGSPACE);
    }

    // wait for a signal or 100ms, whichever is longer...
    w_assert1(amt > 0);
    struct timespec when;
    if(timeout != WAIT_FOREVER)
        sthread_t::timeout_to_timespec(timeout, when);

    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    waiting_xct* wait = new waiting_xct(&amt, &cond);
    DO_PTHREAD(pthread_mutex_lock(&_space_lock));
#ifdef LOG_BUFFER
    _log_buffer->_waiting_for_space = true;
#else
    _waiting_for_space = true;
#endif
    _log_space_waiters.push_back(wait);
    while(amt) {
        /* First time through, someone could have freed up space
           before we acquired this mutex. 2+ times through, maybe our
           previous rounds got us enough that the normal log
           reservation can supply what we still need.
         */
        if(reserve_space(amt)) {
            amt = 0;

            // nullify our entry. Non-racy beause amt > 0 and we hold the mutex
            wait->needed = 0;

            // clean up in case it's pure false alarms
            while(_log_space_waiters.size() && ! _log_space_waiters.back()->needed) {
                delete _log_space_waiters.back();
                _log_space_waiters.pop_back();
            }
            break;
        }
        DBGOUT3(<< "chkpt 3");

        if(smlevel_1::chkpt != NULL) smlevel_1::chkpt->wakeup_and_take();
        if(timeout == WAIT_FOREVER) {
            cerr<<
            "* - * - * tid "<<xct()->tid().get_hi()<<"."<<xct()->tid().get_lo()<<" waiting forever for "<<amt<<" bytes of log" <<endl;
            DO_PTHREAD(pthread_cond_wait(&cond, &_space_lock));
        } else {
            cerr<<
                "* - * - * tid "<<xct()->tid().get_hi()<<"."<<xct()->tid().get_lo()<<" waiting with timeout for "<<amt<<" bytes of log"<<endl;
                int err = pthread_cond_timedwait(&cond, &_space_lock, &when);
                if(err == ETIMEDOUT)
                break;
        }
    }
    cerr<<"* - * - * tid "<<xct()->tid().get_hi()<<"."<<xct()->tid().get_lo()<<" done waiting ("<<amt<<" bytes still needed)" <<endl;

    DO_PTHREAD(pthread_mutex_unlock(&_space_lock));
    return amt? RC(stTIMEOUT) : RCOK;
}

void log_core::release_space(fileoff_t amt)
{
    DBG(<<"log_core::release_space " << amt);
    w_assert1(amt >= 0);
    /* NOTE: The use of _waiting_for_space is purposefully racy
       because we don't want to pay the cost of a mutex for every
       space release (which should happen every transaction
       commit...). Instead waiters use a timeout in case they fall
       through the cracks.

       Waiting transactions are served in FIFO order; those which time
       out set their need to -1 leave it for release_space to clean
       it up.
     */
#ifdef LOG_BUFFER
    if(_log_buffer->_waiting_for_space) {
#else
    if(_waiting_for_space) {
#endif
        DO_PTHREAD(pthread_mutex_lock(&_space_lock));
        while(amt > 0 && _log_space_waiters.size()) {
            bool finished_one = false;
            waiting_xct* wx = _log_space_waiters.front();
            if( ! wx->needed) {
                finished_one = true;
            }
            else {
                fileoff_t can_give = std::min(amt, *wx->needed);
                *wx->needed -= can_give;
                amt -= can_give;
                if(! *wx->needed) {
                    DO_PTHREAD(pthread_cond_signal(wx->cond));
                    finished_one = true;
                }
            }

            if(finished_one) {
                delete wx;
                _log_space_waiters.pop_front();
            }
        }
        if(_log_space_waiters.empty()) {
#ifdef LOG_BUFFER
            _log_buffer->_waiting_for_space = false;
#else
            _waiting_for_space = false;
#endif
        }

        DO_PTHREAD(pthread_mutex_unlock(&_space_lock));
    }

    lintel::unsafe::atomic_fetch_add<fileoff_t>(&_space_available, amt);
}

void
log_core::activate_reservations()
{
    /* With recovery complete we now activate log reservations.

       In fact, the activation should be as simple as setting the mode to
       t_forward_processing, but we also have to account for any space
       the log already occupies. We don't have to double-count
       anything because nothing will be undone should a crash occur at
       this point.
     */
    w_assert1(operating_mode == t_forward_processing);
    // FRJ: not true if any logging occurred during recovery
    // w_assert1(PARTITION_COUNT*_partition_data_size ==
    //       _space_available + _space_rsvd_for_chkpt);
    w_assert1(!_reservations_active);

    // knock off space used by full partitions
    long oldest_pnum = _min_chkpt_rec_lsn.hi();
    long newest_pnum = curr_lsn().hi();
    long full_partitions = newest_pnum - oldest_pnum; // can be zero
    _space_available -= recoverable_space(full_partitions);

    // and knock off the space used so far in the current partition
    _space_available -= curr_lsn().lo();
    _reservations_active = true;
    // NOTE: _reservations_active does not get checked in the
    // methods that reserve or release space, so reservations *CAN*
    // happen during recovery.

    // not mt-safe
    errlog->clog << info_prio
        << "Activating reservations: # full partitions "
            << full_partitions
            << ", space available " << space_left()
        << endl
            << ", oldest partition " << oldest_pnum
            << ", newest partition " << newest_pnum
            << ", # partitions " << PARTITION_COUNT
        << endl ;
}

rc_t
log_core::file_was_archived(const char * /*file*/)
{
    // TODO: should check that this is the oldest,
    // and that we indeed asked for it to be archived.
    _space_available += recoverable_space(1);
    return RCOK;
}
