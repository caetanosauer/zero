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
#include "log_lsn_tracker.h"

// chkpt.h needed to kick checkpoint thread
#include "chkpt.h"
#include "bf_tree.h"

#include "fixable_page_h.h"

#include <sstream>
#include <w_strstream.h>

// needed for skip_log (TODO fix this)
#include "logdef_gen.cpp"

// LOG_BUFFER switch
#include "logbuf_common.h"

typedef smlevel_0::fileoff_t fileoff_t;

bool       log_core::_initialized = false;

// Once the log is created, this points to it. This is the
// implementation of log_m.
log_core *log_core::THE_LOG(NULL); // me

/*********************************************************************
 *
 *  log_core *log_core::new_log_m(logdir,segid,reformat,carray_active_slot_count)
 *
 *  CONSTRUCTOR.  Returns one or the other log types.
 *
 *********************************************************************/

w_rc_t
log_core::new_log_m(
    const char* path,
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

    w_assert1(strlen(path) < sizeof(_logdir));
    strcpy(_logdir, path);

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
void
log_core::_prime(int fd, fileoff_t start, lsn_t next) 
{
#ifdef LOG_BUFFER
    _log_buffer->_prime(fd, start, next);
#else
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
#endif // LOG_BUFFER
}

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

#ifdef LOG_BUFFER
rc_t
log_core::fetch(lsn_t& ll, logrec_t* &rp, lsn_t* nxt, hints_op op)
{
    FUNC(log_core::fetch);
    
    return _log_buffer->fetch(ll, rp, nxt, op);
}
#endif

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
#ifdef LOG_BUFFER
    return _log_buffer->fetch(ll, rp, nxt, forward);
#else
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

#endif // LOG_BUFFER
}

#ifndef LOG_BUFFER
class flush_daemon_thread_t : public smthread_t {
    log_core* _log;
public:
    flush_daemon_thread_t(log_core* log) : 
         smthread_t(t_regular, "flush_daemon", WAIT_NOT_USED), _log(log) { }

    virtual void run() { _log->flush_daemon(); }
};
#endif

void log_core::start_flush_daemon() 
{
#ifdef LOG_BUFFER
    _log_buffer->start_flush_daemon();
#else
    _flush_daemon_running = true;
    _flush_daemon->fork();
#endif
}

void log_core::shutdown() 
{ 
#ifdef LOG_BUFFER
    _log_buffer->shutdown();    
#else
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
#endif
}

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

NORET
log_core::log_core(
                   long bsize, // segment size for the log buffer, set through "sm_logbufsize"
                   bool reformat,
                   int carray_active_slot_count
#ifdef LOG_BUFFER
                   ,
                   int logbuf_seg_count,
                   int logbuf_flush_trigger,
                   int logbuf_block_size
#endif
                   )

    : 
      _reservations_active(false), 
#ifndef LOG_BUFFER
      _waiting_for_space(false),
      _waiting_for_flush(false),
      _start(0), 
      _end(0),
#endif
      _segsize(_ceil(bsize, SEGMENT_SIZE)), // actual segment size for the log buffer,
#ifndef LOG_BUFFER
      _buf(NULL),
      _shutting_down(false),
      _flush_daemon_running(false),
      _carray(new ConsolidationArray(carray_active_slot_count)),
#endif
      _log_corruption(false),
      _curr_index(-1),
      _curr_num(1),
      _readbuf(NULL),
#ifdef LOG_DIRECT_IO
      _writebuf(NULL),
#endif
#ifndef LOG_BUFFER
      _min_chkpt_rec_lsn(first_lsn(1)), 
      _space_available(0),
      _space_rsvd_for_chkpt(0), 
      _partition_size(0), 
      _partition_data_size(0)
#endif
    _skip_log(NULL)
{
    FUNC(log_core::log_core);

    _skip_log = new skip_log;

    DO_PTHREAD(pthread_mutex_init(&_scavenge_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_scavenge_cond, NULL));
#ifndef LOG_BUFFER
    DO_PTHREAD(pthread_mutex_init(&_wait_flush_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_wait_cond, NULL));
    DO_PTHREAD(pthread_cond_init(&_flush_cond, NULL));
    DO_PTHREAD(pthread_mutex_init(&_space_lock, 0));
    DO_PTHREAD(pthread_cond_init(&_space_cond, 0));
#endif 

#ifdef LOG_BUFFER
    // create the log buffer
    // _log_buffer->_partition_data_size is not set at this moment
    _log_buffer = new logbuf_core(logbuf_seg_count, logbuf_flush_trigger, logbuf_block_size, _segsize,
                              0, carray_active_slot_count); 
    _log_buffer->logbuf_set_owner(this);

#else

#ifdef LOG_DIRECT_IO
    posix_memalign((void**)&_buf, LOG_DIO_ALIGN, _segsize);    
#else
    _buf = new char[_segsize];
#endif

#endif // LOG_BUFFER


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

#ifdef LOG_BUFFER
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

#else // LOG_BUFFER

    /* Create thread o flush the log */
    _flush_daemon = new flush_daemon_thread_t(this);

    _oldest_lsn_tracker = new PoorMansOldestLsnTracker(1 << 20);
    w_assert1(_oldest_lsn_tracker);

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

#endif // LOG_BUFFER

    initialize_storage(reformat);
}


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

#ifdef LOG_BUFFER
        delete _log_buffer;
#else
        delete _carray;
#ifdef LOG_DIRECT_IO
        free(_buf);
#else
        delete [] _buf;
#endif
        _buf = NULL;

        delete _oldest_lsn_tracker;
        _oldest_lsn_tracker = NULL;

        DO_PTHREAD(pthread_mutex_destroy(&_wait_flush_lock));
        DO_PTHREAD(pthread_cond_destroy(&_wait_cond));
        DO_PTHREAD(pthread_cond_destroy(&_flush_cond));
#endif // LOG_BUFFER
        DO_PTHREAD(pthread_mutex_destroy(&_scavenge_lock));
        DO_PTHREAD(pthread_cond_destroy(&_scavenge_cond));
        THE_LOG = NULL;
    }
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

rc_t log_core::insert(logrec_t &rec, lsn_t* rlsn)
{
#ifdef LOG_BUFFER
    return _log_buffer->insert(rec, rlsn);
#else
    // If log corruption is turned on,  zero out
    // important parts of the log to fake crash (by making the
    // log appear to end here).
    if (_log_corruption) {
        smlevel_0::errlog->clog << error_prio 
        << "Generating corrupt log record at lsn: " << curr_lsn() << flushl;
        rec.corrupt();
        // Now turn it off.
        _log_corruption = false;
    }

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
#endif // LOG_BUFFER
}


// Return when we know that the given lsn is durable. Wait for the
// log flush daemon to ensure that it's durable.
rc_t log_core::flush(const lsn_t &to_lsn, bool block, bool signal, bool *ret_flushed)
{
#ifdef LOG_BUFFER
    return _log_buffer->flush(to_lsn, block, signal, ret_flushed);
#else
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
#endif // LOG_BUFFER
}

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

// Find the log record at orig_lsn and turn it into a compensation
// back to undo_lsn
rc_t log_core::compensate(const lsn_t& orig_lsn, const lsn_t& undo_lsn) 
{
#ifdef LOG_BUFFER
    return _log_buffer->compensate(orig_lsn, undo_lsn);
#else
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
#endif // LOG_BUFFER
}
