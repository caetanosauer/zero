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

/*<std-header orig-src='shore' incl-file-exclusion='SRV_LOG_H'>

 $Id: log_core.h,v 1.11 2010/09/21 14:26:19 nhall Exp $

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

#ifndef LOG_CORE_H
#define LOG_CORE_H
#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <AtomicCounter.hpp>
#include <vector> // only for _collect_single_page_recovery_logs()

// in sm_base for the purpose of log callback function argument type
class      partition_t ; // forward

class sm_options;
class ConsolidationArray;
struct CArraySlot;
class PoorMansOldestLsnTracker;
class plog_xct_t;
class ticker_thread_t;
class fetch_buffer_loader_t;

#include <partition.h>
#include "mcs_lock.h"
#include "tatas.h"
#include "log_storage.h"

class log_core
{
    // CArray interface needs to be exposed to plog_xct_t to implement
    // atomic commit protocol.
    friend class plog_xct_t;
public:
    log_core(const sm_options&);
    virtual           ~log_core();

    rc_t init();

    static const std::string IMPL_NAME;

    rc_t            insert(logrec_t &r, lsn_t* l = NULL);
    rc_t            flush(const lsn_t &lsn, bool block=true, bool signal=true, bool *ret_flushed=NULL);
    rc_t    flush_all(bool block=true) {
                          return flush(curr_lsn().advance(-1), block); }
    rc_t            compensate(const lsn_t &orig_lsn, const lsn_t& undo_lsn);
    rc_t            fetch(lsn_t &lsn, logrec_t* &rec, lsn_t* nxt, const bool forward);
    void            shutdown();
    rc_t            truncate();

    lsn_t curr_lsn() const { return _curr_lsn; }

    lsn_t durable_lsn() const { return _durable_lsn; }

    void start_flush_daemon()
    {
        _flush_daemon_running = true;
        _flush_daemon->fork();
    }

    long                 segsize() const { return _segsize; }

    void            flush_daemon();

    lsn_t           flush_daemon_work(lsn_t old_mark);

    rc_t load_fetch_buffers();
    void discard_fetch_buffers();

    // DO NOT MAKE SEGMENT_SIZE smaller than 3 pages!  Since we need to
    // fit at least a single max-sized log record in a segment.
    // It would make no sense whatsoever to make it that small.
    // TODO: we need a better way to parameterize this; if a page
    // is large, we don't necessarily want to force writes to be
    // large; but we do need to make the segment size some reasonable
    // number of pages. If pages are 32K, then 128 blocks is only
    // four pages, which will accommodate all log records .
    //
    // NOTE: we have to fit two checkpoints into a segment, and
    // the checkpoint size is a function of the number of buffers in
    // the buffer pool among other things; so a maximum-sized checkpoint
    // is pretty big and the smaller the page size, the bigger it is.
    // 128 pages is 32 32-K pages, which is room enough for
    // 10+ max-sized log records.
#if SM_PAGESIZE < 8192
    enum { SEGMENT_SIZE= 256 * log_storage::BLOCK_SIZE };
#else
    enum { SEGMENT_SIZE= 128 * log_storage::BLOCK_SIZE };
#endif

    // Functions delegated to log_storage (CS TODO)
    string make_log_name(uint32_t p)
    {
        return _storage->make_log_name(p);
    }

    void release()
    {
        _storage->release_partition_lock();
    }

    PoorMansOldestLsnTracker* get_oldest_lsn_tracker()
    {
        return _oldest_lsn_tracker;
    }

    static lsn_t first_lsn(uint32_t pnum) { return lsn_t(pnum, 0); }

protected:

    char*                _buf; // log buffer: _segsize buffer into which
                         // inserts copy log records with log_core::insert

    /** Buffers for fetch operation -- used during log analysis and
     * single-page redo. One buffer is used for each partition.
     * The number of partitions is specified by sm_log_fetch_buf_partitions */
    vector<char*> _fetch_buffers;
    uint32_t _fetch_buf_first;
    uint32_t _fetch_buf_last;
    lsn_t _fetch_buf_begin;
    lsn_t _fetch_buf_end;
    fetch_buffer_loader_t* _fetch_buf_loader;

    char*           _readbuf;
    char *          readbuf() { return _readbuf; }

    ticker_thread_t* _ticker;

    lsn_t           _curr_lsn;
    lsn_t           _durable_lsn;

    // Set of pointers into _buf (circular log buffer)
    // and associated lsns. See detailed comments at log_core::insert
    struct epoch {
        lsn_t base_lsn; // lsn of _buf[0] for this epoch

        long base; // absolute position of _buf[0] (absolute meaning
                   // relative to the beginning of log.1)
        long start; // offset from _buf[0] of this epoch
        long end;  // offset into log buffers _buf[0] of tail of
                   // log. Wraps modulo log buffer size, aka segsize.
        epoch()
            : base_lsn(lsn_t::null), base(0), start(0), end(0)
        {
        }
        epoch(lsn_t l, long b, long s, long e)
            : base_lsn(l), base(b), start(s), end(e)
        {
            w_assert1(e >= s);
        }
        epoch volatile* vthis() { return this; }
    };


    /**
     * \ingroup CARRAY
     *  @{
     */
    epoch                _buf_epoch;
    epoch                _cur_epoch;
    epoch                _old_epoch;

    void _acquire_buffer_space(CArraySlot* info, long size);
    lsn_t _copy_to_buffer(logrec_t &rec, long pos, long size, CArraySlot* info);
    bool _update_epochs(CArraySlot* info);
    rc_t _join_carray(CArraySlot*& info, long& pos, int32_t size);
    rc_t _leave_carray(CArraySlot* info, int32_t size);
    void _copy_raw(CArraySlot* info, long& pos, const char* data, size_t size);
    /** @}*/

    log_storage*    _storage;
    PoorMansOldestLsnTracker* _oldest_lsn_tracker;

    enum { invalid_fhdl = -1 };

    long _start; // byte number of oldest unwritten byte
    long                 start_byte() const { return _start; }

    long _end; // byte number of insertion point
    long                 end_byte() const { return _end; }

    long _segsize; // log buffer size

    lsn_t                _flush_lsn;

    /** \ingroup CARRAY */

    /*
     * See src/internals.h, section LOG_M_INTERNAL
    Divisions:

    Physical layout:

    The log consists of an unbounded number of "partitions" each
    consisting of a fixed number of "segments." A partition is the
    largest file that will be created and a segment is the size of the
    in-memory buffer. Segments are further divided into "blocks" which
    are the unit of I/O.

    Threads insert "entries" into the log (log records).

    One or more entries make up an "epoch" (data that will be flushed
    using a single I/O). Epochs normally end at the end of a segment.
    The log flush daemon constantly flushes any unflushed portion of
    "valid" epochs. (An epoch is valid if its end > start.)
    When an epoch reaches the end of a segment, the final log entry
    will usually spill over into the next segment and the next
    entry will begin a new epoch at a non-zero
    offset of the new segment. However, a log entry which would spill
    over into a new partition will begin a new epoch and join it.
    Log records do not span partitions.
    */

    /* FRJ: Partitions are not protected by either the insert or flush
       mutex, but are instead managed separately using a combination
       of mutex and reference counts. We do this because read
       operations (e.g. fetch) need not impact either inserts or
       flushes because (by definition) we read only already-written
       data, which insert/flush never touches.

       Any time we change which file a partition_t points at (via open
       or close), we must acquire the partition mutex. Each call to
       open() increments a reference count which will be decremented
       by a matching call to close(). Once a partition is open threads
       may safely use it without the mutex because it will not be
       closed until the ref count goes to zero. In particular, log
       inserts do *not* acquire the partition mutex unless they need
       to change the curr_partition.

       A thread should always acquire the partition mutex last. This
       should happen naturally, since log_m acquires insert/flush
       mutexen and srv_log acquires the partition mutex.
     */

    /** @cond */ char    _padding[CACHELINE_SIZE]; /** @endcond */
    tatas_lock           _flush_lock;
    /** @cond */ char    _padding2[CACHELINE_TATAS_PADDING]; /** @endcond */
    tatas_lock           _comp_lock;
    /** @cond */ char    _padding3[CACHELINE_TATAS_PADDING]; /** @endcond */
    /** Lock to protect threads acquiring their log buffer. */
    mcs_lock             _insert_lock;
    /** @cond */ char    _padding4[CACHELINE_MCS_PADDING]; /** @endcond */

    // paired with _wait_cond, _flush_cond
    pthread_mutex_t      _wait_flush_lock;
    pthread_cond_t       _wait_cond;  // paired with _wait_flush_lock
    pthread_cond_t       _flush_cond;  // paird with _wait_flush_lock

    bool _waiting_for_flush; // protected by log_m::_wait_flush_lock

    sthread_t*           _flush_daemon;
    /// @todo both of the below should become std::atomic_flag's at some time
    lintel::Atomic<bool> _shutting_down;
    lintel::Atomic<bool> _flush_daemon_running; // for asserts only

    /**
     * Consolidation array for this log manager.
     * \ingroup CARRAY
     */
    ConsolidationArray*  _carray;

}; // log_core


/**
 * \brief Log-scan iterator
 * \ingroup SSMLOG
 * \details
 * Used in restart to scan the log.
 */
class log_i {
public:
    /// start a scan of the given log a the given log sequence number.
    NORET                        log_i(log_core& l, const lsn_t& lsn, const bool forward = true) ;
    NORET                        ~log_i();

    /// Get the next log record for transaction, put its sequence number in argument \a lsn
    bool                         xct_next(lsn_t& lsn, logrec_t*& r);
    bool                         xct_next(lsn_t& lsn, logrec_t& r);

    /// Get the return code from the last next() call.
    w_rc_t&                      get_last_rc();
private:
    log_core&                       log;
    lsn_t                        cursor;
    w_rc_t                       last_rc;
    bool                         forward_scan;
}; // log_i

inline NORET
log_i::log_i(log_core& l, const lsn_t& lsn, const bool forward)  // Default: true for forward scan
    : log(l), cursor(lsn), forward_scan(forward)
{ }

inline
log_i::~log_i()
{ last_rc.verify(); }

inline w_rc_t&
log_i::get_last_rc()
{ return last_rc; }
/*<std-footer incl-file-exclusion='LOG_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
