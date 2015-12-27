/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"
#include "w_base.h"
#include "w_error.h"
#include "w_debug.h"
#include "w_list.h"
#include "w_findprime.h"


#include "logbuf_common.h"

// in order to include log_core.h
// start
#define SM_SOURCE
#include "sm_base.h"
#include "logdef_gen.cpp"
#include "log.h"
// end

// in order to include critical_section.h
// start
#include "sm_base.h"
#include "critical_section.h"
// end

#include "tatas.h"

#include "logbuf_core.h"
#include "logbuf_seg.h"

#include "sm_options.h"
#include <pthread.h>
#include <memory.h>
#include <AtomicCounter.hpp>
#include "log_carray.h"
#include "log_lsn_tracker.h"

#include "sdisk.h"

const std::string logbuf_core::IMPL_NAME = "logbuf";

/*********************************************************************
 *
 *  logbuf_core::logbuf_core(uint32_t count, uint32_t flush_trigger, uint32_t block_size,
 *                 uint32_t seg_size, uint32_t part_size, int active_slot_count)
 *
 *  Initialize the log buffer
 *
 *  NOTE: _partition_data_size will be updated with the actual value in
 *  log_core::set_partition_data_size()
 *
 *********************************************************************/
logbuf_core::logbuf_core(const sm_options& options)
//                   const char* path,
//                   bool reformat,
//                 uint32_t count, // IN: max number of segments in the log buffer
//                 uint32_t flush_trigger, // IN: max number of unflushed segments in the write buffer
//                                         //     before a forced flush
//                 uint32_t block_size, // IN: block size in the log partition
//                 uint32_t seg_size,  // IN: segment size
//                 uint32_t part_size, // IN: usable partition size
//                 int active_slot_count // IN: slot number in ConsolidationArray
//)
    : log_common(options),
    _to_archive_seg(NULL),
    _to_insert_seg(NULL),
    _to_flush_seg(NULL)
{
    FUNC(logbuf_core::logbuf_core);

    std::string logdir = options.get_string_option("sm_logdir", "");
    if (logdir.empty()) {
        errlog->clog << fatal_prio
            << "ERROR: sm_logdir must be set to enable logging." << flushl;
        W_FATAL(eCRASH);
    }
    const char* path = logdir.c_str();
    bool reformat = options.get_bool_option("sm_reformat_log", false);
    uint32_t count = options.get_int_option("sm_logbuf_seg_count",
            LOGBUF_SEG_COUNT);
    uint32_t flush_trigger = options.get_int_option("sm_logbuf_flush_trigger",
            LOGBUF_FLUSH_TRIGGER);
    uint32_t block_size = options.get_int_option("sm_logbuf_block_size",
            LOGBUF_BLOCK_SIZE);
    uint32_t part_size = options.get_int_option("sm_logbuf_part_size",
            0);
    DBGOUT3(<< "log buffer: constructor");

    _max_seg_count = count;
    _flush_trigger = flush_trigger;
    _block_size = block_size;
    _part_size = part_size;

    // logbuf ignores segsize set by log_common
    _segsize = options.get_int_option("sm_logbufsize", LOGBUF_SEG_SIZE);
    DBGOUT3(<< "_segsize " << _segsize);

    // 3 tail blocks
    _tail_size = 3 * _block_size;

    _actual_segsize = _segsize + _tail_size;

    _seg_count = 0;

    _seg_list = new logbuf_seg_list_t(W_LIST_ARG(logbuf_seg, _link),
            &_seg_list_lock);
    if (_seg_list == NULL) {
        ERROUT("out of memory for the log buffer");
        W_FATAL(fcOUTOFMEMORY);
    }

    // TODO: is this number of buckets good???
    int buckets = w_findprime(1024 + (count / 4));
    _hashtable = new logbuf_hashtable(buckets);
    if (_hashtable == NULL) {
        ERROUT("out of memory for the hash table");
        W_FATAL(fcOUTOFMEMORY);
    }

    // performance stats
    hits = 0;
    reads = 0;

    _storage = new log_storage(path, reformat, _curr_lsn, _durable_lsn,
            _flush_lsn, _segsize);
    if (!reformat) {
        // CS: prime was initially called when oppening a partition for append
        // during recovery -- I believe this is equivalent (TODO - verify)
        _prime(_durable_lsn);
    }

    // the log buffer (the epochs) is designed to hold log records from at most two partitions
    // so its capacity cannot exceed the partition size
    // otherwise, there could be log records from three parttitions in the buffer
    if(_max_seg_count * _segsize > _partition_data_size()) {
        errlog->clog << error_prio
                     << "Log buf seg count too big or total log size (sm_logsize) too small: "
                     << " LOGBUF_SEG_COUNT=" <<  LOGBUF_SEG_COUNT
                     << " _segsize=" << _segsize
                     << " _partition_data_size=" << _partition_data_size()
                     << " max_logsz=" << max_logsz
                     << endl;
        errlog->clog << error_prio << endl;
        fprintf(stderr, "Log buf seg count too big or total log size (sm_logsize) too small ");
        W_FATAL(eINTERNAL);
    }

    start_flush_daemon();
}

/*********************************************************************
 *
 *  logbuf_core::~logbuf_core()
 *
 *  Destroy the log buffer
 *
 *********************************************************************/
logbuf_core::~logbuf_core()
{
    if (_seg_list != NULL) {
        logbuf_seg *cur = NULL;
        while((cur=_seg_list->pop())!=NULL) {
            _remove_seg(cur);
            delete cur;
            _seg_count--;
        }

        w_assert1(_seg_count == 0);

        delete _seg_list;
        _seg_list = NULL;

    }

    if (_hashtable != NULL) {
        delete _hashtable;
        _hashtable = NULL;
    }

    delete _storage;
}

/*********************************************************************
 *
 *  void logbuf_core::logbuf_print(const char *string, int level)
 *
 *  Print current state of the log buffer (thread-safe)
 *
 *********************************************************************/
void logbuf_core::logbuf_print(const char *string, int level) {

    if (level == 3)
    {
#if W_DEBUG_LEVEL >= 3
        CRITICAL_SECTION(cs, &_logbuf_lock);

        DBGOUT3(<< " === PRINT @ " << string << " ===");

        if (_seg_list != NULL) {
            {
                DBGOUT3(<< " Log Buffer State (" << _seg_count <<
                        "/" << _max_seg_count << ")");

                DBGOUT3(<< "     _start: " << _start);
                DBGOUT3(<< "     _end: " << _end);
                DBGOUT3(<< "     _free: " << _free);

                DBGOUT3(<< "     _to_archive: " << _to_archive_lsn);
                DBGOUT3(<< "     _to_flush: " << _to_flush_lsn);
                DBGOUT3(<< "     _to_insert: " << _to_insert_lsn);

                DBGOUT3(<< "     _buf_epoch: " <<
                        _buf_epoch.base_lsn << " " <<
                        _buf_epoch.base << " " << _buf_epoch.start << " " <<
                        _buf_epoch.end);
                DBGOUT3(<< "     _cur_epoch: " <<
                        _cur_epoch.base_lsn << " " <<
                        _cur_epoch.base << " " << _cur_epoch.start << " " <<
                        _cur_epoch.end);
                DBGOUT3(<< "     _old_epoch: " <<
                        _old_epoch.base_lsn << " " <<
                        _old_epoch.base << " " << _old_epoch.start << " " <<
                        _old_epoch.end);


                DBGOUT3(<< " Log Buffer Seg List (" <<
                        _seg_list->count() << "/"
                        <<_max_seg_count << ")");

                logbuf_seg *cur = _seg_list->top();
                int i=0;

                while(cur) {
                    DBGOUT3(<< " - seg: " << cur->base_lsn);
                    cur = _seg_list->next_of(cur);
                    i++;
                }
                DBGOUT3(<< " Log Buffer Hit Rate");
                DBGOUT3(<< "     fetches: " << reads);
                DBGOUT3(<< "     hits: " << hits);
                DBGOUT3(<< "     hit rate: " << ((float)hits)/reads);


                DBGOUT3(<< " ======================");
            }
        }
        else {
            DBGOUT3(<< " _seg_list is NULL...");
        }
#endif
    }
    else {
        CRITICAL_SECTION(cs, &_logbuf_lock);

        DBGOUT0(<< " === PRINT @ " << string << " ===");

        if (_seg_list != NULL) {
            {
                DBGOUT0(<< " Log Buffer State (" << _seg_count <<
                        "/" << _max_seg_count << ")");

                DBGOUT0(<< "     _start: " << _start);
                DBGOUT0(<< "     _end: " << _end);
                DBGOUT0(<< "     _free: " << _free);

                DBGOUT0(<< "     _to_archive: " << _to_archive_lsn);
                DBGOUT0(<< "     _to_flush: " << _to_flush_lsn);
                DBGOUT0(<< "     _to_insert: " << _to_insert_lsn);

                DBGOUT0(<< "     _buf_epoch: " <<
                        _buf_epoch.base_lsn << " " <<
                        _buf_epoch.base << " " << _buf_epoch.start << " " <<
                        _buf_epoch.end);
                DBGOUT0(<< "     _cur_epoch: " <<
                        _cur_epoch.base_lsn << " " <<
                        _cur_epoch.base << " " << _cur_epoch.start << " " <<
                        _cur_epoch.end);
                DBGOUT0(<< "     _old_epoch: " <<
                        _old_epoch.base_lsn << " " <<
                        _old_epoch.base << " " << _old_epoch.start << " " <<
                        _old_epoch.end);


                DBGOUT0(<< " Log Buffer Seg List (" <<
                        _seg_list->count() << "/"
                        <<_max_seg_count << ")");

                logbuf_seg *cur = _seg_list->top();
                int i=0;

                while(cur) {
                    DBGOUT0(<< " - seg: " << cur->base_lsn);
                    cur = _seg_list->next_of(cur);
                    i++;
                }

                DBGOUT0(<< " Log Buffer Hit Rate");
                DBGOUT0(<< "     fetches: " << reads);
                DBGOUT0(<< "     hits: " << hits);
                DBGOUT0(<< "     hit rate: " << ((float)hits)/reads);

                DBGOUT0(<< " ======================");
            }
        }
        else {
            DBGOUT0(<< " _seg_list is NULL...");
        }

    }
}


/*********************************************************************
 *
 *  void logbuf_core::logbuf_print_nolock(const char *string, int level)
 *
 *  Print current state of the log buffer without holding the lock
 *
 *********************************************************************/
void logbuf_core::logbuf_print_nolock(const char *string, int level) {

    if (level == 3)
    {
#if W_DEBUG_LEVEL >= 3

        DBGOUT3(<< " === PRINT @ " << string << " ===");

        if (_seg_list != NULL) {
            {
                DBGOUT3(<< " Log Buffer State (" << _seg_count <<
                        "/" << _max_seg_count << ")");

                DBGOUT3(<< "     _start: " << _start);
                DBGOUT3(<< "     _end: " << _end);
                DBGOUT3(<< "     _free: " << _free);

                DBGOUT3(<< "     _to_archive: " << _to_archive_lsn);
                DBGOUT3(<< "     _to_flush: " << _to_flush_lsn);
                DBGOUT3(<< "     _to_insert: " << _to_insert_lsn);

                DBGOUT3(<< "     _buf_epoch: " <<
                        _buf_epoch.base_lsn << " " <<
                        _buf_epoch.base << " " << _buf_epoch.start << " " <<
                        _buf_epoch.end);
                DBGOUT3(<< "     _cur_epoch: " <<
                        _cur_epoch.base_lsn << " " <<
                        _cur_epoch.base << " " << _cur_epoch.start << " " <<
                        _cur_epoch.end);
                DBGOUT3(<< "     _old_epoch: " <<
                        _old_epoch.base_lsn << " " <<
                        _old_epoch.base << " " << _old_epoch.start << " " <<
                        _old_epoch.end);


                DBGOUT3(<< " Log Buffer Seg List (" <<
                        _seg_list->count() << "/"
                        <<_max_seg_count << ")");

                logbuf_seg *cur = _seg_list->top();
                int i=0;

                while(cur) {
                    DBGOUT3(<< " - seg: " << cur->base_lsn);
                    cur = _seg_list->next_of(cur);
                    i++;
                }

                DBGOUT3(<< " Log Buffer Hit Rate");
                DBGOUT3(<< "     fetches: " << reads);
                DBGOUT3(<< "     hits: " << hits);
                DBGOUT3(<< "     hit rate: " << ((float)hits)/reads);

                DBGOUT3(<< " ======================");
            }
        }
        else {
            DBGOUT3(<< " _seg_list is NULL...");
        }
#endif
    }
    else {

        DBGOUT0(<< " === PRINT @ " << string << " ===");

        if (_seg_list != NULL) {
            {
                DBGOUT0(<< " Log Buffer State (" << _seg_count <<
                        "/" << _max_seg_count << ")");

                DBGOUT0(<< "     _start: " << _start);
                DBGOUT0(<< "     _end: " << _end);
                DBGOUT0(<< "     _free: " << _free);

                DBGOUT0(<< "     _to_archive: " << _to_archive_lsn);
                DBGOUT0(<< "     _to_flush: " << _to_flush_lsn);
                DBGOUT0(<< "     _to_insert: " << _to_insert_lsn);

                DBGOUT0(<< "     _buf_epoch: " <<
                        _buf_epoch.base_lsn << " " <<
                        _buf_epoch.base << " " << _buf_epoch.start << " " <<
                        _buf_epoch.end);
                DBGOUT0(<< "     _cur_epoch: " <<
                        _cur_epoch.base_lsn << " " <<
                        _cur_epoch.base << " " << _cur_epoch.start << " " <<
                        _cur_epoch.end);
                DBGOUT0(<< "     _old_epoch: " <<
                        _old_epoch.base_lsn << " " <<
                        _old_epoch.base << " " << _old_epoch.start << " " <<
                        _old_epoch.end);


                DBGOUT0(<< " Log Buffer Seg List (" <<
                        _seg_list->count() << "/"
                        <<_max_seg_count << ")");

                logbuf_seg *cur = _seg_list->top();
                int i=0;

                while(cur) {
                    DBGOUT0(<< " - seg: " << cur->base_lsn);
                    cur = _seg_list->next_of(cur);
                    i++;
                }

                DBGOUT0(<< " Log Buffer Hit Rate");
                DBGOUT0(<< "     fetches: " << reads);
                DBGOUT0(<< "     hits: " << hits);
                DBGOUT0(<< "     hit rate: " << ((float)hits)/reads);

                DBGOUT0(<< " ======================");
            }
        }
        else {
            DBGOUT0(<< " _seg_list is NULL...");
        }

    }
}



// =================== prime =========================

/*********************************************************************
 *
 *  void logbuf_core::_prime(int fd, smlevel_0::fileoff_t start, lsn_t next)
 *
 *  Initialize various pointers in the log buffer based on the log
 *
 *  Modified from log_core::_prime. For M2 and M3.
 *
 *********************************************************************/
void logbuf_core::_prime(
                     lsn_t next // IN: the next available lsn to insert to
)
{
    // if next is the end of a segment/the start of a new segment,
    // we should read the current segment, instead of the new one
    //uint64_t size = next.lo() == 0 ? 0 : (next.lo()-1) % _segsize+1;
    //uint64_t base = next.lo() - size;
    //lsn_t seg_lsn = lsn_t(next.hi(), base);

    size_t size = _segsize;
#ifdef LOG_DIRECT_IO
    size = _ceil(size, _block_size);
#endif

    logbuf_seg *first_seg = NULL;

    {
        _logbuf_lock.acquire();
        first_seg = _get_new_seg_for_fetch();
        _logbuf_lock.release();
    }

    w_assert0(first_seg != NULL);

//    DBGOUT3(<< "_prime: read seg " << seg_lsn);

    // read in the valid portion (0 - size)
    //W_COERCE(me()->pread(fd, first_seg->buf, size, base));
    long prime_offset = 0;
    if (next != lsn_t::null) {
        prime_offset = _storage->prime(first_seg->buf, next, size, false);
    }

    first_seg->base_lsn = lsn_t(next.hi(), next.lo() - prime_offset);

    _insert_seg_to_list_for_insertion(first_seg);
    _insert_seg_to_hashtable_for_insertion(first_seg);

    _to_archive_seg = _to_insert_seg = _to_flush_seg = first_seg;
    _to_archive_lsn = _to_insert_lsn = _to_flush_lsn = next;

    _durable_lsn = _curr_lsn = _flush_lsn = next;


    // this is the first log partition, base = 0
    _buf_epoch = _cur_epoch = epoch(lsn_t(next.hi(),0), 0, next.lo(), next.lo());

    _start = _end = next.lo();

    _free = _segsize - prime_offset;

}


// =================== fetch (no hints) =========================


/*********************************************************************
 *
 *  rc_t logbuf_core::fetch(lsn_t& ll, logrec_t*& rp, lsn_t* nxt, const bool forward)
 *
 *  Fetch a log record from a specified lsn
 *
 *  For M2 and M3. Modified from log_core::fetch.
 *  It calls logbuf_core::_fetch instead of partition_t::read
 *
 *  For forward scan (default direction), fetch the log record at ll, and return the next lsn to fetch in nxt
 *  For backward scan, fetch the log record before ll, and return the lsn of the fetched log record in nxt
 *
 *  NOTE:
 *  right now we are still using log_core's read buffer, because the mutex is still being held during fetches
 *  the mutex is acquired through _acquire()
 *  the caller must release it when it is done with the fetched log record
 *
 *********************************************************************/
rc_t
logbuf_core::fetch(
               lsn_t& ll, // IN/OUT: the lsn we want to fetch/the lsn we actually fetched
               logrec_t*& rp, // IN/OUT: actual log record, stored in the read buffer in the log manager
               lsn_t* nxt, // OUT: the lsn we are going to use as ll in next fetch request
               const bool forward // IN: forward or backward scan
)
{
    /*
     * STEP 1: Open the partition
     */
    FUNC(log_core::fetch);

    DBGTHRD(<<"fetching lsn " << ll
        << " , _curr_lsn = " << curr_lsn()
        << " , _durable_lsn = " << durable_lsn());

#if W_DEBUG_LEVEL > 0
    _sanity_check();
#endif

    // protect against double-acquire
    _storage->acquire_partition_lock(); // caller must release it

    // it's not sufficient to flush to ll, since ll is at the *beginning* of
    // what we want to read, so force a flush when necessary
    lsn_t must_be_durable = ll + sizeof(logrec_t);
    if(must_be_durable > _durable_lsn) {
        W_DO(flush(must_be_durable));
    }
    if (forward && ll >= curr_lsn()) {
        // w_assert0(ll == curr_lsn());
        // reading the curr_lsn during recovery yields a skip log record,
        // but since there is no next partition to open, scan must stop
        // here, without handling skip below
        // exception/error should not be used for control flow (TODO)
        return RC(eEOF);
    }
    if (!forward && ll == lsn_t::null) {
        // for a backward scan, nxt pointer is set to null
        // when the first log record in the first partition is set
        return RC(eEOF);
    }

    // Find and open the partition
    partition_t* p = _storage->find_partition(ll, true, false, forward);

    /*
     * STEP 2: Read log record from the partition
     * - Read log record contents into buffer
     * - Advance LSN in ll
     * - Set next pointer accordingly
     * - If reading forward and skip was fetched, fetch again from next partition
     */
    if (false == forward) {
        // backward scan
        // we want to fetch the log record preceding lsn
        // sizeof(lsn_t) == 8
        // the lsn of a log record is stored in the last 8 byte of the record
        // i.e., the preceding 8 bytes of the current log record
        w_assert1(ll.lo()>=(int)sizeof(lsn_t));

        ll.advance(-(int)sizeof(lsn_t));

        W_COERCE(_get_lsn_for_backward_scan(ll, p));

        W_COERCE(_fetch(rp, ll, p));
        if (nxt) {
            *nxt = ll;
        }
    }
    else {
        // forward scan
        W_COERCE(_fetch(rp, ll, p));
        logrec_t        &r = *rp;
        if (r.type() == logrec_t::t_skip && r.get_lsn_ck() == ll) {
            DBGTHRD(<<"seeked to skip" << ll );
            DBGTHRD(<<"getting next partition.");
            ll = first_lsn(ll.hi() + 1);

            p = _storage->get_partition(ll.hi());
            if(!p) {
                //p = _open_partition_for_read(ll.hi(), lsn_t::null, false, false);
                p = _storage->find_partition(ll, false, false, forward);
            }

            // re-read
            W_COERCE(_fetch(rp, ll, p));
        }
        if (nxt) {
            lsn_t tmp = ll;
            *nxt = tmp.advance(r.length());
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

    DBGTHRD(<<"fetch at lsn " << ll  << " returns " << r);
#if W_DEBUG_LEVEL > 2
    _sanity_check();
#endif

    // caller must release the _partition_lock mutex
    return RCOK;
}



/*********************************************************************
 *
 *  w_rc_t logbuf_core::_fetch(logrec_t* &rec, lsn_t &lsn, partition_t *p)
 *
 *  Fetch a log record from the log buffer.
 *
 *  This is the internal read function for every fetch
 *  It looks up the hashtable for every requested lsn
 *  If miss, read in the entire segment containing the log record from the partition
 *
 *********************************************************************/
w_rc_t logbuf_core::_fetch(
                       logrec_t* &rec, // IN/OUT: the fetched log record
                       lsn_t &lsn, // IN: the lsn of the log record
                       partition_t *p // IN: which partition this log record is in
)
{

    // use the log manager's read buffer for now
    rec = (logrec_t *)_readbuf;

    reads++;

    uint64_t offset = lsn.lo() % _segsize;
    lsn_t seg_lsn = lsn_t(lsn.data() - offset);
    logbuf_seg *found = NULL;
    logrec_t *rp = NULL;


    {
        _logbuf_lock.acquire();

        found = _hashtable->lookup(seg_lsn.data());
        if (found != NULL) {
            // hit
//            DBGOUT3(<< " HIT " << found->base_lsn
//                    << " " << offset);

            hits++;
            if (rec != NULL) {
                // we don't have to worry about whether the log record is spanning
                // across two segments because if that's the case, the record
                // will be in the tail blocks

                rp = (logrec_t*)(found->buf+offset);
                if( rp->length() > sizeof(logrec_t) ||
                    rp->length() < rp->header_size() ) {
                    W_FATAL_MSG(fcINTERNAL, << "HIT: BUG? or corrupted log record");
                }
                memcpy((char *)rec, (char*)rp, rp->length());
            }
            _logbuf_lock.release();
        }
        else {
            // miss
//            DBGOUT3(<< " MISS " << seg_lsn
//                    << " " << offset);

            //logbuf_print("before MISS");

            /**
             *
             *  _get_new_seg_for_fetch() always returns a valid in-memory segment descriptor (non-NULL)
             *  and the memory buffer in the segment descriptor is always free.
             *  _get_new_seg_for_fetch() does not care about which segment/lsn we want to fetch
             *  it just returns a free in-mem segment descriptor.
             *  Then, _fetch() call read_seg to read the entire segment from the log file/partition
             *  and store it in the segment descriptor.
             *  It sets the segment descriptor's base_lsn to be the lsn we are fetching for.
             *
             */
            found = _get_new_seg_for_fetch();


            _logbuf_lock.release();

            // avoid warnings about unused parameter
            if (p) {
                // read the entire segment and 3 extra blocks as tails
                W_COERCE(p->read_seg(seg_lsn, found->buf, _actual_segsize));
            }

            {
                found->base_lsn = seg_lsn;

                if (rec != NULL) {
                    // we don't have to worry about whether the log record is spanning
                    // across two segments because if that's the case, the record
                    // will be in the tail blocks

                    rp = (logrec_t*)(found->buf+offset);
                    if( rp->length() > sizeof(logrec_t) ||
                        rp->length() < rp->header_size() ) {
                        W_FATAL_MSG(fcINTERNAL, << "MISS: BUG? or corrupted log record");
                    }
                    memcpy((char *)rec, (char*)rp, rp->length());
                }
            }

            _logbuf_lock.acquire();

            _insert_seg_for_fetch(found);

            _logbuf_lock.release();


            //logbuf_print("after MISS");
        }

    }

    return RCOK;
}

/*********************************************************************
 *
 *  w_rc_t logbuf_core::_get_lsn_for_backward_scan(lsn_t &lsn, partition_t *p)
 *
 *  Find out the lsn of the previous log record
 *
 *  NOTE: the input lsn points to the location where the lsn of the previous log record is stored
 *
 *********************************************************************/
w_rc_t logbuf_core::_get_lsn_for_backward_scan(lsn_t &lsn, // IN/OUT: the position where the lsn is stored/the actual lsn value
                                           partition_t *p
)
{

    uint64_t offset = lsn.lo() % _segsize;

    lsn_t seg_lsn = lsn_t(lsn.data() - offset);
    logbuf_seg *found = NULL;

    {
        _logbuf_lock.acquire();

        found = _hashtable->lookup(seg_lsn.data());
        if (found != NULL) {
            // hit
            lsn = *((lsn_t*)(found->buf+offset));
            _logbuf_lock.release();
        }
        else {
            // miss
            found = _get_new_seg_for_fetch();

            _logbuf_lock.release();

            // avoid warnings about unused parameter
            if (p) {
                // read the entire segment and 3 extra blocks as tails
                W_COERCE(p->read_seg(seg_lsn, found->buf, _actual_segsize));
            }

            found->base_lsn = seg_lsn;
            lsn = *((lsn_t*)(found->buf+offset));


            _logbuf_lock.acquire();
            _insert_seg_for_fetch(found);
            _logbuf_lock.release();
        }
    }

    return RCOK;

}



/*********************************************************************
 *
 *  int logbuf_core::logbuf_fetch(lsn_t lsn)
 *
 *  Fetch function for M1
 *
 *  For testing
 *  It does not return any log record
 *  It only returns the state of the buffer lookup: MISS, HIT, or INVALID lsn
 *
 *********************************************************************/
int logbuf_core::logbuf_fetch(lsn_t lsn) {
//    DBGOUT3(<< " logbuf_fetch: " << lsn.data());

    uint64_t offset = lsn.lo() % _segsize;
    lsn_t seg_lsn = lsn_t(lsn.data() - offset);
    logbuf_seg *found = NULL;

    {
        _logbuf_lock.acquire();

        if (lsn >= _to_insert_lsn) {
//            DBGOUT3(<< "BUG??? invalid lsn in fetch");
            _logbuf_lock.release();
            return -1;  // INVALID
        }

        reads++;

        found = _hashtable->lookup(seg_lsn.data());
        if (found != NULL) {
            // hit
//            DBGOUT3(<< " HIT " << found->base_lsn
//                    << " " << offset);

            _logbuf_lock.release();

            return 1; // HIT
        }
        else {
            // miss
//            DBGOUT3(<< " MISS " << seg_lsn
//                    << " " << offset);


            found = _get_new_seg_for_fetch();

            _logbuf_lock.release();

            // read
            found->base_lsn = seg_lsn;

            _logbuf_lock.acquire();

            _insert_seg_for_fetch(found);

            _logbuf_lock.release();

            return 0; // MISS
        }

    }
}


/*********************************************************************
 *
 * int logbuf_core::fetch_for_test(lsn_t& lsn, logrec_t*& rp) {
 *
 *  A fetch function for testing M2 and M3
 *
 *  It calls the actual fetch and releases the mutex properly
 *  It also returns the state of the buffer lookup: MISS, HIT, or INVALID lsn
 *
 *********************************************************************/
int logbuf_core::fetch_for_test(lsn_t& lsn, logrec_t*& rp) {

    uint64_t offset = lsn.lo() % _segsize;
    lsn_t seg_lsn = lsn_t(lsn.data() - offset);
    logbuf_seg *found = NULL;

    int ret = 0;

    // check the status of the lsn
    {
        _logbuf_lock.acquire();

        if (lsn >= _to_insert_lsn) {
            //DBGOUT3(<< "BUG??? invalid lsn in fetch");
            _logbuf_lock.release();
            ret = -1;  // INVALID
        }
        else {
            found = _hashtable->lookup(seg_lsn.data());
            if (found != NULL) {
                // hit
                // DBGOUT3(<< " HIT " << found->base_lsn
                //         << " " << offset);

                _logbuf_lock.release();

                ret =  1; // HIT
            }
            else {
                // miss
                // DBGOUT3(<< " MISS " << seg_lsn
                //         << " " << offset);

                _logbuf_lock.release();

                ret =  0; // MISS
            }
        }
    }

    // do real fetch
    rc_t rc = fetch(lsn, rp, NULL, true);
    if (rc.is_error()) {
        DBGOUT3(<< "ERROR " << rc.get_message());
    }

    // important! release the lock....
    release();


    return ret;
}

// =================== insert =========================

/*********************************************************************
 *
 *  logbuf_seg *logbuf_core::_lookup_for_compensate(lsn_t lsn)
 *
 *  Special lookup for compensate
 *
 *********************************************************************/
logbuf_seg *logbuf_core::_lookup_for_compensate(lsn_t lsn) {
    return _hashtable->lookup(lsn.data());
}

/*********************************************************************
 *
 *  rc_t logbuf_core::compensate(const lsn_t& orig_lsn, const lsn_t& undo_lsn)
 *
 *  Find the log record at orig_lsn and turn it into a compensation back to undo_lsn
 *
 *  Modified from log_core::compensate
 *
 *********************************************************************/
rc_t logbuf_core::compensate(const lsn_t& orig_lsn, const lsn_t& undo_lsn) {
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
        DBGOUT3( << "BAD log_core::compensate (already flushed) - orig_lsn: " << orig_lsn
                 << ", flush_lsn: " << _flush_lsn << ", undo_lsn: "
                 << undo_lsn);
        return RC(eBADCOMPENSATION);
    }

    CRITICAL_SECTION(cs, _comp_lock);
    // check again; did we just miss it?
    lsn_t flsn = _flush_lsn;
    if(orig_lsn < flsn) {
        DBGOUT3( << "BAD log_core::compensate (already flushed) - orig_lsn: " << orig_lsn
                 << ", flush_lsn: " << _flush_lsn << ", undo_lsn: "
                 << undo_lsn);
        return RC(eBADCOMPENSATION);
    }


    // old code

    // /* where does it live? the buffer is always aligned with a
    //    buffer-sized chunk of the partition, so all we need to do is
    //    take a modulus of the lsn to get its buffer position. It may be
    //    wrapped but we know it's valid because we're less than a buffer
    //    size from the current _flush_lsn -- nothing newer can be
    //    overwritten until we release the mutex.
    //  */
    // long pos = orig_lsn.lo() % segsize();
    // if(pos >= segsize() - logrec_t::hdr_non_ssx_sz)
    //     return RC(eBADCOMPENSATION); // split record. forget it.

    // // aligned?
    // w_assert1((pos & 0x7) == 0);

    // // grab the record and make sure it's valid
    // logrec_t* s = (logrec_t*) &_buf[pos];
    // //


    int64_t pos = orig_lsn.lo() % segsize();
    lsn_t seg_lsn = lsn_t(orig_lsn.data() - pos);
    logbuf_seg *found = NULL;

    logrec_t* s;


    if(pos >= segsize() - logrec_t::hdr_non_ssx_sz) {
        DBGOUT3( << "BAD log_core::compensate (split record) - orig_lsn: " << orig_lsn
                 << ", flush_lsn: " << _flush_lsn << ", undo_lsn: "
                 << undo_lsn);

        return RC(eBADCOMPENSATION); // split record. forget it.
    }
    // aligned?
    w_assert1((pos & 0x7) == 0);

    {
        // find the segment containing the requested log record
        found = _lookup_for_compensate(seg_lsn);
        w_assert1(found);
        s  = (logrec_t*) &found->buf[pos];

    }

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
        DBGOUT3( << "BAD log_core::compensate (bug?) - orig_lsn: " << orig_lsn
                 << ", flush_lsn: " << _flush_lsn << ", undo_lsn: "
                 << undo_lsn);

                return RC(eBADCOMPENSATION);
    }
    if (!s->is_single_sys_xct()) {
        w_assert1(s->xid_prev() == lsn_t::null || s->xid_prev() >= undo_lsn);

        if(s->is_undoable_clr()) {
            DBGOUT3( << "BAD log_core::compensate (undoable_clr) - orig_lsn: " << orig_lsn
                     << ", flush_lsn: " << _flush_lsn << ", undo_lsn: "
                     << undo_lsn);
            return RC(eBADCOMPENSATION);
        }

        // success!
        DBGTHRD(<<"COMPENSATING LOG RECORD " << undo_lsn << " : " << *s);
        s->set_clr(undo_lsn);
        // DBGOUT3( << "GOOD log_core::compensate - orig_lsn: " << orig_lsn
        //          << ", flush_lsn: " << _flush_lsn << ", undo_lsn: "
        //          << undo_lsn << ", to_insert: " << _to_insert_lsn);

    }
    return RCOK;
}


/*********************************************************************
 *
 *  void logbuf_core::_reserve_buffer_space(CArraySlot *info, long recsize)
 *
 *  Help function to reserve free space from the log buffer for the insertion
 *
 *  NOTE: the caller must hold _insert_lock
 *
 *********************************************************************/
void logbuf_core::_reserve_buffer_space(CArraySlot *info, long recsize) {

    long needed = recsize;

    // if (needed > _segsize)
    //     DBGOUT0(<< "HUGE insertion!");

    do {
        int64_t free = _free;

        // calculate the actual free space
        // new partition is the special case
        if ((_to_insert_lsn.lo() + needed) > _partition_data_size()) {
            // new partition, cannot use the remaining free space
            uint64_t offset = _to_insert_lsn.lo() - _to_insert_seg->base_lsn.lo();
            uint64_t free_in_seg = _segsize - offset;
            free -= free_in_seg;
        }
        else {
            // current partition
            // just use free
        }

        w_assert1(free>=0);

        // allocate new segments when necessary
        // handle cases where the combined insertion spans more than 1 segments
        if (free >= needed) {
            // we are good
            //DBGOUT3(<< "reserve: _free " << _free << " needed " << needed);
            break;
        }
        else {
            // we need more free space
            // this function may trigger replacement
            _get_more_space_for_insertion(info);

        }

    } while (true);

}

/*********************************************************************
 *
 *  void logbuf_core::_acquire_buffer_space(CArraySlot* info, long recsize)
 *
 *  Acquire free space from the log buffer for the insertion
 *
 *  Modifed from log_core::_acquire_buffer_space
 *  Due to the use of CArray, this recsize is the sum of a group of insertions (combined insertion)
 *  This function is called by the "leader" of the combined insertion
 *
 *  NOTE: the caller must be holding _insert_lock
 *
 *********************************************************************/
void logbuf_core::_acquire_buffer_space(CArraySlot* info, long recsize) {
    w_assert2(recsize > 0);


//    DBGOUT3(<< " insert: reserve buffer space! " << recsize);
    _reserve_buffer_space(info, recsize);

    lintel::atomic_thread_fence(lintel::memory_order_consume);

    /* end_byte() is the byte-offset-from-start-of-log-file
     * version of _cur_epoch.end */
    w_assert2(_buf_epoch.end % segsize() == end_byte() % segsize());

    /* _curr_lsn is the lsn of the next-to-be-inserted log record, i.e.,
     * the next byte of the log to be written
     */
    /* _cur_epoch.end is the offset into the log buffer of the _curr_lsn */
    w_assert2(_buf_epoch.end % segsize() == _curr_lsn.lo() % segsize());

    w_assert2(end_byte() >= start_byte());


    // the following is similar to the original implementation, but there are a
    // few differences:
    // - the new log buffer does not wrap, so there are only two cases:
    // insertion to the same partition or to new partition
    // - the epoch now covers one or more segments in the same partition
    // - CArraySlot (info) now stores the starting segment of this combined
    // insertion. It helps, during _copy_to_buffer, to locate the actual segment
    // for a single insertion quickly by walking the list, instead of doing
    // hashtable lookups
    // TODO: is it really faster than hashtable lookup?

    long end = _buf_epoch.end;

    long old_end = _buf_epoch.base + end;
    long new_end = end + recsize;


    lsn_t curr_lsn = _to_insert_lsn;
    lsn_t next_lsn = _buf_epoch.base_lsn + new_end;
    long new_base = -1;
    long start_pos = end % segsize();  // offset in the starting segment

    w_assert1(_to_insert_seg);
    logbuf_seg *cur_seg = _to_insert_seg;
    uint64_t offset = _to_insert_lsn.lo() - _to_insert_seg->base_lsn.lo();
    uint64_t free_in_seg = _segsize - offset;
    uint64_t needed = recsize;

    if ((_to_insert_lsn.lo() + recsize) <= _partition_data_size()) {
        // curr partition
//        DBGOUT3(<< " insert: current partition! " << recsize);

        // update _buf_epoch
        _buf_epoch.end = new_end;

        // _to_insert_lsn, _end, and _free are good
        // free_in_seg is available
    }
    else {
        // new partition
        // assuming the size of a combined insertion is << partition size
        // we don't open a new partition until more log space is required
//        DBGOUT3(<< " insert: new partition! " << recsize);

        // update _buf_epoch
        curr_lsn = lsn_t(next_lsn.hi()+1, 0);
        next_lsn = curr_lsn + recsize;

        // new epoch covers the new partition
        new_base = _buf_epoch.base + _partition_data_size();
        start_pos = 0;
        _buf_epoch = epoch(curr_lsn, new_base, 0, new_end=recsize);

        // new _to_insert_lsn and _end (before we grab the new segments)
        _to_insert_lsn = curr_lsn;

        // udpate _end
        _end = _buf_epoch.base;
        _free -= free_in_seg;

        // cannot use the free space in current segment
        free_in_seg = 0;
    }

    // special case: current seg is already full
    if (free_in_seg == 0) {
        // grab a new seg
        // we just follow the list because we have already reserved enough free
        // space (or segments) for this insertion
        cur_seg = _seg_list->next_of(cur_seg);
        w_assert1(cur_seg);

        // add the new seg to the hashtable
        cur_seg->base_lsn = _to_insert_lsn;
        _insert_seg_to_hashtable_for_insertion(cur_seg);


        _to_insert_seg = cur_seg;

        // the entire new segment is free to use!
        free_in_seg = _segsize;
    }

    // update starting seg for this combined insertion
    info->start_seg = _to_insert_seg;

    // need more segments?
    while (free_in_seg < needed) {
        _to_insert_lsn += free_in_seg;

        // udpate _end
        _end += free_in_seg;
        needed -= free_in_seg;
        _free -= free_in_seg;

        // grab a new seg
        cur_seg = _seg_list->next_of(cur_seg);
        w_assert1(cur_seg);

        cur_seg->base_lsn = _to_insert_lsn;
        _insert_seg_to_hashtable_for_insertion(cur_seg);

        _to_insert_seg = cur_seg;

        free_in_seg = _segsize;
    }

    // last seg (full or partial)
    if (needed>0) {
        _to_insert_lsn += needed;
        // udpate _end
        _end += needed;
        _free -= needed;
    }

    w_assert1(_free>=0);


    _curr_lsn = _to_insert_lsn;


    _carray->join_expose(info);


    _insert_lock.release(&info->me);

    info->lsn = curr_lsn; // where will we end up on disk?
    info->old_end = old_end; // lets us serialize with our predecessor after memcpy
    info->start_pos = start_pos; // starting position of this "combined" insertion
    info->pos = start_pos + recsize; // coordinates groups of threads sharing a log allocation
    info->new_end = new_end; // eventually assigned to _cur_epoch
    info->new_base = new_base; // positive if we started a new partition
    info->error = w_error_ok;

}


/*********************************************************************
 *
 *  void logbuf_core::_acquire_buffer_space(CArraySlot* info, long recsize)
 *
 *  Copy individual log record to the log buffer
 *
 *  Modifed from log_core::_copy_to_buffer
 *  This functions is called by individual transaction
 *
 *********************************************************************/
lsn_t logbuf_core::_copy_to_buffer(
                               logrec_t &rec, // IN: the log record in a transaction
                               long pos, // IN: the relative position of the insertion wrt
                                         //     the group / "combined" insertion
                               long recsize, // IN: the size of the log record
                               CArraySlot* info // IN: the slot of the group insertion
)
{

    /*
      do the memcpy (or two)
    */
    lsn_t rlsn = info->lsn + pos;
    rec.set_lsn_ck(rlsn);

    pos += info->start_pos;

    // find the correct segment
    logbuf_seg *cur = info->start_seg;
    while(pos >= _segsize) {
        pos -= _segsize;
        w_assert1(cur);
        cur = _seg_list->next_of(cur);
    }

    w_assert1(pos>=0);

    char const* data = (char const*) &rec;
    long spillsize = pos + recsize - _segsize;

    //DBGOUT3(<< "lsn " << cur->base_lsn << " pos " << pos << " recsize " << recsize);

    if(spillsize <= 0) {
        // normal insert (within one segment)
        memcpy(cur->buf+pos, data, recsize);
    }
    else {
        // spanning log records (across two segments)
        // partsize is the portion that stays in current segment
        // spillsize is the portion that stays in next segment

        long partsize = recsize - spillsize;

        // Copy log record to buffer

        // store the entire record in current segment + tails
        memcpy(cur->buf+pos, data, recsize);

        // store the spilled part in next segment
        cur = _seg_list->next_of(cur);
        w_assert1(cur);
        memcpy(cur->buf, data+partsize, spillsize);
    }

    return rlsn;
}


/*********************************************************************
 *
 *  bool logbuf_core::_update_epochs(CArraySlot* info)
 *
 *  Update epochs to reflect the latest insertion
 *
 *  Modifed from log_core::_update_epochs
 *  This functions is called by individual transaction
 *
 *********************************************************************/
bool logbuf_core::_update_epochs(CArraySlot* info) {
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
        // If delegated-buffer-release is off, we simply spin until predecessors
        // complete.
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
        while(*&_cur_epoch.vthis()->end + *&_cur_epoch.vthis()->base != info->old_end);
    }

    // now update the epoch(s)
    while (info != NULL) {
        w_assert1(*&_cur_epoch.vthis()->end + *&_cur_epoch.vthis()->base ==
                  info->old_end);

        if (info->new_base > 0) {
            // new partition
            CRITICAL_SECTION(cs, _flush_lock);

            // TODO: not sure if this assertion still holds
            //w_assert3(_old_epoch.start == _old_epoch.end);
            _old_epoch = _cur_epoch;
            _cur_epoch = epoch(info->lsn, info->new_base, 0, info->new_end);
        }
        else {
            // curr partition
            // update _cur_epoch
            w_assert1(_cur_epoch.start < info->new_end);
            _cur_epoch.end = info->new_end;
        }

        // we might have to also release delegated buffer(s).
        info = _carray->grab_delegated_expose(info);
    }

    return false;
}


/*********************************************************************
 *
 *  rc_t logbuf_core::insert(logrec_t &rec, lsn_t* rlsn) {
 *
 *  Insert function for M2 and M3
 *
 *  Same as log_core::insert
 *
 *********************************************************************/
rc_t logbuf_core::insert(
                     logrec_t &rec, // IN: the log record we are going to insert
                     lsn_t* rlsn // OUT: which lsn this log record ends up with
)
{

    // verify the length of the log record
    // TODO: enable it in opt build?
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
//    DBGOUT3(<< " insert @ lsn: " << rec_lsn << " type " << rec.type() << " length " << rec.length() );

    ADD_TSTAT(log_bytes_generated,size);
    return RCOK;
}



/*********************************************************************
 *
 *  logrec_t *logbuf_core::logbuf_fake_logrec(uint32_t recsize)
 *
 *  Helper function to create a fake log record for M1 testing
 *
 *********************************************************************/
logrec_t *logbuf_core::logbuf_fake_logrec(uint32_t recsize) {
    char *tmp = new char[recsize];
    memset(tmp, 0, recsize);
    ((uint16_t *)tmp)[0] = recsize;
    return (logrec_t*)tmp;
}


/*********************************************************************
 *
 *  rc_t logbuf_core::logbuf_insert(long recsize)
 *
 *  Fake insertion function for M1 testing
 *
 *********************************************************************/
rc_t logbuf_core::logbuf_insert(long recsize) {
    logrec_t *logrec;
    rc_t ret;
    lsn_t lsn;

    logrec = logbuf_fake_logrec(recsize);
    ret = insert(*logrec, &lsn);

    // remember to free this fake log record...
    delete[] (char*)logrec;

    // // return the next available lsn after this insertion
    // // just an estimation... not thread-safe
    // lsn = _to_insert_lsn;

    return ret;
}


// =================== flush =========================

/*********************************************************************
 *
 *  void logbuf_core::shutdown()
 *
 *  Shutdown the flush daemon
 *
 *  Same as log_core::shutdown()
 *
 *********************************************************************/
void logbuf_core::shutdown() {
    DBGOUT3(<< "stopping the flush daemon");

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

/*********************************************************************
 *
 *  void logbuf_core::_flushX(lsn_t start_lsn, uint64_t start, uint64_t end)
 *
 *  Flush log records starting at start_lsn
 *
 *  Modified from log_core::_flushX
 *  Called by flush_daemon_work
 *  It is now able to flush multiple segments
 *
 *********************************************************************/
void logbuf_core::_flushX(lsn_t start_lsn, uint64_t start, uint64_t end) {

    w_assert1(end >= start);

    partition_t *p = _storage->get_partition_for_flush(start_lsn, 0,0,0,0);


    logbuf_seg *cur = _to_flush_seg;
    uint64_t offset_in_seg, size_in_seg, delta, write_size, to_write;
    uint32_t seg_cnt;

    offset_in_seg = start % _segsize;

    // total number of segments we are going to flush
    seg_cnt = ( _ceil(end, _segsize) - (start - offset_in_seg) ) / _segsize;

//    DBGOUT3(<< "flushX: start_lsn " << start_lsn << " start " << start << " end " << end << " seg_cnt " << seg_cnt);


    // when _prime starts with an empty segment (offset_in_seg == 0 &&
    // _to_flush_seg->base_lsn == start_lsn), _to_flush_seg points to the
    // correct segment. But in other cases when offset_in_seg == 0, _to_flush_seg
    // points to the previous segment; we have to find the correct segment first
    if (offset_in_seg == 0 && _to_flush_seg->base_lsn < start_lsn && end > start) {
        cur = _seg_list->next_of(cur);
        w_assert1(cur);
        offset_in_seg = 0;
    }

    // rounded down (block-aligned!!!)
    delta = offset_in_seg % _block_size;
    offset_in_seg = offset_in_seg - delta;
    size_in_seg = _segsize - offset_in_seg;
    to_write = write_size = end - start + delta;

    uint64_t written;
    written = end - start;

    // allocate iovecs
    // every iovec holds a full or partial segment
    typedef sdisk_base_t::iovec_t iovec_t;

    // TODO: use a static array in partition_t or logbuf_core?
#ifdef LOG_DIRECT_IO
    // one more iovec will be filled later by p->flush
    // the last partial block of the log buffer + skip log record + paddings
    iovec_t *iov = new iovec_t[seg_cnt+1];
#else
    // two more iovecs will be filled later by p->flush
    // one for the skip record, and the other for the padding 0's
    iovec_t *iov = new iovec_t[seg_cnt+2];
#endif

    if (iov == NULL) {
        ERROUT("Out of memory when allocating iovecs");
        W_FATAL(eINTERNAL);
    }

    // multiple segments
    uint32_t i=0;

    while (to_write > size_in_seg) {
        w_assert1(cur);

//        DBGOUT3(<< "IOV " << cur->base_lsn << " " << offset_in_seg << " " <<
//                size_in_seg);

        iov[i] = iovec_t((char*)cur->buf + offset_in_seg, size_in_seg);
        to_write -= size_in_seg;
        offset_in_seg = 0;
        size_in_seg = _segsize;
        i++;

        cur = _seg_list->next_of(cur);

#ifdef LOG_DIRECT_IO
        w_assert1((long)(iov[i].iov_base)%LOG_DIO_ALIGN == 0);
        w_assert1((iov[i].iov_len)%LOG_DIO_ALIGN == 0);
#endif

    }

    // last segment (full or partial)
    if (to_write > 0) {
        w_assert1(cur);
        w_assert1(i==seg_cnt-1);

//        DBGOUT3(<< "IOV (last) " << cur->base_lsn << " " << offset_in_seg << " " <<
//                to_write);

        iov[i] = iovec_t((char*)cur->buf + offset_in_seg, to_write);

#ifdef LOG_DIRECT_IO
        w_assert1((long)(iov[i].iov_base)%LOG_DIO_ALIGN == 0);
        //w_assert1((iov[i].iov_len)%LOG_DIO_ALIGN == 0);
#endif

    }


    // finally flush all iovs
    p->flush(p->fhdl_app(), start_lsn, written, write_size, iov, seg_cnt);
    p->set_size(start_lsn.lo() + written);

    // update _to_flush_seg
    // _to_flush_seg always points to the last flushed seg
    // even when the seg has already been flushed completely
    {
        //CRITICAL_SECTION(cs, _logbuf_lock);

        _to_flush_seg = cur;
    }

    delete[] iov;

}

/*********************************************************************
 *
 *  lsn_t logbuf_core::flush_daemon_work(lsn_t old_mark)
 *
 *  Flush new log records from the log buffer
 *
 *  Modified from log_core::flush_daemon_work
 *
 *********************************************************************/
lsn_t logbuf_core::flush_daemon_work(lsn_t old_mark)
{

    lsn_t start_lsn;
    uint64_t start;
    uint64_t end;
    lsn_t end_lsn;
    long new_start; // start after flush

    {
        CRITICAL_SECTION(cs, _flush_lock);

        if (_old_epoch.start == _old_epoch.end) {
            // curr partition
            // flush
            //    _cur_epoch.base_lsn   _cur_epoch.start to _cur_epoch.end

            start_lsn = _cur_epoch.base_lsn + _cur_epoch.start;
            start = _cur_epoch.start;
            end = _cur_epoch.end;
            end_lsn = _cur_epoch.base_lsn + _cur_epoch.end;

            // nothing to flush
            if (start == end)
                return old_mark;

//            DBGOUT3(<< " flush: current partition!");

            _cur_epoch.start = end;

            new_start = _cur_epoch.base + _cur_epoch.end;
        }
        else {
            // new partition
            // flush
            //    _old_epoch.base_lsn  _old_epoch.start to _old_epoch.end
            // the portion in the new partition (_cur_epoch) will be flushed in the
            // next flush
//            DBGOUT3(<< " flush: new partition!");

            start_lsn = _old_epoch.base_lsn + _old_epoch.start;
            start = _old_epoch.start;
            end = _old_epoch.end;
            end_lsn = _cur_epoch.base_lsn;

            _old_epoch.start = end;

            new_start = _cur_epoch.base;

        }
    }

    {
        // Avoid interference with compensations.
        CRITICAL_SECTION(cs, _comp_lock);
        _flush_lsn = end_lsn;
    }

    // flush
    _flushX(start_lsn, start, end);


    // after flush is done
    // _to_flush_seg is updated in _flushX
    w_assert1(_to_flush_seg);
    _to_flush_lsn = end_lsn;
    _durable_lsn = end_lsn;
    _start = new_start;


    return end_lsn;
}


/*********************************************************************
 *
 * rc_t logbuf_core::flush(const lsn_t &to_lsn, bool block, bool signal, bool *ret_flushed)
 *
 *  Flush function for M2 and M3
 *
 *  Modified from log_core::flush
 *
 *********************************************************************/
rc_t logbuf_core::flush(
                    const lsn_t &to_lsn, // IN/OUT: flush log records upto to_lsn
                    bool block, // IN: whether this flush is blocking
                    bool signal, // IN: whether we want to signal the flush daemon to do the flush
                    bool *ret_flushed // OUT: whether the requested flush has been done
)
{
    {
//        DBGOUT3(<< " flush @ to_lsn: " << to_lsn);

        w_assert1(signal || !block); // signal=false can be used only when
                                     // block=false
        ASSERT_FITS_IN_POINTER(lsn_t);
        // else our reads to _durable_lsn would be unsafe

        // don't try to flush past end of log -- we might wait forever...
        lsn_t lsn = std::min(to_lsn, (*&_to_insert_lsn)+ -1);

        if(lsn >= *&_to_flush_lsn) {
            if (!block) {
                *&_waiting_for_flush = true;
                if (signal) {
                    DO_PTHREAD(pthread_cond_signal(&_flush_cond));
                }
                if (ret_flushed) *ret_flushed = false; // not yet flushed
            }  else {
                CRITICAL_SECTION(cs, _wait_flush_lock);
                while(lsn >= *&_to_flush_lsn) {
                    *&_waiting_for_flush = true;
                    // Use signal since the only thread that should be waiting
                    // on the _flush_cond is the log flush daemon.
                    //DBGOUT3(<< " " << "flush inside");
                    //DBGOUT3(<< " " << lsn << " " << _to_flush_lsn);

                    DO_PTHREAD(pthread_cond_signal(&_flush_cond));
                    DO_PTHREAD(pthread_cond_wait(&_wait_cond,
                                                 &_wait_flush_lock));
                }
                if (ret_flushed) *ret_flushed = true;// now flushed!
            }
        } else {
            INC_TSTAT(log_dup_sync_cnt);
            if (ret_flushed) *ret_flushed = true; // already flushed
        }
    }
    return RCOK;
}


/*********************************************************************
 *
 *  rc_t logbuf_core::logbuf_flush(const lsn_t &to_lsn, bool block, bool signal, bool *ret_flushed)
 *
 *  Fake flush function for M1
 *
 *********************************************************************/
rc_t logbuf_core::logbuf_flush(const lsn_t &to_lsn, bool block, bool signal, bool
                       *ret_flushed) {
    return flush (to_lsn, block, signal, ret_flushed);
}


// =================== archive =========================
// any variable or function that is related to archive is not used for now and is not tested

/*********************************************************************
 *
 *  void logbuf_core::logbuf_archive()
 *
 *  Fake archive for M1
 *
 *********************************************************************/
void logbuf_core::logbuf_archive() {
    {
        CRITICAL_SECTION(cs, &_logbuf_lock);

        DBGOUT3(<< " archive: ");

        if (_to_archive_lsn == _to_flush_lsn) {
            // should not archive unflushed log records
            DBGOUT3(<< "cannot archive unflushed log records");
            return;
        }


        _to_archive_lsn = _to_flush_lsn;

        logbuf_seg *found = _to_flush_seg;
        // TODO: lookup for log archive must exclude unfushed log records
        //logbuf_seg *found = _hashtable->lookup(_to_archive_lsn.data());
        if (found == NULL) {
            ERROUT("the latest to_flush segment not found");
            W_FATAL(eINTERNAL);
        }
        else {
            _to_archive_seg = found;
        }

    }
}


// =================== internal functions =========================
/*********************************************************************
 *
 *  void logbuf_core::_insert_seg_for_fetch(logbuf_seg *seg)
 *
 *  Insert a segment descriptor to the log buffer for fetches
 *
 *********************************************************************/
void logbuf_core::_insert_seg_for_fetch(logbuf_seg *seg) {
    {
        // NOTE: disabling to_archive for now
        // // insert the seg immediately before _to_archive_seg
        //_seg_list->insert_before(seg, _to_archive_seg);

        // insert it to the head when to_archive is disabled
        _seg_list->push(seg);

        // add it to the hash table
        _hashtable->insert_if_not_exists(seg->base_lsn.data(), seg);

    }
}

/*********************************************************************
 *
 *  void logbuf_core::_insert_seg_to_list_for_insertion(logbuf_seg *seg)
 *
 *  Insert a segment descriptor to the list for insertions
 *
 *********************************************************************/
void logbuf_core::_insert_seg_to_list_for_insertion(logbuf_seg *seg) {
    {
        // always append it at tail
        _seg_list->append(seg);
    }
}

/*********************************************************************
 *
 *  void logbuf_core::_insert_seg_to_hashtable_for_insertion(logbuf_seg *seg)
 *
 *  Insert a segment descriptor to the hashtable for insertions
 *
 *********************************************************************/
void logbuf_core::_insert_seg_to_hashtable_for_insertion(logbuf_seg *seg) {
    {
        // add it to the hash table
        _hashtable->insert_if_not_exists(seg->base_lsn.data(), seg);
    }
}

/*********************************************************************
 *
 *  void logbuf_core::_remove_seg(logbuf_seg *seg)
 *
 *  Remove the segment descriptor from the log buffer
 *
 *********************************************************************/
void logbuf_core::_remove_seg(logbuf_seg *seg) {
    {
        _hashtable->remove(seg->base_lsn.data());
        _seg_list->remove(seg);
        seg->base_lsn = lsn_t::null;
    }
}


/*********************************************************************
 *
 *  logbuf_seg *logbuf_core::_get_new_seg_for_fetch()
 *
 *  Get a free segment for a missed fetch request
 *
 *  NOTE: the caller must be holding _logbuf_lock
 *        the caller is responsible to insert the returned segment to the list & the hashtable
 *
 *********************************************************************/
logbuf_seg *logbuf_core::_get_new_seg_for_fetch() {
    logbuf_seg *free_seg = NULL;

    int cnt=0;

    //logbuf_print_nolock("before get_new_seg_for_fetch");

    if (_seg_count == _max_seg_count) {
        // already full
//        DBGOUT3("evict old seg");
        do {
            cnt++;

            free_seg = _replacement();

            // looks like all segments are used for insertions
            if (free_seg == NULL) {

                _logbuf_lock.release();

                long used = _end - _start;
                // this is just an approximation (upper bound)
                if (used/_segsize + 1 >= _flush_trigger) {
                    force_a_flush();
                }
                else {
                    // this is impossible... just in case
                    DBGOUT0(<< "BUG @ _get_new_seg_for_fetch " << cnt);
                }

                _logbuf_lock.acquire();
            }
        }
        while (free_seg == NULL);
    }
    else {
        // not full
        // allocate 3 more blocks as tails
//        DBGOUT3("allocate new seg");
        free_seg = new logbuf_seg(_actual_segsize);
        _seg_count++;
    }


    //logbuf_print_nolock("after get_new_seg");

    return free_seg;
}


/*********************************************************************
 *
 *  void logbuf_core::_get_more_space_for_insertion(CArraySlot *info)
 *
 *  Get more log buffer space for current insertion
 *
 *  NOTE: the caller must NOT be holding _logbuf_lock
 *        the caller must be holding _insert_lock
 *        the caller is responsible to insert any segment to the hashtable
 *
 *********************************************************************/
void logbuf_core::_get_more_space_for_insertion(CArraySlot *info) {
    logbuf_seg *free_seg = NULL;

    w_assert1(info!=NULL);

    //logbuf_print("_get_more_space_for_insertion");

    _logbuf_lock.acquire();

    if (_seg_count == _max_seg_count) {
        // already full
//        DBGOUT3("evict old seg");

        free_seg = _replacement();

        _logbuf_lock.release();

        if (free_seg == NULL) {

            long used = _end - _start;
            if (used/_segsize + 1 >= _flush_trigger) {

                if(info)
                    _insert_lock.release(&info->me);

                force_a_flush();

                if(info)
                    _insert_lock.acquire(&info->me);

            }
            else {
                // there must be enough free space already?
                DBGOUT0(<< "BUG @ _get_more_space_for_insertion ");
            }
        }
        else {
            free_seg->base_lsn = lsn_t::null;
            _insert_seg_to_list_for_insertion(free_seg);
            _free += _segsize;
        }


    }
    else {
        // not full
        // allocate 3 more blocks as tails
//        DBGOUT3("allocate new seg");
        free_seg = new logbuf_seg(_actual_segsize);
        _seg_count++;

        free_seg->base_lsn = lsn_t::null;
        // append this segment to the list, but not to the hashtable, because it's still not used
        _insert_seg_to_list_for_insertion(free_seg);
        _free += _segsize;

        _logbuf_lock.release();

    }

    //logbuf_print("_get_more_space_for_insertion");

}


/*********************************************************************
 *
 *  logbuf_seg *logbuf_core::_replacement()
 *
 *  Evict segments to make space for new insertions or fetches
 *
 *  NOTE: the caller must be holding _logbuf_lock
 *
 *********************************************************************/
logbuf_seg *logbuf_core::_replacement() {

    logbuf_seg *to_evict = NULL;
    //
    //logbuf_seg *to_archive = NULL;
    logbuf_seg *to_flush = NULL;

    to_evict = NULL;
    //to_archive = _to_archive_seg;
    to_flush = _to_flush_seg;

    // NOTE: disable to_archive for now
    // // TODO: use LRU for the free buffer?
    // if (to_archive != NULL) {
    //     to_evict = _seg_list->top(); // evict the left-most segment
    //     if (to_archive != to_evict) {
    //         DBGOUT3(<< "evict seg " << to_evict->base_lsn << " in archived buffer");
    //         _remove_seg(to_evict);
    //         return to_evict;
    //     }
    //     else {
    //         DBGOUT3(<< "cannot find a replacement seg in archived buffer");
    //     }
    // }
    // else {
    //     ERROUT("_to_archive_seg is NULL");
    //     W_FATAL(eINTERNAL);
    //     return NULL;
    // }

    if (to_flush != NULL) {
        if (to_flush != _seg_list->top()) {

            // orignal policy with to_archive disabled
            //to_evict = _seg_list->prev_of(to_flush);

            // current policy with to_archive disabled
            // always evict the head of the list
            to_evict = _seg_list->top();

            _remove_seg(to_evict);

            return to_evict;

            // NOTE: disabling to_archive for now
            // if (to_evict != to_archive) {
            //     DBGOUT3(<< "evict seg " << to_evict->base_lsn << " in read buffer");
            //     _remove_seg(to_evict);
            //     return to_evict;
            // }
            // else {
            //     DBGOUT3(<< "trying to evict _to_archive_seg?");
            // }
        }
        else {
            DBGOUT3(<< "cannot find a replacement seg in read buffer");
            return NULL;
        }
    }
    else {
        ERROUT("_to_flush_seg is NULL");
        W_FATAL(eINTERNAL);
        return NULL;
    }
}

/*********************************************************************
 *
 *  void logbuf_core::force_a_flush()
 *
 *  Helper function to signal the flush daemon to perform a flush
 *
 *  Called by _get_new_seg_for_fetch and _get_more_space_for_insertion
 *
 *********************************************************************/
void logbuf_core::force_a_flush() {

    // last resort
//    DBGOUT3(<< "last resort: FORCE a flush");

    // when there are too many unflushed segments in the write buffer
    // force a flush

    {
        CRITICAL_SECTION(cs, _wait_flush_lock);

        // CS: switched from waiting_for_space to waiting_for_flush
        *&_waiting_for_flush = true;

        // Use signal since the only thread that should be waiting
        // on the _flush_cond is the log flush daemon.
        DO_PTHREAD(pthread_cond_signal(&_flush_cond));
        DO_PTHREAD(pthread_cond_wait(&_wait_cond,
                                     &_wait_flush_lock));
    }

}
