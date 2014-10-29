/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define LOG_STORAGE_C

#include "sm_int_1.h"
#include "chkpt.h"

#include "log_core.h"

typedef smlevel_0::fileoff_t fileoff_t;

/*********************************************************************
 *
 * log_core::_version_major
 * log_core::_version_minor
 *
 * increment version_minor if a new log record is appended to the
 * list of log records and the semantics, ids and formats of existing
 * log records remains unchanged.
 *
 * for all other changes to log records or their semantics
 * _version_major should be incremented and version_minor set to 0.
 *
 *********************************************************************/
uint32_t const log_core::_version_major = 6;
uint32_t const log_core::_version_minor = 0;
const char log_core::_SLASH = '/';
const char log_core::_master_prefix[] = "chk."; // same size as _log_prefix
const char log_core::_log_prefix[] = "log.";
char       log_core::_logdir[max_devname];

fileoff_t log_core::partition_size(long psize)
{
     long p = psize - BLOCK_SIZE;
     return _floor(p, SEGMENT_SIZE) + BLOCK_SIZE; 
}

fileoff_t log_core::min_partition_size()
{ 
     return _floor(SEGMENT_SIZE, SEGMENT_SIZE) + BLOCK_SIZE; 
}

fileoff_t log_core::max_partition_size()
{
    fileoff_t tmp = sthread_t::max_os_file_size;
    tmp = tmp > lsn_t::max.lo() ? lsn_t::max.lo() : tmp;
    return  partition_size(tmp);
}

partition_index_t log_core::_get_index(uint32_t n) const
{
    const partition_t        *p;
    for(int i=0; i<PARTITION_COUNT; i++) {
        p = _partition(i);
        if(p->num()==n) return i;
    }
    return -1;
}

partition_t * log_core::_n_partition(partition_number_t n) const
{
    partition_index_t i = _get_index(n);
    return (i<0)? (partition_t *)0 : _partition(i);
}

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

partition_t * log_core::curr_partition() const
{
    w_assert3(partition_index() >= 0);
    return _partition(partition_index());
}

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

/*********************************************************************
 *
 *  log_core::make_log_name(idx, buf, bufsz)
 *
 *  Make up the name of a log file in buf.
 *
 *********************************************************************/
const char *
log_core::make_log_name(uint32_t idx, char* buf, int bufsz)
{
    // this is a static function w_assert2(_partition_lock.is_mine()==true);
    w_ostrstream s(buf, (int) bufsz);
    s << _logdir << _SLASH
      << _log_prefix << idx << ends;
    w_assert1(s);
    return buf;
}

void log_core::set_master(const lsn_t& mlsn, const lsn_t  & min_rec_lsn, 
        const lsn_t &min_xct_lsn) 
{
    CRITICAL_SECTION(cs, _partition_lock);
    lsn_t min_lsn = std::min(min_rec_lsn, min_xct_lsn);

    // This used to descend to raw_log or unix_log:
    w_assert1(log_core::THE_LOG != NULL);
    _write_master(mlsn, min_lsn);

    _master_lsn = mlsn;
    _min_chkpt_rec_lsn = min_lsn;
}

void log_core::_make_master_name(
    const lsn_t&         master_lsn, 
    const lsn_t&        min_chkpt_rec_lsn,
    char*                 buf,
    int                        bufsz,
    bool                old_style)
{
    w_ostrstream s(buf, (int) bufsz);

    s << _logdir << _SLASH << _master_prefix;
    lsn_t         array[2];
    array[0] = master_lsn;
    array[1] = min_chkpt_rec_lsn;

    _create_master_chkpt_string(s, 2, array, old_style);
    s << ends;
    w_assert1(s);
}

void log_core::_write_master(const lsn_t &l, const lsn_t &min) 
{
    /*
     *  create new master record
     */
    char _chkpt_meta_buf[CHKPT_META_BUF];
    _make_master_name(l, min, _chkpt_meta_buf, CHKPT_META_BUF);
    DBGTHRD(<< "writing checkpoint master: " << _chkpt_meta_buf);

    FILE* f = fopen(_chkpt_meta_buf, "a");
    if (! f) {
        w_rc_t e = RC(eOS);    
        smlevel_0::errlog->clog << fatal_prio 
            << "ERROR: could not open a new log checkpoint file: "
            << _chkpt_meta_buf << flushl;
        W_COERCE(e);
    }

    {        /* write ending lsns into the master chkpt record */
        lsn_t         array[PARTITION_COUNT];
        int j = log_core::THE_LOG->get_last_lsns(array);
        if(j > 0) {
            w_ostrstream s(_chkpt_meta_buf, CHKPT_META_BUF);
            _create_master_chkpt_contents(s, j, array);
        } else {
            memset(_chkpt_meta_buf, '\0', 1);
        }
        int length = strlen(_chkpt_meta_buf) + 1;
        DBG(<< " #lsns=" << j
            << " write this to master checkpoint record: " <<
                _chkpt_meta_buf);

        if(fwrite(_chkpt_meta_buf, length, 1, f) != 1) {
            w_rc_t e = RC(eOS);    
            smlevel_0::errlog->clog << fatal_prio 
                << "ERROR: could not write log checkpoint file contents"
                << _chkpt_meta_buf << flushl;
            W_COERCE(e);
        }
    }
    fclose(f);

    /*
     *  destroy old master record
     */
    _make_master_name(_master_lsn, 
                _min_chkpt_rec_lsn, _chkpt_meta_buf, CHKPT_META_BUF);
    (void) unlink(_chkpt_meta_buf);
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

/* Compute size of the biggest checkpoint we ever risk having to take...
 */
long log_core::max_chkpt_size() const 
{
    /* BUG: the number of transactions which might need to be
       checkpointed is potentially unbounded. However, it's rather
       unlikely we'll ever see more than 5k at any one time, especially
       each active transaction uses an active user thread
       
       The number of granted locks per transaction is also potentially
       unbounded.  Use a guess average value per active transaction,
       it should be unusual to see maximum active transactions and every
       transaction has the average number of locks
     */
    static long const GUESS_MAX_XCT_COUNT = 5000;
    static long const GUESS_EACH_XCT_LOCK_COUNT = 5;
    static long const FUDGE = sizeof(logrec_t);
    long bf_tab_size = bf->get_block_cnt()*sizeof(chkpt_bf_tab_t::brec_t);
    long xct_tab_size = GUESS_MAX_XCT_COUNT*sizeof(chkpt_xct_tab_t::xrec_t);
    long xct_lock_size = GUESS_EACH_XCT_LOCK_COUNT*GUESS_MAX_XCT_COUNT*sizeof(chkpt_xct_lock_t::lockrec_t);
    long dev_tab_size = max_vols*sizeof(chkpt_dev_tab_t::devrec_t);
    return FUDGE + bf_tab_size + xct_tab_size + xct_lock_size + dev_tab_size;
}

rc_t                
log_core::file_was_archived(const char * /*file*/)
{
    // TODO: should check that this is the oldest, 
    // and that we indeed asked for it to be archived.
    _space_available += recoverable_space(1);
    return RCOK;
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

fileoff_t log_core::take_space(fileoff_t *ptr, int amt) 
{
    BOOST_STATIC_ASSERT(sizeof(fileoff_t) == sizeof(int64_t));
    fileoff_t ov = lintel::unsafe::atomic_load(const_cast<int64_t*>(ptr));
    // fileoff_t ov = *ptr;
#if W_DEBUG_LEVEL > 0
    DBGTHRD("take_space " << amt << " old value of ? " << ov);
#endif
    while(1) {
        if (ov < amt) {
            return 0;
        }
	fileoff_t nv = ov - amt;
	if (lintel::unsafe::atomic_compare_exchange_strong(const_cast<int64_t*>(ptr), &ov, nv)) {
	    return amt;
        }
    }
}

fileoff_t log_core::reserve_space(fileoff_t amt) 
{
    return (amt > 0)? take_space(&_space_available, amt) : 0;
}

fileoff_t log_core::consume_chkpt_reservation(fileoff_t amt)
{
    if(operating_mode != t_forward_processing)
       return amt; // not yet active -- pretend it worked

    return (amt > 0)? 
        take_space(&_space_rsvd_for_chkpt, amt) : 0;
}

// make sure we have enough log reservation (conservative)
// NOTE: this has to be compared with the size of a partition,
// which _set_size does (it knows the size of a partition)
bool log_core::verify_chkpt_reservation() 
{
    fileoff_t space_needed = max_chkpt_size();
    while(*&_space_rsvd_for_chkpt < 2*space_needed) {
        if(reserve_space(space_needed)) {
            // abuse take_space...
            take_space(&_space_rsvd_for_chkpt, -space_needed);
        } else if(*&_space_rsvd_for_chkpt < space_needed) {
            /* oops...

               can't even guarantee the minimum of one checkpoint
               needed to reclaim log space and solve the problem
             */
            W_FATAL(eOUTOFLOGSPACE);
        } else {
            // must reclaim a log partition
            return false;
        }
    }
    return true;
}

// Determine if this lsn is holding up scavenging of logs by (being 
// on a presumably hot page, and) being a rec_lsn that's in the oldest open
// log partition and that oldest partition being sufficiently aged....
bool log_core::squeezed_by(const lsn_t &self)  const 
{
    // many partitions are open
    return 
    ((curr_lsn().file() - global_min_lsn().file()) >=  (PARTITION_COUNT-2))
        &&
    (self.file() == global_min_lsn().file())  // the given lsn 
                                              // is in the oldest file
    ;
}

int log_core::get_last_lsns(lsn_t *array)
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

void log_core::_create_master_chkpt_string(
                ostream&        s,
                int                arraysize,
                const lsn_t*        array,
                bool                old_style)
{
    w_assert1(arraysize >= 2);
    if (old_style)  {
        s << array[0] << '.' << array[1];

    }  else  {
        s << 'v' << _version_major << '.' << _version_minor ;
        for(int i=0; i< arraysize; i++) {
                s << '_' << array[i];
        }
    }
}

rc_t log_core::_check_version(uint32_t major, uint32_t minor)
{
        if (major == _version_major && minor <= _version_minor)
                return RCOK;

        w_error_codes err = (major < _version_major)
                        ? eLOGVERSIONTOOOLD : eLOGVERSIONTOONEW;

        smlevel_0::errlog->clog << fatal_prio 
            << "ERROR: log version too "
            << ((err == eLOGVERSIONTOOOLD) ? "old" : "new")
            << " sm ("
            << _version_major << " . " << _version_minor
            << ") log ("
            << major << " . " << minor
            << flushl;

        return RC(err);
}

void log_core::_create_master_chkpt_contents(
                ostream&        s,
                int                arraysize,
                const lsn_t*        array
                )
{
    for(int i=0; i< arraysize; i++) {
            s << '_' << array[i];
    }
    s << ends;
}

rc_t log_core::_parse_master_chkpt_contents(
                istream&            s,
                int&                    listlength,
                lsn_t*                    lsnlist
                )
{
    listlength = 0;
    char separator;
    while(!s.eof()) {
        s >> separator;
        if(!s.eof()) {
            w_assert9(separator == '_' || separator == '.');
            s >> lsnlist[listlength];
            DBG(<< listlength << ": extra lsn = " << 
                lsnlist[listlength]);
            if(!s.fail()) {
                listlength++;
            }
        }
    }
    return RCOK;
}

rc_t log_core::_parse_master_chkpt_string(
                istream&            s,
                lsn_t&              master_lsn,
                lsn_t&              min_chkpt_rec_lsn,
                int&                    number_of_others,
                lsn_t*                    others,
                bool&                    old_style)
{
    uint32_t major = 1;
    uint32_t minor = 0;
    char separator;

    s >> separator;

    if (separator == 'v')  {                // has version, otherwise default to 1.0
        old_style = false;
        s >> major >> separator >> minor;
        w_assert9(separator == '.');
        s >> separator;
        w_assert9(separator == '_');
    }  else  {
        old_style = true;
        s.putback(separator);
    }

    s >> master_lsn >> separator >> min_chkpt_rec_lsn;
    w_assert9(separator == '_' || separator == '.');

    if (!s)  {
        return RC(eBADMASTERCHKPTFORMAT);
    }

    number_of_others = 0;
    while(!s.eof()) {
        s >> separator;
        if(separator == '\0') break; // end of string

        if(!s.eof()) {
            w_assert9(separator == '_' || separator == '.');
            s >> others[number_of_others];
            DBG(<< number_of_others << ": extra lsn = " << 
                others[number_of_others]);
            if(!s.fail()) {
                number_of_others++;
            }
        }
    }

    return _check_version(major, minor);
}

w_rc_t log_core::_read_master( 
        const char *fname,
        int prefix_len,
        lsn_t &tmp,
        lsn_t& tmp1,
        lsn_t* lsnlist,
        int&   listlength,
        bool&  old_style
)
{
    rc_t         rc;
    {
        /* make a copy */
        int        len = strlen(fname+prefix_len) + 1;
        char *buf = new char[len];
        memcpy(buf, fname+prefix_len, len);
        w_istrstream s(buf);

        rc = _parse_master_chkpt_string(s, tmp, tmp1, 
                                       listlength, lsnlist, old_style);
        delete [] buf;
        if (rc.is_error()) {
            smlevel_0::errlog->clog << fatal_prio 
            << "bad master log file \"" << fname << "\"" << flushl;
            W_COERCE(rc);
        }
        DBG(<<"_parse_master_chkpt_string returns tmp= " << tmp
            << " tmp1=" << tmp1
            << " old_style=" << old_style);
    }

    /*  
     * read the file for the rest of the lsn list
     */
    {
        char*         buf = new char[smlevel_0::max_devname];
        if (!buf)
            W_FATAL(fcOUTOFMEMORY);
        w_auto_delete_array_t<char> ad_fname(buf);
        w_ostrstream s(buf, int(smlevel_0::max_devname));
        s << _logdir << _SLASH << fname << ends;

        FILE* f = fopen(buf, "r");
        if(f) {
            char _chkpt_meta_buf[CHKPT_META_BUF];
            int n = fread(_chkpt_meta_buf, 1, CHKPT_META_BUF, f);
            if(n  > 0) {
                /* Be paranoid about checking for the null, since a lack
                   of it could send the istrstream driving through memory
                   trying to parse the information. */
                void *null = memchr(_chkpt_meta_buf, '\0', CHKPT_META_BUF);
                if (!null) {
                    smlevel_0::errlog->clog << fatal_prio 
                        << "invalid master log file format \"" 
                        << buf << "\"" << flushl;
                    W_FATAL(eINTERNAL);
                }
                    
                w_istrstream s(_chkpt_meta_buf);
                rc = _parse_master_chkpt_contents(s, listlength, lsnlist);
                if (rc.is_error())  {
                    smlevel_0::errlog->clog << fatal_prio 
                        << "bad master log file contents \"" 
                        << buf << "\"" << flushl;
                    W_COERCE(rc);
                }
            }
            fclose(f);
        } else {
            /* backward compatibility with minor version 0: 
             * treat empty file ok
             */
            w_rc_t e = RC(eOS);
            smlevel_0::errlog->clog << fatal_prio
                << "ERROR: could not open existing log checkpoint file: "
                << buf << flushl;
            W_COERCE(e);
        }
    }
    return RCOK;
}
