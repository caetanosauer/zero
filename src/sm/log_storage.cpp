/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define LOG_STORAGE_C

#include "sm_base.h"
#include "chkpt.h"

#include <regex>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <os_interface.h>
#include <largefile_aware.h>

#include "log_storage.h"
#include "log_core.h"
// needed for skip_log (TODO fix this)
#include "logdef_gen.cpp"

typedef smlevel_0::fileoff_t fileoff_t;
const string log_storage::log_prefix = "log.";
const string log_storage::log_regex = "log\\.[1-9][0-9]*";

/*
 * Opens log files in logdir and initializes partitions as well as the
 * given LSN's. The buffer given in prime_buf is primed with the contents
 * found in the last block of the last partition -- this logic was moved
 * from the various prime methods of the old log_core.
 */
log_storage::log_storage(const char* path, bool reformat, lsn_t& curr_lsn,
        lsn_t& durable_lsn, lsn_t& flush_lsn, long segsize)
    :
        _logpath(path),
        _segsize(segsize),
        _partition_size(0),
        _partition_data_size(0),
        _curr_partition(NULL)
{
    _logdir = new char[strlen(path) + 1]; // +1 for \0 byte
    strcpy(_logdir, path);

    _skip_log = new skip_log;

    // By the time we get here, the max_logsize should already have been
    // adjusted by the sm options-handling code, so it should be
    // a legitimate value now.
    W_COERCE(_set_partition_size(log_common::partition_size));

    // FRJ: we don't actually *need* this (no trx around yet), but we
    // don't want to trip the assertions that watch for it.
    CRITICAL_SECTION(cs, _partition_lock);

    partition_number_t  last_partition = 1;

    if (!reformat && !fs::exists(_logpath)) {
        cerr << "Error: could not open the log directory " << dir_name() <<endl;
        W_COERCE(RC(eOS));
    }

    fs::directory_iterator it(_logpath), eod;
    std::regex rx(log_regex, std::regex::basic);
    for (; it != eod; it++) {
        fs::path fpath = it->path();
        string fname = fpath.filename().string();

        if (regex_match(fname, rx)) {
            if (reformat) {
                fs::remove(fpath);
                continue;
            }

            long pnum = std::stoi(fname.substr(log_prefix.length()));
            _partitions[pnum] = new partition_t(this, pnum);

            if (pnum >= last_partition) {
                last_partition = pnum;
            }
        }
        else {
            cerr << "log_storage: cannot parse filename " << fname << endl;
            W_FATAL(fcINTERNAL);
        }

    }



    partition_t* p = get_partition(last_partition);
    if (!p) {
        create_partition(last_partition);
        p = get_partition(last_partition);
        w_assert0(p);
    }

    size_t pos = p->get_size(false);
    lsn_t new_lsn(last_partition, pos);
    curr_lsn = durable_lsn = flush_lsn = new_lsn;

    W_COERCE(p->open_for_append());
    _curr_partition = p;
    w_assert1(durable_lsn == curr_lsn);

    if(!p) {
        cerr << "ERROR: could not open log file for partition "
            << last_partition << endl;
        W_FATAL(eINTERNAL);
    }

    w_assert3(p->num() == last_partition);
}

log_storage::~log_storage()
{
    partition_t* p;
    partition_map_t::iterator it = _partitions.begin();
    while (it != _partitions.end()) {
        p = it->second;
        p->close_for_read();
        p->close_for_append();
        it++;
    }

    _partitions.clear();

    delete _skip_log;
    delete _logdir;
}

partition_t *
log_storage::get_partition_for_flush(lsn_t start_lsn,
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
            // CS TODO: this may deadlock because recycling also needs _partition_lock
            // grab the lock -- we're about to mess with partitions
            CRITICAL_SECTION(cs, _partition_lock);
            W_COERCE(p->close_for_append());
            p = create_partition(n+1);
            W_COERCE(p->open_for_append());
            _curr_partition = p;
        }
    }

    return p;
}

fileoff_t log_storage::partition_size(long psize)
{
     long p = psize - BLOCK_SIZE;
     return _floor(p, log_core::SEGMENT_SIZE) + BLOCK_SIZE;
}

fileoff_t log_storage::min_partition_size()
{
     return _floor(log_core::SEGMENT_SIZE, log_core::SEGMENT_SIZE)
         + BLOCK_SIZE;
}

fileoff_t log_storage::max_partition_size()
{
    fileoff_t tmp = sthread_t::max_os_file_size;
    tmp = tmp > lsn_t::max.lo() ? lsn_t::max.lo() : tmp;
    return  partition_size(tmp);
}

partition_t* log_storage::get_partition(partition_number_t n) const
{
    partition_map_t::const_iterator it = _partitions.find(n);
    if (it == _partitions.end()) { return NULL; }
    return it->second;
}

/*********************************************************************
 *
 *  log_storage::close_min(n)
 *
 *  Close the partition with the smallest index(num) or an unused
 *  partition, and
 *  return a ptr to the partition
 *
 *  The argument n is the partition number for which we are going
 *  to use the free partition.
 *
 *********************************************************************/
// CS TODO: disabled for now because we are supporting an unbouded
// number of partitions -- bounded list & recycling will be implemented later
// MUTEX: partition
#if 0
partition_t        *
log_storage::_close_min(partition_number_t n)
{
    // kick the cleaner thread(s)
    //if(smlevel_0::bf) smlevel_0::bf->wakeup_cleaners();

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
            msg << "Thread " << me()->id << " "
            << "Out of log space  ("
            //<< space_left()
            << "); No empty partitions."
            << endl;
            fprintf(stderr, "%s\n", msg.c_str());
        }

        if(tries++ > 8) W_FATAL(eOUTOFLOGSPACE);
        //if(smlevel_0::bf) smlevel_0::bf->wakeup_cleaners();
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
            cerr  << msg.c_str() << endl;
        }
        w_assert1(victim->num() < min_chkpt_rec_lsn().hi());

        victim->close(true);
        victim->destroy();

    } else {
        w_assert3(! victim->is_open_for_append());
        w_assert3(! victim->is_open_for_read());
    }

    victim->clear();

    return victim;
}
#endif

// Prime buf with the partial block ending at 'next'; return the size of that
// partial block (possibly 0)
//
// We are about to write a record for a certain lsn(next).  If we haven't been
// appending to this file (e.g., it's startup), we need to make sure the first
// part of the buffer contains the last partial block in the file, so that when
// we append that block to the file, we aren't clobbering the tail of the file
// (partition).
//
// This reads from the given file descriptor, the necessary block to cover the
// lsn.
long
log_storage::prime(char* buf, lsn_t next, size_t block_size)
{
    // get offset of block that contains "next"
    sm_diskaddr_t b = sm_diskaddr_t(_floor(next.lo(), block_size));

    // Offset of next within the block we're about to read
    long prime_offset = next.lo() - b;
    w_assert3(prime_offset >= 0);

    if(prime_offset > 0) {
        w_assert3(block_size > 0);
        partition_t* p = curr_partition();
        logrec_t* dummy;
        W_COERCE(p->read(buf, dummy, next, NULL));
        w_assert3((char*) dummy - buf == prime_offset);
    }
    return prime_offset;
}

partition_t* log_storage::create_partition(partition_number_t pnum)
{
#if W_DEBUG_LEVEL > 2
    // No other partition may be open for append
    partition_map_t::iterator it = _partitions.begin();
    for (; it != _partitions.end(); it++) {
        w_assert3(!it->second->is_open_for_append());
    }
#endif

    // we should also free up if necessary, as done in close_min
    partition_t* p = get_partition(pnum);
    if (p) {
        W_FATAL_MSG(eINTERNAL, << "Partition " << pnum << " already exists");
    }

    p = new partition_t(this, pnum);
    p->set_size(0);

    w_assert3(_partitions.find(pnum) == _partitions.end());
    _partitions[pnum] = p;

    return p;
}

partition_t * log_storage::curr_partition() const
{
    return _curr_partition;
}

w_rc_t log_storage::_set_partition_size(fileoff_t size)
{
    fileoff_t usable_psize = size;

    // partition must hold at least one buffer...
    if (usable_psize < _segsize) {
        W_FATAL(eOUTOFLOGSPACE);
    }

    // largest integral multiple of segsize() not greater than usable_psize:
    _partition_data_size = _floor(usable_psize, _segsize);

    if(_partition_data_size == 0)
    {
        cerr << "log size is too small: size "<<size<<" usable_psize "<<usable_psize
        <<", segsize() "<<_segsize<<", blocksize "<<BLOCK_SIZE<< endl;
        W_FATAL(eOUTOFLOGSPACE);
    }
    _partition_size = _partition_data_size + BLOCK_SIZE;
    DBGTHRD(<< "log_storage::_set_size setting _partition_size (limit LIMIT) "
            << _partition_size);

    return RCOK;
}

string log_storage::make_log_name(partition_number_t pnum) const
{
    return make_log_path(pnum).string();
}

fs::path log_storage::make_log_path(partition_number_t pnum) const
{
    return _logpath / fs::path(log_prefix + to_string(pnum));
}

void
log_storage::acquire_partition_lock()
{
    _partition_lock.acquire(&me()->get_log_me_node());
}
void
log_storage::release_partition_lock()
{
    _partition_lock.release(me()->get_log_me_node());
}

