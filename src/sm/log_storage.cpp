/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define SM_SOURCE
#define LOG_STORAGE_C

#include "sm_base.h"
#include "chkpt.h"
#include <sys/stat.h>

#include <cstdio>        /* XXX for log recovery */
#include <sys/types.h>
#include <sys/stat.h>
#include <os_interface.h>
#include <largefile_aware.h>

#include "log_storage.h"

#include "log_core.h"

// needed for skip_log (TODO fix this)
#include "logdef_gen.cpp"

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
const uint32_t log_storage::_version_major = 6;
const uint32_t log_storage::_version_minor = 1;
const char log_storage::_SLASH = '/';
const char log_storage::_master_prefix[] = "chk."; // same size as _log_prefix
const char log_storage::_log_prefix[] = "log.";

/*
 * Opens log files in logdir and initializes partitions as well as the
 * given LSN's. The buffer given in prime_buf is primed with the contents
 * found in the last block of the last partition -- this logic was moved
 * from the various prime methods of the old log_core.
 */
log_storage::log_storage(const char* path, bool reformat, lsn_t& curr_lsn,
        lsn_t& durable_lsn, lsn_t& flush_lsn, long segsize)
    :
      _segsize(segsize),
      _partition_size(0),
      _partition_data_size(0),
      _curr_num(1),
      _curr_partition(NULL)
{
    // CS TODO: pass sm_options
    _logdir = new char[strlen(path) + 1]; // +1 for \0 byte
    strcpy(_logdir, path);

    _skip_log = new skip_log;

    DO_PTHREAD(pthread_mutex_init(&_scavenge_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_scavenge_cond, NULL));

    // By the time we get here, the max_logsize should already have been
    // adjusted by the sm options-handling code, so it should be
    // a legitimate value now.
    W_COERCE(_set_partition_size(log_common::partition_size));

    DBGOUT3(<< "SEG SIZE " << _segsize << " PARTITION DATA SIZE " << _partition_data_size);

    // FRJ: we don't actually *need* this (no trx around yet), but we
    // don't want to trip the assertions that watch for it.
    CRITICAL_SECTION(cs, _partition_lock);

    partition_number_t  last_partition = partition_num();
    bool                last_partition_exists = false;

    /*
     * STEP 1: open file handler for log directory
     */
    fileoff_t eof= fileoff_t(0);
    os_dir_t ldir = os_opendir(dir_name());
    if (! ldir)
    {
        w_rc_t e = RC(eOS);
        cerr << "Error: could not open the log directory " << dir_name() <<endl;
        fprintf(stderr, "Error: could not open the log directory %s\n",
                    dir_name());

        cerr << "\tNote: the log directory is specified using\n"
            "\t      the sm_logdir option." << endl << endl;

        W_COERCE(e);
    }
    DBGTHRD(<<"opendir " << dir_name() << " succeeded");

    /*
     *  scan directory for master lsn and last log file
     */

    os_dirent_t *dd=0;

    char *fname = new char [smlevel_0::max_devname];
    if (!fname)
        W_FATAL(fcOUTOFMEMORY);

    /* Create a list of lsns for the partitions - this
     * will be used to store any hints about the last
     * lsns of the partitions (stored with checkpoint meta-info
     */
    vector<lsn_t> lsnlist;

    /*
     * STEP 2: Reformat log if necessary
     */
    DBGTHRD(<<"reformat= " << reformat
            << " last_partition "  << last_partition
            << " last_partition_exists "  << last_partition_exists
            );
    if (reformat)
    {
        cerr << "Reformatting logs..." << endl;

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

            memset(name, '\0', namelen+1);
            strncpy(name, d, orig_namelen);
            DBGTHRD(<<"name= " << name);

            bool parse_ok = (strncmp(name,master_prefix(),strlen(master_prefix()))==0);
            if(!parse_ok) {
                parse_ok = (strncmp(name,log_prefix(),strlen(log_prefix()))==0);
            }
            if(parse_ok) {
                cerr << "\t" << name << "..." << endl;

                {
                    w_ostrstream s(fname, (int) smlevel_0::max_devname);
                    s << dir_name() << _SLASH << name << ends;
                    w_assert1(s);
                    if( unlink(fname) < 0) {
                        w_rc_t e = RC(fcOS);
                        cerr << "unlink(" << fname << "):"
                            << endl << e << endl;
                    }
                }
            }

            delete[] name;
        }

        _write_master();

        //  os_closedir(ldir);
        w_assert3(!last_partition_exists);
    }

    /*
     * STEP 3: Scan files in the log directory.
     * When chk file is found, set _master_lsn and _min_chkpt_rec_lsn.
     * For log.* files, look for maximum partition number (last_partition)
     */
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

        unsigned int         namelen = prefix_len;
        const char *         dn = dd->d_name;
        unsigned int         orig_namelen = strlen(dn);

        namelen = namelen > orig_namelen ? namelen : orig_namelen;
        char *                name = new char [namelen+1];

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
        // CS TODO: master should be read first, to guarantee that we have
        // something on lsnlist before opening log files
        if (parse_ok) {
            lsn_t tmp;
            if (strcmp(buf, master_prefix()) == 0)
            {
                DBGOUT5(<<"found master file " << buf);
                w_istrstream s(name + prefix_len);
                W_COERCE(_check_version(s));
            }
            else if (strcmp(buf, log_prefix()) == 0)  {
                DBGOUT5(<<"found log file " << buf);

                w_istrstream s(name + prefix_len);
                uint32_t curr;
                if (! (s >> curr))  {
                    cerr << "bad log file \"" << name << "\"" << endl;
                    W_FATAL(eINTERNAL);
                }

                lsn_t lasthint = lsn_t::null;
                for (size_t q = 0; q < lsnlist.size(); q++) {
                    if(lsnlist[q].hi() == curr) {
                        lasthint = lsnlist[q];
                    }
                }

                partition_t* p = new partition_t();
                p->init(this);
                p->peek(curr, lasthint, true);
                p->open_for_read(curr, true);

                _partitions[curr] = p;
                p->close();

                if (curr >= last_partition) {
                    last_partition = curr;
                    last_partition_exists = true;
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
            cerr << "log_core: cannot parse filename \""
                                    << name << "\".  Maybe a data volume in the logging directory?"
                                    << endl;
            W_FATAL(fcINTERNAL);
        }

        delete[] buf;
        delete[] name;
    }
    os_closedir(ldir);

    DBGOUT5(<<"after closedir  "
            << " last_partition "  << last_partition
            << " last_partition_exists "  << last_partition_exists
            );

#if W_DEBUG_LEVEL > 2
    if(reformat) {
        w_assert3(partition_num() == 1);
    }
#endif

    /*
     * STEP 5: Truncate at last complete log rec

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
    {
        DBGOUT5(<<" truncate last complete log rec ");

        make_log_name(last_partition, fname, smlevel_0::max_devname);
        DBGOUT5(<<" checking " << fname);

        FILE *f =  fopen(fname, "r");
        DBGOUT5(<<" opened " << fname << " fp " << f << " pos " << pos);

        fileoff_t start_pos = pos;

        // CS TODO: try to open on LSN higher than 0. This used to be where
        // we would get the seek position from the master LSN if the partitions
        // matched.

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
                            cerr << "ERROR: unexpected log file inconsistency." << endl;
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
                        cerr << "Found unexpected end of log -- probably due to a previous crash."
                            << endl;
                        cerr << "   Recovery will continue ..." << endl;
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
                    cerr << "fopen(" << fname << "):" << endl << e << endl;
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

                eof = ftell(f);
                w_rc_t e = RC(eOS);        /* collect the error in case it is needed */
                DBGOUT5(<<"eof is now " << eof);


                if(((eof) % BLOCK_SIZE) != 0) {
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
        <<" current_lsn " << curr_lsn
        <<" durable_lsn " << durable_lsn);

    lsn_t new_lsn(last_partition, pos);


    curr_lsn = durable_lsn = flush_lsn = new_lsn;



    DBGOUT2( << "partition num = " << partition_num()
            <<" current_lsn " << curr_lsn
            <<" durable_lsn " << durable_lsn);

    {
        /*
         *  create/open the "current" partition
         *  "current" could be new or existing
         *  Check its size and all the records in it
         *  by passing "true" for the last argument to open()
         */
        lsn_t lasthint = lsn_t::null;
        for(size_t q = 0; q < lsnlist.size(); q++) {
            if(lsnlist[q].hi() == last_partition) {
                lasthint = lsnlist[q];
            }
        }
        // open_partition will set LSN's and current
        partition_t *p = _open_partition_for_append(last_partition, lasthint,
                last_partition_exists, true);
        w_assert1(durable_lsn == curr_lsn); // better be startup/recovery!

        if(!p) {
            cerr << "ERROR: could not open log file for partition "
            << last_partition << endl;
            W_FATAL(eINTERNAL);
        }

        w_assert3(p->num() == last_partition);
        w_assert3(partition_num() == last_partition);

    }
    DBGOUT2( << "partition num = " << partition_num()
            <<" current_lsn " << curr_lsn
            <<" durable_lsn " << durable_lsn);

    delete[] fname;
}

log_storage::~log_storage()
{
    partition_t* p;
    partition_map_t::iterator it = _partitions.begin();
    while (it != _partitions.end()) {
        p = it->second;
        p->close_for_read();
        p->close_for_append();
        p->clear();
        it++;
    }

    _partitions.clear();

    DO_PTHREAD(pthread_mutex_destroy(&_scavenge_lock));
    DO_PTHREAD(pthread_cond_destroy(&_scavenge_cond));

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
            p->close();
            p = _open_partition_for_append(n+1, lsn_t::null, false, false);
        }

        // it's a new partition -- size is now 0
        w_assert3(curr_partition()->size()== 0);
        w_assert3(partition_num() != 0);
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
log_storage::prime(char* buf, lsn_t next, size_t block_size, bool read_whole_block)
{
    // get offset of block that contains "next"
    sm_diskaddr_t b = sm_diskaddr_t(_floor(next.lo(), block_size));

    long prime_offset = next.lo() - b;
    /*
     * CS: Handle case where next is exactly at a block border.
     * This is used by logbuf_core, where read_whole_block == false.
     * In that case, we must read the whole segment.
     * Another way to think of this is that we did not explicitly
     * require reading the whole block, but the position of next is
     * telling us to read a whole block.
     */
    if (!read_whole_block && prime_offset == 0 && next.lo() > 0) {
        prime_offset = block_size;
        b -= block_size;
    }

    w_assert3(prime_offset >= 0);
    if(prime_offset > 0) {
        size_t read_size = read_whole_block ? block_size : prime_offset;
        w_assert3(read_size > 0);
        partition_t* p = curr_partition();
        W_COERCE(me()->pread(p->fhdl_app(), buf, read_size, b));
    }
    return prime_offset;
}

partition_t*
log_storage::find_partition(lsn_t& ll, bool existing, bool recovery, bool forward)
{
    partition_t        *p = 0;
    uint32_t        last_hi=0;
    while (!p) {
        if(last_hi == ll.hi()) {
            // can happen on the 2nd or subsequent round
            // but not first
            W_FATAL_MSG(eEOF, << "Partition not found for" << ll);
        }
        last_hi = ll.hi();

        DBG(<<" about to open " << ll.hi());
        //                                 part#, end_hint, existing, recovery
        if ((p = _open_partition_for_read(ll.hi(), lsn_t::null, existing, recovery))) {

            // opened one... is it the right one?
            DBGTHRD(<<"opened... p->size()=" << p->size());

            if (forward && recovery) {
            if (ll.lo() >= p->size() ||
                     (p->size() == partition_t::nosize && ll.lo() >= limit()))
            {
                DBGTHRD(<<"seeking to " << ll.lo() << ";  beyond p->size() ... OR ...");
                DBGTHRD(<<"limit()=" << limit() << " & p->size()=="
                        << int(partition_t::nosize));

                ll = log_m::first_lsn(ll.hi() + 1);
                DBGTHRD(<<"getting next partition: " << ll);
                p = 0; continue;
            }
            }
        }
    }

    return p;
}

bool log_storage::_partition_exists(partition_number_t pnum)
{
    char fname[smlevel_0::max_devname];
    make_log_name(pnum, fname, smlevel_0::max_devname);
    int flags = smthread_t::OPEN_RDONLY;
    int fd = -1;
    rc_t rc = me()->open(fname, flags, 0644, fd);
    if (rc.is_error()) {
        // we assume file does not exist for any kind of error -- more info
        // than that is unfortunately not available with sdisk_unix_t::open
        return false;
    }
    else {
        W_COERCE(me()->close(fd));
        return true;
    }
}

rc_t log_storage::last_lsn_in_partition(partition_number_t pnum, lsn_t& lsn)
{
    partition_t* p = get_partition(pnum);
    if(!p) {
        // we are not sure if the previous partition exists or not
        // if it does exist, we want to know its size
        // if not, we have reached EOF
        if (!_partition_exists(pnum)) {
            lsn = lsn_t::null;
        }
        bool recovery = false;
        bool existing = true;
        p = _open_partition_for_read(pnum, lsn_t::null, existing, recovery);
        if(!p)
            W_FATAL_MSG(eINTERNAL, << "Open partition failed");
    }

    if (p->size() == partition_t::nosize) {
        lsn = lsn_t::null;
        return RCOK;
    }
    if (p->size() == 0) {
        // CS TODO: surely we can to better than this
        // this is a special case
        // when the partition does not exist,
        // _open_partition_for_read->_open_partitionp->peek would create an empty file
        // we must remove this file immediately
        p->close(true);
        destroy_file(p->num());
        p->destroy();
        lsn = lsn_t::null;
        return RCOK;
    }

    // this partition is already opened
    lsn = lsn_t(pnum, p->size());
    return RCOK;
}

/*********************************************************************
 *
 *  log_storage::_open_partition_for_append() calls _open_partition with
 *                            forappend=true)
 *  log_storage::_open_partition_for_read() calls _open_partition with
 *                            forappend=false)
 *
 *  log_storage::_open_partition(num, end_hint, existing,
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
log_storage::_open_partition(partition_number_t  __num,
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
    partition_t *p = get_partition(__num);

    if(p && existing && !forappend) {
        W_COERCE(p->open_for_read(__num));

        w_assert3(p->is_open_for_read());
        w_assert3(p->num() == __num);
        w_assert3(p->exists());
    }


    if(forappend) {
#if W_DEBUG_LEVEL > 2
        // No other partition may be open for append
        partition_map_t::iterator it = _partitions.begin();
        for (; it != _partitions.end(); it++) {
            w_assert3(!it->second->is_open_for_append());
        }
#endif
        // If creating a new, we should also free up if necessary, as done
        // in close_min
        /*
         *  This becomes the current partition.
         */
        if (!p) {
            p = new partition_t();
            p->init(this);

            w_assert3(during_recovery || partition_num() == __num - 1);
            w_assert3(_partitions.find(__num) == _partitions.end());
            _partitions[__num] = p;
        }
        p->open_for_append(__num, end_hint);
        set_current(p);
        w_assert3(p->exists());
        w_assert3(p->is_open_for_append());
    }

    return p;
}

void
log_storage::set_current(partition_t* p)
{
    _curr_num = p->num();
    _curr_partition = p;
}

partition_t * log_storage::curr_partition() const
{
    return _curr_partition;
}

/*
 * This code used to be in log_core::scavenge, which is currently
 * the only caller of this method.
 * WARNING: caller must acquire and release partition lock!
 */
int log_storage::delete_old_partitions(lsn_t lsn)
{
    partition_t        *p;

    partition_number_t min_num;
    {
        /*
         *  find min_num -- the lowest of all the partitions
         */
        min_num = partition_num();

        partition_map_t::iterator it = _partitions.begin();
        while (it != _partitions.end()) {
            p = it->second;
            if(p->num() > 0 &&  p->num() < min_num) {
                min_num = p->num();
            }
            it++;
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
        p = get_partition(min_num);
        w_assert3(p);
        // CS: this check doe snot seem crucial -- commented for now
        //if (durable_lsn() < p->first_lsn() )  {
            //W_FATAL(fcINTERNAL); // why would this ever happen?
        //}
        //w_assert3(durable_lsn() >= p->first_lsn());
        DBGTHRD( << "scavenging log " << p->num() << endl );
        count++;
        p->close(true);
        destroy_file(p->num());
        p->destroy();
    }

    return count;
}

void
log_storage::destroy_file(partition_number_t n)
{
    char        *fname = new char[smlevel_0::max_devname];
    if (!fname)
        W_FATAL(fcOUTOFMEMORY);
    make_log_name(n, fname, smlevel_0::max_devname);
    if (unlink(fname) == -1)  {
        w_rc_t e = RC(eOS);
        cerr << "destroy_file " << n << " " << fname << ":" <<endl
             << e << endl;
    }

    delete[] fname;
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

void
log_storage::sanity_check() const
{
#if W_DEBUG_LEVEL > 1
    const partition_t*  p;
    bool                found_current=false;

    // we should not be calling this when
    // we're in any intermediate state, i.e.,
    // while there's no current index

    partition_map_t::const_iterator it = _partitions.begin();
    for (; it != _partitions.end(); it++) {
        p = it->second;
        p->sanity_check();

        // at most one open for append at any time
        if(p->num()>0) {
            w_assert1(p->exists());
            w_assert1(p ==  get_partition(p->num()));

            if(p->num() == partition_num()) {
                w_assert1(!found_current);
                found_current = true;
                w_assert1(p ==  curr_partition());
                w_assert1(p->is_open_for_append());
            }
        } else {
            w_assert1(p->num() != partition_num());
            w_assert1(!p->exists());
        }
    }
#endif
}

/*********************************************************************
 *
 *  log_storage::make_log_name(idx, buf, bufsz)
 *
 *  Make up the name of a log file in buf.
 *
 *********************************************************************/
const char *
log_storage::make_log_name(uint32_t idx, char* buf, int bufsz)
{
    // this is a static function w_assert2(_partition_lock.is_mine()==true);
    w_ostrstream s(buf, (int) bufsz);
    s << _logdir << _SLASH
      << _log_prefix << idx << ends;
    w_assert1(s);
    return buf;
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

void
log_storage::acquire_scavenge_lock()
{
    DO_PTHREAD(pthread_mutex_lock(&_scavenge_lock));
}
void
log_storage::release_scavenge_lock()
{
    DO_PTHREAD(pthread_mutex_unlock(&_scavenge_lock));
}

void
log_storage::signal_scavenge_cond()
{
    DO_PTHREAD(pthread_cond_signal(&_scavenge_cond));
}

rc_t log_storage::_check_version(istream& s)
{
    uint32_t major = 1;
    uint32_t minor = 0;

    char separator;
    s >> separator;
    if (separator == 'v')  {
        s >> major >> separator >> minor;
        w_assert9(separator == '.');
    }

    if (major == _version_major && minor <= _version_minor)
        return RCOK;

    w_error_codes err = (major < _version_major)
        ? eLOGVERSIONTOOOLD : eLOGVERSIONTOONEW;

    cerr << "ERROR: log version too "
        << ((err == eLOGVERSIONTOOOLD) ? "old" : "new")
        << " sm ("
        << _version_major << " . " << _version_minor
        << ") log ("
        << major << " . " << minor
        << endl;

    return RC(err);
}

void log_storage::_write_master()
{
    char* buf = new char[smlevel_0::max_devname];
    w_ostrstream s(buf, (int) smlevel_0::max_devname);
    s << _logdir << _SLASH << _master_prefix << "v" << _version_major
        << "." << _version_minor << ends;

    FILE* f = fopen(buf, "a");
    if (! f) {
        w_rc_t e = RC(eOS);
        cerr << "ERROR: could not open a new chk file: " << buf << endl;
        W_COERCE(e);
    }
    delete[] buf;
}

