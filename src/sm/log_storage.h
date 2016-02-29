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

#ifndef LOG_STORAGE_H
#define LOG_STORAGE_H
#include "w_defines.h"

#include "log.h"
#include <partition.h>
#include <map>
#include <vector>

typedef    smlevel_0::partition_number_t partition_number_t;
typedef std::map<partition_number_t, partition_t*> partition_map_t;

class skip_log; // forward

/*  -- do not edit anything above this line --   </std-header>*/

class log_storage {

    typedef smlevel_0::fileoff_t fileoff_t;

    // use friend mechanism until better interface is implemented
    friend class partition_t;

public:
    log_storage(const char* path, bool reformat, lsn_t& curr_lsn,
        lsn_t& durable_lsn, lsn_t& flush_lsn, long segsize);
    virtual ~log_storage();

    partition_t*    get_partition_for_flush(lsn_t start_lsn,
                            long start1, long end1, long start2, long end2);
    partition_t*    find_partition(lsn_t&, bool existing, bool recovery, bool forward);
    rc_t last_lsn_in_partition(partition_number_t pnum, lsn_t& lsn);
    partition_t*    curr_partition() const;
    int             delete_old_partitions(lsn_t);
    long            prime(char* buf, lsn_t next, size_t block_size,
                            bool read_whole_block = true);
    void            acquire_partition_lock();
    void            release_partition_lock();
    void            set_master(const lsn_t& master_lsn, const lsn_t& min_lsn,
                            const lsn_t& min_xct_lsn);

    void            acquire_scavenge_lock();
    void            release_scavenge_lock();
    void            signal_scavenge_cond();

    // for partition_t
    void                set_current(partition_t* p);
    virtual partition_number_t  partition_num() const { return _curr_num; }
    partition_t *       get_partition(partition_number_t n) const;
    static long         floor2(long offset, long block_size)
                            { return offset & -block_size; }
    static long         ceil2(long offset, long block_size)
                           { return
                               floor2(offset + block_size - 1, block_size); }


    const char *        dir_name() { return _logdir; }

    lsn_t               master_lsn() const {
                            ASSERT_FITS_IN_POINTER(lsn_t);
                            // else need to grab the partition mutex
                            return _master_lsn;
                        }

    // public for use in xct_impl in log-full handling...
    /**\brief
     * \details
     * Set at constructor time and when a new master is created (set_master)
     */
    lsn_t               min_chkpt_rec_lsn() const {
                            ASSERT_FITS_IN_POINTER(lsn_t);
                            // else need to grab the partition mutex
                            return _min_chkpt_rec_lsn;
                        }
    // used by partition_t
    skip_log*       get_skip_log()  { return _skip_log; }

    /* Q: how much reservable space does scavenging pcount partitions
          give back?

      A: everything except a bit we have to keep to ensure the log
          can always be flushed.
     */
    size_t              recoverable_space(int pcount) const {
                               return pcount*(_partition_data_size - BLOCK_SIZE);
                            }

    fileoff_t           partition_data_size() const {
                            return _partition_data_size; }

    /**\brief used by partition */
    fileoff_t limit() const { return _partition_size; }

    /**\brief Return name of log file for given partition number.
     * \details
     * Used by xct for error-reporting and callback-handling.
     */
    const char * make_log_name(uint32_t n,
                        char*              buf,
                        int                bufsz);

    static long         _floor(long offset, long block_size)
                            { return (offset/block_size)*block_size; }
    static long         _ceil(long offset, long block_size)
                            { return _floor(offset + block_size - 1, block_size); }
    static fileoff_t          partition_size(long psize);
    static fileoff_t          min_partition_size();
    static fileoff_t          max_partition_size();

    // use in dump_page_lsn_chain (log_spr.cpp)
    // TODO fix dump function and delete this -- not thread safe!
    void reset_master_lsn(lsn_t master) { _master_lsn = master; }

    lsn_t               global_min_lsn() const
        {  return std::min(_master_lsn, _min_chkpt_rec_lsn); }

private:
    void                _prime(int fd, fileoff_t start, lsn_t next);
    void     destroy_file(partition_number_t n, bool e);

    partition_t *       _close_min(partition_number_t n);
                                // the defaults are for the case
                                // in which we're opening a file to
                                // be the new "current"
    partition_t *       _open_partition(partition_number_t n,
                            const lsn_t&  end_hint,
                            bool existing,
                            bool forappend,
                            bool during_recovery
                        );
    partition_t *       _open_partition_for_append(partition_number_t n,
                            const lsn_t&  end_hint,
                            bool existing,
                            bool during_recovery
                        ) { return _open_partition(n,
                                    end_hint, existing,
                                    true, during_recovery);
                          }
    partition_t *       _open_partition_for_read(partition_number_t n,
                            const lsn_t&  end_hint,
                            bool existing,
                            bool during_recovery
                        ) { return _open_partition(n,
                                    end_hint, existing,
                                    false, during_recovery);
                          }

    /**\brief Helper for _write_master */
    static void         _create_master_chkpt_contents(
                            ostream&        s,
                            const vector<lsn_t>&    array
                            );

    /**\brief Helper for _make_master_name */
    static void         _create_master_chkpt_string(
                            ostream&        o,
                            const vector<lsn_t>&       array,
                            bool            old_style = false
                            );

    /**\brief Helper for _read_master */
    static rc_t         _parse_master_chkpt_contents(
                            istream&      s,
                            vector<lsn_t>&        lsnlist
                            );

    /**\brief Helper for _read_master */
    static rc_t         _parse_master_chkpt_string(
                            istream&      s,
                            lsn_t&        master_lsn,
                            lsn_t&        min_chkpt_rec_lsn,
                            vector<lsn_t>&        others,
                            bool&         old_style
                            );

    /**\brief Helper for parse_master_chkpt_string */
    static rc_t         _check_version(
                            uint32_t        major,
                            uint32_t        minor
                            );
    // helper for set_master
    void                _write_master(const lsn_t &l, const lsn_t &min);

    // used by implementation
    w_rc_t              _read_master(
                            const char *fname,
                            int prefix_len,
                            lsn_t &tmp,
                            lsn_t& tmp1,
                            vector<lsn_t>& lsnlist,
                            bool&  old_style
                            );
    void                _make_master_name(
                            const lsn_t&        master_lsn,
                            const lsn_t&        min_chkpt_rec_lsn,
                            char*               buf,
                            int                 bufsz,
                            bool                old_style = false);
    bool _partition_exists(partition_number_t pnum);


private:
    char*           _logdir;
    long            _segsize;
    fileoff_t               _partition_size;
    fileoff_t               _partition_data_size;
    lsn_t                   _min_chkpt_rec_lsn;

    partition_map_t _partitions;
    partition_number_t  _curr_num;
    partition_t* _curr_partition;

    skip_log*           _skip_log;
    lsn_t                   _master_lsn;
    mutable queue_based_block_lock_t _partition_lock;

    pthread_mutex_t     _scavenge_lock;
    pthread_cond_t      _scavenge_cond;

    w_rc_t          _set_partition_size(fileoff_t psize);

    int             get_last_lsns(vector<lsn_t>& array);

private:
    // forbid copy
    log_storage(const log_storage&);
    log_storage& operator=(const log_storage&);

public:
    enum { BLOCK_SIZE=partition_t::XFERSIZE };

    static const char    _SLASH;
    static const uint32_t  _version_major;
    static const uint32_t  _version_minor;
    static const char    _master_prefix[];
    static const char    _log_prefix[];
    static const char *master_prefix() { return _master_prefix; }
    static const char *log_prefix() { return _log_prefix; }

    void sanity_check() const;
};

#endif
