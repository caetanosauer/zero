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

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

class skip_log;

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
    partition_t*    curr_partition() const;
    long            prime(char* buf, lsn_t next, size_t block_size);
    void            acquire_partition_lock();
    void            release_partition_lock();

    partition_t *       get_partition(partition_number_t n) const;

    static long         floor2(long offset, long block_size)
                            { return offset & -block_size; }
    static long         ceil2(long offset, long block_size)
                           { return
                               floor2(offset + block_size - 1, block_size); }


    const char *        dir_name() { return _logdir; }

    // used by partition_t
    skip_log*       get_skip_log()  { return _skip_log; }

    fileoff_t           partition_data_size() const {
                            return _partition_data_size; }

    /**\brief used by partition */
    fileoff_t limit() const { return _partition_size; }

    string make_log_name(partition_number_t pnum) const;
    fs::path make_log_path(partition_number_t pnum) const;

    static long         _floor(long offset, long block_size)
                            { return (offset/block_size)*block_size; }
    static long         _ceil(long offset, long block_size)
                            { return _floor(offset + block_size - 1, block_size); }

    static fileoff_t          partition_size(long psize);
    static fileoff_t          min_partition_size();
    static fileoff_t          max_partition_size();

private:
    void                _prime(int fd, fileoff_t start, lsn_t next);
    partition_t* create_partition(partition_number_t pnum);

private:
    fs::path        _logpath;
    char*           _logdir;
    long            _segsize;
    fileoff_t               _partition_size;
    fileoff_t               _partition_data_size;

    partition_map_t _partitions;
    partition_t* _curr_partition;

    skip_log*           _skip_log;
    mutable queue_based_block_lock_t _partition_lock;

    w_rc_t          _set_partition_size(fileoff_t psize);

private:
    // forbid copy
    log_storage(const log_storage&);
    log_storage& operator=(const log_storage&);

public:
    enum { BLOCK_SIZE=partition_t::XFERSIZE };
    static const string log_prefix;
    static const string log_regex;
};

#endif
