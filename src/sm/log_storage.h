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

#include "sm_options.h"
#include <partition.h>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>

typedef    smlevel_0::partition_number_t partition_number_t;
typedef std::map<partition_number_t, shared_ptr<partition_t>> partition_map_t;

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

class skip_log;
class partition_recycler_t;

class log_storage {

    typedef smlevel_0::fileoff_t fileoff_t;

    // use friend mechanism until better interface is implemented
    friend class partition_t;
    friend class partition_recycler_t;

public:
    log_storage(const sm_options&);
    virtual ~log_storage();

    shared_ptr<partition_t>    get_partition_for_flush(lsn_t start_lsn,
                            long start1, long end1, long start2, long end2);
    shared_ptr<partition_t>    curr_partition() const;

    shared_ptr<partition_t>       get_partition(partition_number_t n) const;

    // used by partition_t
    skip_log*       get_skip_log()  { return _skip_log; }

    fileoff_t get_partition_size() const { return _partition_size; }

    string make_log_name(partition_number_t pnum) const;
    fs::path make_log_path(partition_number_t pnum) const;
    fs::path make_chkpt_path(lsn_t lsn) const;

    void add_checkpoint(lsn_t lsn);

    void wakeup_recycler(bool chkpt_only = false);
    unsigned delete_old_partitions(bool chkpt_only = false, partition_number_t older_than = 0);

private:
    shared_ptr<partition_t> create_partition(partition_number_t pnum);

    fs::path _logpath;
    fileoff_t _partition_size;

    partition_map_t _partitions;
    shared_ptr<partition_t> _curr_partition;

    vector<lsn_t> _checkpoints;

    skip_log* _skip_log;

    unsigned _max_partitions;
    bool _delete_old_partitions;

    // forbid copy
    log_storage(const log_storage&);
    log_storage& operator=(const log_storage&);

    void try_delete(partition_number_t);

    // Latch to protect access to partition map
    mutable mcs_rwlock _partition_map_latch;

    unique_ptr<partition_recycler_t> _recycler_thread;

public:
    enum { BLOCK_SIZE = partition_t::XFERSIZE };
    static const string log_prefix;
    static const string log_regex;
    static const string chkpt_prefix;
    static const string chkpt_regex;
};

#endif
