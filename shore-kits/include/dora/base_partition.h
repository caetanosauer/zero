/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
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

/** @file:   base_partition.h
 *
 *  @brief:  Interface of each table partition in DORA
 *
 *  @author: Ippokratis Pandis, Aug 2010
 */

#ifndef __DORA_BASE_PARTITION_H
#define __DORA_BASE_PARTITION_H


#include <cstdio>

#include "util.h"

#include "dora/common.h"
#include "dora/base_action.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_table.h"
#include "sm/shore/srmwqueue.h"

using namespace shore;


ENTER_NAMESPACE(dora);


/******************************************************************** 
 *
 * @enum:  ePATState
 *
 * @brief: Working thread status for the partition
 *
 *         - PATS_SINGLE   : only the primary-owner thread is active
 *         - PATS_MULTIPLE : some of the standby threads are active
 *
 ********************************************************************/

enum ePATState { PATS_UNDEF    = 0x1, 
                 PATS_SINGLE   = 0x2, 
                 PATS_MULTIPLE = 0x4 
};


/******************************************************************** 
 *
 * @class: base_partition_t
 *
 * @brief: Non-template based interface for the data partitions
 *
 ********************************************************************/

class base_partition_t
{
protected:

    ShoreEnv*       _env;    
    table_desc_t*   _table;

    uint             _part_id;
    ePartitionPolicy _part_policy;

    // partition active thread state and count
    ePATState volatile _pat_state; 
    uint volatile      _pat_count;
    tatas_lock         _pat_count_lock;

    // protecting primary owner and pool of standby worker threads
    tatas_lock    _owner_lock;
    tatas_lock    _standby_lock;
    uint          _standby_cnt;

    // processor binding
    processorid_t _prs_id;

public:

    base_partition_t(ShoreEnv* env, table_desc_t* ptable, 
                     const uint apartid, 
                     const processorid_t aprsid);

    virtual ~base_partition_t();

    uint part_id() const { return (_part_id); }
    void set_part_id(const uint pid);
    table_desc_t* table() const { return (_table); } 

    // partition policy
    ePartitionPolicy get_part_policy();
    void set_part_policy(const ePartitionPolicy aPartPolicy);

    // partition state and active threads
    ePATState get_pat_state();
    ePATState dec_active_thr();
    ePATState inc_active_thr();

    // Partition Interface //

    // enqueue lock needed to enforce an ordering across trxs
    mcs_lock _enqueue_lock;

    // the status of the queues
    virtual int has_input(void) const=0;
    virtual int has_committed(void) const=0;

    // dequeueing from the queues
    virtual base_action_t* dequeue()=0;
    virtual base_action_t* dequeue_commit()=0;

    // resets/initializes the partition, possibly to a new processor
    virtual int reset(const processorid_t aprsid, const uint standby_sz)=0;
 
    // Goes over all the actions and aborts them
    virtual int abort_all_enqueued()=0;

    // stops the partition
    virtual void stop()=0;

    // prepares the partition for a new run
    virtual w_rc_t prepareNewRun()=0;

    // stats
    virtual void statistics(worker_stats_t& gather)=0;
    virtual void stlsize(uint& gather)=0;

    // dumps information
    virtual void dump();

}; // EOF: base_partition_t

EXIT_NAMESPACE(dora);

#endif /** __DORA_PARTITION_H */

