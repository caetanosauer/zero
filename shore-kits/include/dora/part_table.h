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

/** @file:   part_table.h
 *
 *  @brief:  Declaration of each table in DORA.
 *
 *  @note:   Implemented as a vector of partitions. The routing information 
 *           (for example, table in the case of range partitioning) is stored
 *           at the specific sub-class (for example, range_part_table_impl)
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */


#ifndef __DORA_PART_TABLE_H
#define __DORA_PART_TABLE_H


#include <cstdio>

#include "util.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_table.h"

#include "dora/base_partition.h"

using namespace shore;


ENTER_NAMESPACE(dora);


/******************************************************************** 
 *
 * @class: part_table_t
 *
 * @brief: Abstract class for representing a table as a set of (data) partitions
 *
 ********************************************************************/

class part_table_t
{
public:

    typedef map<shpid_t, base_partition_t*>           BasePartitionPtrMap;
    typedef map<shpid_t, base_partition_t*>::iterator BPPMapIt;
    typedef map<shpid_t, base_partition_t*>::const_iterator BPPMapCIt;
    
protected:

    ShoreEnv*                _env;    
    table_desc_t*            _table;
    // The partitioning information is stored a the table_desc_t (_table)

    tatas_lock               _lock;

    // Vector of pointer to base partitions
    BasePartitionPtrMap   _bppmap;

    // processor binding
    processorid_t      _start_prs_id;
    processorid_t      _next_prs_id;
    uint               _prs_range;

    // per partition key estimation
    uint               _key_estimation;
   
public:

    part_table_t(ShoreEnv* env, table_desc_t* ptable,
                 const processorid_t aprs,
                 const uint acpurange,
                 const uint keyEstimation);

    virtual ~part_table_t();


    //// Control table ////

    // Stops all partitions
    virtual w_rc_t stop();

    // Prepares all partitions for a new run
    virtual w_rc_t prepareNewRun();

    // Return the appropriate partition. This decision is based on the type
    // of the partitioning scheme used and the DataType used for the routing 
    virtual w_rc_t getPartIdxByKey(const cvec_t& cvkey, lpid_t& pid)=0;

    // Re-adjustss partitions. It is called before new runs to make sure
    // that the logical partitions are in sync with the partitioning scheme
    // used by the system.
    virtual w_rc_t repartition()=0;

    //// CPU placement ////

    // reset all partitions
    virtual w_rc_t reset();
    
    // move to another range of processors
    w_rc_t move(const processorid_t aprs, const uint acpurange);

    // decide the next processor
    virtual processorid_t next_cpu(const processorid_t& aprd);

    table_desc_t* table() const;

    //// For debugging ////

    // information
    void statistics() const;

    // information
    void info() const;

    // dumps information
    void dump() const;

}; // EOF: part_table_t

EXIT_NAMESPACE(dora);

#endif /** __DORA_PART_TABLE_H */

