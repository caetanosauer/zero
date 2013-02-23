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

/** @file:   range_table_i.h
 *
 *  @brief:  Template-based implementation of range-partitioned tables in DORA
 *
 *  @date:   Aug 2010
 *
 *  @author: Ippokratis Pandis (ipandis)
 */


#ifndef __DORA_RANGE_TABLE_I_H
#define __DORA_RANGE_TABLE_I_H

#include "dora/key.h"
#include "dora/partition.h"
#include "dora/action.h"
#include "dora/range_part_table.h"

using namespace shore;


ENTER_NAMESPACE(dora);


/******************************************************************** 
 *
 * @class: range_table_i
 *
 * @brief: Template-based class for a range data partitioned table
 *
 ********************************************************************/

template <class DataType>
class range_table_i : public range_table_t
{
public:

    typedef key_wrapper_t<DataType>     DKey;
    typedef action_t<DataType>          Action;
    typedef partition_t<DataType>       rpImpl;

    typedef map< shpid_t, partition_t<DataType>* >  rpImplPtrMap;

protected:
   
    // The map of pages --> pointers to partitions
    rpImplPtrMap _pmap;

public:

    range_table_i(ShoreEnv* env, table_desc_t* ptable, const uint dtype,
                  const processorid_t aprs,  
                  const uint acpurange,
                  const uint keyEstimation)
        : range_table_t(env,ptable,dtype,aprs,acpurange,keyEstimation)
    { 
    }

    ~range_table_i() { }

    rpImpl* get(const shpid_t& pid) { return (_pmap[pid]); }

protected:

    w_rc_t _create_one_part(const shpid_t& pid, base_partition_t*& abp);

}; // EOF: range_table_i


/****************************************************************** 
 *
 * @fn:    _create_one_part()
 *
 * @brief: Creates one (template-based) partition and adds it to the
 *         vector
 *
 * @note:  Assumes that a mutex is already held by the caller 
 *
 ******************************************************************/

template <class DataType>
w_rc_t range_table_i<DataType>::_create_one_part(const shpid_t& pid,
                                                 base_partition_t*& abp)
{   
    // Create the partition
    rpImpl* prp = new rpImpl(PartTable::_env, PartTable::_table, pid,
                             PartTable::_next_prs_id,
                             PartTable::_key_estimation);
    if (!prp) {
        TRACE( TRACE_ALWAYS, "Problem in creating partition (%d)\n", pid);
        return (RC(de_GEN_PARTITION));
    }

    // Save it as a base partition
    abp = prp;

    // Update the map
    _pmap[pid] = prp;

    // And reset the partition
    prp->reset();
    return (RCOK);
}

EXIT_NAMESPACE(dora);

#endif /** __DORA_RANGE_TABLE_I_H */

