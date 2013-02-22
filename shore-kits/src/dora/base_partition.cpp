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

/** @file:   base_partition.cpp
 *
 *  @brief:  Interface for DORA partitions
 *
 *  @author: Ippokratis Pandis, Aug 2010
 */


#include "dora/base_partition.h"


ENTER_NAMESPACE(dora);


base_partition_t::base_partition_t(ShoreEnv* env, table_desc_t* ptable, 
                                   const uint apartid, 
                                   const processorid_t aprsid) 
    : _env(env), _table(ptable), 
      _part_id(apartid), _part_policy(PP_UNDEF), 
      _pat_state(PATS_UNDEF), _pat_count(0),
      _standby_cnt(DF_NUM_OF_STANDBY_THRS),
      _prs_id(aprsid)          
{
    assert (_env);
    assert (_table);
}


base_partition_t::~base_partition_t() 
{ 
}    


void base_partition_t::set_part_id(const uint pid)
{
    _part_id = pid;
}

// Partition policy
ePartitionPolicy base_partition_t::get_part_policy() 
{ 
    return (_part_policy); 
}

void base_partition_t::set_part_policy(const ePartitionPolicy aPartPolicy) 
{
    assert (aPartPolicy!=PP_UNDEF);
    _part_policy = aPartPolicy;
}


// Information about the partition
void base_partition_t::dump() 
{
    TRACE( TRACE_DEBUG, "Policy            (%d)\n", _part_policy);
    TRACE( TRACE_DEBUG, "Active Thr Status (%d)\n", _pat_state);
    TRACE( TRACE_DEBUG, "Active Thr Count  (%d)\n", _pat_count);
}


// active threads functions

/****************************************************************** 
 *
 * @fn:    get_pat_state()
 *
 * @brief: Returns the state of the partition 
 *         (single or multiple threads active)
 *
 ******************************************************************/

ePATState base_partition_t::get_pat_state() 
{ 
    return (*&_pat_state); 
}



/****************************************************************** 
 *
 * @fn:     dec_active_thr()
 *
 * @brief:  Decreases active thread count by 1.
 *          If thread_count is 1, changes state to PATS_SINGLE
 *
 * @return: Retuns the updated state of the partition
 *
 ******************************************************************/

ePATState base_partition_t::dec_active_thr() 
{
    assert (_pat_count>1);
    assert (_pat_state==PATS_MULTIPLE);
    CRITICAL_SECTION(pat_cs, _pat_count_lock);
    _pat_count--;
    if (_pat_count==1) {
        _pat_state = PATS_SINGLE;
        return (PATS_SINGLE);
    }
    assert (_pat_count>0);
    return (PATS_MULTIPLE);
}


/****************************************************************** 
 *
 * @fn:     inc_active_thr()
 *
 * @brief:  Increases active thread count by 1.
 *          If thread_count is >1, changes state to PATS_MULTIPLE
 *
 * @return: Retuns the updated state of the partition
 *
 ******************************************************************/

ePATState base_partition_t::inc_active_thr() 
{
    CRITICAL_SECTION(pat_cs, _pat_count_lock);
    _pat_count++;
    if (_pat_count>1) {
        _pat_state = PATS_MULTIPLE;
        return (PATS_MULTIPLE);
    }
    assert (_pat_count>0);
    return (_pat_state);
}

EXIT_NAMESPACE(dora);

