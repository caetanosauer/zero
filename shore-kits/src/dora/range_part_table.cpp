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

/** @file:   range_part_table.cpp
 *
 *  @brief:  Range partitioned table class in DORA
 *
 *  @date:   Aug 2010
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#include "dora/range_part_table.h"
#include "dora/dora_error.h"

using namespace shore;


ENTER_NAMESPACE(dora);


/****************************************************************** 
 *
 * @fn:    constructor
 *
 * @brief: Populates the key-range map and creates one partition per
 *
 ******************************************************************/

range_table_t::range_table_t(ShoreEnv* env, table_desc_t* ptable, const uint dtype,
                             const processorid_t aprs,
                             const uint acpurange,
                             const uint keyEstimation) 
    : part_table_t(env,ptable,aprs,acpurange,keyEstimation), _dtype(dtype)
{
    _prMap = NULL;
}


range_table_t::~range_table_t()
{
}



/****************************************************************** 
 *
 * @fn:    repartition()
 *
 * @brief: This function does four things:
 *         (1) Update the new range partitioning information
 *         (2) Readjusts the boundaries of existis partitions
 *         (3) Deletes partitions that are in excess
 *         (4) Creates partitions if more need to be created
 *
 * @note:  Assumes that the partitioned table lock is being held by
 *         the called (prepareNewRun())
 *
 ******************************************************************/

w_rc_t range_table_t::repartition()
{
    // Read the updated partitioning information and if different from the
    // one currently used, update.
    dkey_ranges_map* drm = NULL;
    W_DO(_get_updated_map(drm));
    assert(drm);
    if ((_prMap != NULL) && (_prMap->is_same(*drm))) {
        TRACE( TRACE_STATISTICS, "Not partitioning changes in (%s)\n", 
               _table->name());
        delete (drm);
        return (RCOK);
    }

    // There has been a change in partitioning information, go and modify 
    // logical partitions
    _prMap = drm;    
    assert (_prMap);

    // Save the old mapping to a temp map
    BasePartitionPtrMap tmpmap = _bppmap;

    // Clear the mapping and repopulate it
    _bppmap.clear();

    // Get a vector of the shpid_t (partition ids)
    vector<lpid_t> pidVec;
    W_DO(_prMap->get_all_partitions(pidVec));    

    // Start iterating over all the old partitions 
    // and setting the values to the new _bppvec
    vector<lpid_t>::iterator pidIt =  pidVec.begin();
    BPPMapIt oldPartIt = tmpmap.begin();
    uint cnt=0;

    while ((pidIt != pidVec.end()) && (oldPartIt != tmpmap.end())) {
        // As long as we have both already created old partitions and new pids
        // insert entries in the new map
        // Essentially, we do a repositioning of the pointers in the map 
        _bppmap[(*pidIt).page] = (*oldPartIt).second;
        pidIt++;
        oldPartIt++;
        cnt++;
    }

    if (cnt>0) {
        TRACE( TRACE_STATISTICS, "Repositioned (%d) (%s) partitions\n",
               cnt, _table->name());
        cnt=0;
    }            
               

    while (oldPartIt != tmpmap.end()) {
        // There are some old partitions that need to be stopped and destroyed
        (*oldPartIt).second->stop();
        delete ((*oldPartIt).second);
        oldPartIt++;
        cnt++;
    }

    if (cnt>0) {
        TRACE( TRACE_STATISTICS, "Deleted (%d) (%s) partitions\n",
               cnt, _table->name());
        cnt=0;
    }            


    while (pidIt != pidVec.end()) {
        // There are some partitions that need to be created

        // The create_one_part() will create one partition and will put the
        // pointer to the corresponding place of the map
        base_partition_t* abp = NULL;
        W_DO(create_one_part((*pidIt).page,abp)); 
        assert (abp);
        _bppmap[(*pidIt).page] = abp;
        pidIt++;
        cnt++;
    }

    if (cnt>0) {
        TRACE( TRACE_STATISTICS, "Created (%d) (%s) partitions\n",
               cnt, _table->name());
        cnt=0;
    }            
    return (RCOK);
}



/****************************************************************** 
 *
 * @fn:    _get_updated_map()
 *
 * @brief: Read the updated key map, depending on the flavor of DORA used 
 *         (plain, plp*)
 *         - If PLP*, from the sm::range_map
 *         - If DORA, from the dkey_map
 *
 ******************************************************************/

w_rc_t range_table_t::_get_updated_map(dkey_ranges_map*& drm)
{
    w_rc_t r = RCOK;
    
    rangemap_smt_t* rsm = new rangemap_smt_t(_env,_table,_dtype);
    assert (rsm);
    rsm->fork();
    rsm->join();

    r = rsm->_rc;
    if (!r.is_error()) { drm = rsm->_drm; }

    delete(rsm);
    rsm = NULL;

    return (r);
}


/****************************************************************** 
 *
 * @fn:    create_one_part()
 *
 * @brief: Creates one partition and adds it to the vectors
 *
 * @note:  Assumes that the partitioned table lock is being held by
 *         the called (readjust())
 *
 ******************************************************************/

w_rc_t range_table_t::create_one_part(const shpid_t& pid, base_partition_t*& abp)
{   
    // Create a new partition object with a specific index
    w_rc_t r = _create_one_part(pid,abp);

    if (r.is_error()) {
        TRACE( TRACE_ALWAYS, "Problem in creating partition for (%s)\n", 
               _table->name());
        return (RC(de_GEN_PARTITION));
    }    

    // Update next cpu
    PartTable::_next_prs_id = PartTable::next_cpu(PartTable::_next_prs_id);
    return (r);
}


EXIT_NAMESPACE(dora);

