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

/** @file:   range_part_table.h
 *
 *  @brief:  Range partitioned table class in DORA
 *
 *  @date:   Oct 2008
 *
 *  @author: Ippokratis Pandis (ipandis)
 */


#ifndef __DORA_RANGE_PART_TABLE_H
#define __DORA_RANGE_PART_TABLE_H

#include "dora/dkey_ranges_map.h"

#include "dora/part_table.h"
#include "dora/base_partition.h"


using namespace shore;

ENTER_NAMESPACE(dora);


/******************************************************************** 
 *
 * @class: range_table_t
 *
 * @brief: Abstract class for a range data partitioned table
 *
 ********************************************************************/

class range_table_t : public part_table_t
{
public:

    typedef part_table_t PartTable;

protected:

    // Whether it is plain DORA or a PLP flavor
    uint _dtype;
    
    // key ranges map - The DORA version
    guard<dkey_ranges_map> _prMap;

public:

    range_table_t(ShoreEnv* env, table_desc_t* ptable, const uint dtype,
                  const processorid_t aprs,  
                  const uint acpurange,
                  const uint keyEstimation);

    ~range_table_t();

    virtual w_rc_t create_one_part(const shpid_t& pid, base_partition_t*& abp);


    // Wrappers of the getPartitionByKey. Return the idx of the partition
    // in the array of base_partitions (_bppvec)

    inline w_rc_t getPartIdxByKey(const cvec_t& cvkey, lpid_t& pid) {
        return (_prMap->get_partition(cvkey,pid));
    }

    // Reads the updated range partitioning information (if plp* from the sm::range_map_keys,
    // if dora from the dkeymap) and adjusts the boundaries of all logical partitions.
    // If needed, creates new partitions.
    w_rc_t repartition();

private:

    w_rc_t _get_updated_map(dkey_ranges_map*& drm);

protected:

    virtual w_rc_t _create_one_part(const shpid_t& pid, base_partition_t*& abp)=0;

}; // EOF: range_table_t


EXIT_NAMESPACE(dora);

#endif /** __DORA_RANGE_PART_TABLE_H */

