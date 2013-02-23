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

/** @file:   dkey_ranges_map.h
 *
 *  @brief:  Wrapper of the key_ranges_map, used by DORA
 *
 *  @date:   Aug 2010
 *
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Pinar Tozun (pinar)
 *  @author: Ryan Johnson (ryanjohn)
 */


#ifndef __DORA_DKEY_RANGES_MAP_H
#define __DORA_DKEY_RANGES_MAP_H

#include "sm_vas.h"
#include "util.h"

#include "dora/dora_error.h"
#include "dora/common.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_table.h"

using namespace std;
using namespace shore;


ENTER_NAMESPACE(dora);


/******************************************************************** 
 *
 * @class: dkey_ranges_map
 *
 * @brief: Thin wrapper of the key_ranges_map
 *
 * @note:  The key_ranges_map maps char* to lpid_t. We use the lpid_t.page
 *         (which is a uint4_t) as the partition id.  
 *
 ********************************************************************/

class dkey_ranges_map
{
private: 

    bool _isplp;    
    key_ranges_map* _rmap;
    sinfo_s _sinfo;
    
public:
 
    // There are two constructors:
    // One used by plain DORA, which is a main-memory data structure
    // One used by PLP, which is a persistently stored range map
    dkey_ranges_map(const stid_t& stid,
                    const cvec_t& minKeyCV, 
                    const cvec_t& maxKeyCV, 
                    const uint numParts);
    dkey_ranges_map(const stid_t& stid,key_ranges_map* pkrm);
    ~dkey_ranges_map();


    // Get the partition id for the given key, which is unscrambled
    inline w_rc_t get_partition(const cvec_t& cvkey, lpid_t& lpid) {
        return(_rmap->getPartitionByUnscrambledKey(_sinfo,cvkey,lpid));
    }

    w_rc_t get_all_partitions(vector<lpid_t>& pidVec);


    // RangeMap maintenance
    w_rc_t add_partition(const cvec_t& key, lpid_t& lpid);
    w_rc_t delete_partition(const lpid_t& lpid);

    // Returns true if they are same
    bool is_same(const dkey_ranges_map& drm);

private:

    // Not allowed
    dkey_ranges_map();
    
}; // EOF: dkey_ranges_map




/****************************************************************** 
 *
 *  @class: rangemap_smt_t
 *
 *  @brief: An smthread inherited class that it is used just for
 *          accessing the key_range_map of a specific store
 *
 ******************************************************************/

class rangemap_smt_t : public thread_t 
{
private:
    
    ShoreEnv* _env;    
    table_desc_t* _table;
    uint _dtype;

public:

    dkey_ranges_map* _drm;
    w_rc_t _rc;

    rangemap_smt_t(ShoreEnv* env, table_desc_t* tabledesc, uint dtype);
    ~rangemap_smt_t();
    void work();
    
}; // EOF: rangemap_smt_t





EXIT_NAMESPACE(dora);

#endif // __DORA_DKEY_RANGES_MAP_H
