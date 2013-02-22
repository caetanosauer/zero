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

/** @file:   dkey_ranges_map.cpp
 *
 *  @brief:  Thin wrapper of the key_ranges_map
 *
 * @note:  The key_ranges_map maps char* to lpid_t. We use the lpid_t.page
 *         (which is a uint4_t) as the partition id.  
 *
 *  @date:   Aug 2010
 *
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Pinar Tozun (pinar)
 *  @author: Ryan Johnson (ryanjohn)
 */


#include "dora/dkey_ranges_map.h"

#include "sm/shore/shore_env.h"


ENTER_NAMESPACE(dora);


/****************************************************************** 
 *
 * Constructors
 *
 ******************************************************************/

// This should be called when in plain DORA
dkey_ranges_map::dkey_ranges_map(const stid_t& stid,
                                 const cvec_t& minKey, 
                                 const cvec_t& maxKey, 
                                 const uint numParts)
    : _isplp(false)
{
    w_rc_t e = ss_m::get_store_info(stid,_sinfo);
    if (e.is_error()) {  assert(0); }
    _rmap = new key_ranges_map(_sinfo,minKey,maxKey,numParts,false);
}


// This should be called when in PLP
dkey_ranges_map::dkey_ranges_map(const stid_t& stid,
                                 key_ranges_map* krm)
    : _isplp(true)
{
    _rmap = new key_ranges_map();
    *(_rmap) = *(krm);

    w_rc_t e = ss_m::get_store_info(stid,_sinfo);
    if (e.is_error()) {  assert(0); }
}

dkey_ranges_map::~dkey_ranges_map()
{
    if ((!_isplp) && (_rmap)) {
        delete (_rmap);
        _rmap = NULL;
    }
}


/****************************************************************** 
 *
 * @fn:    get_all_partitions()
 *
 * @brief: Returns the list of pids for all the partitions
 *
 ******************************************************************/

w_rc_t dkey_ranges_map::get_all_partitions(vector<lpid_t>& pidVec)
{
    return(_rmap->getAllPartitions(pidVec));
}


/****************************************************************** 
 *
 * @fn:    add_partition()
 *
 * @brief: Splits the partition where "key" belongs to two partitions. 
 *         The start of the second partition is the "key".
 *
 ******************************************************************/

w_rc_t dkey_ranges_map::add_partition(const cvec_t& key, lpid_t& lpid)
{
    return(_rmap->addPartition(key,lpid));
}


/****************************************************************** 
 *
 * @fn:    delete_partition()
 *
 * @brief: Deletes a partition based on the partition number.
 *
 * @note:  The deletion is achieved by merging the partition with the
 *         partition which is before that, unless the partition is 
 *         the left-most partition.
 *
 ******************************************************************/

w_rc_t dkey_ranges_map::delete_partition(const lpid_t& /* lpid */)
{
    assert (0); // TODO check key_ranges_map API
    //return(_rmap->deletePartition(lpid));
    return (RCOK);
}


/****************************************************************** 
 *
 * @fn:    is_same()
 *
 ******************************************************************/

bool dkey_ranges_map::is_same(const dkey_ranges_map& drm) 
{
#warning IP: For now, is_same() checks only the number of partitions
    return (_rmap->getNumPartitions() == drm._rmap->getNumPartitions());
}




/****************************************************************** 
 *
 * @class: rangemap_smt_t
 *
 * @brief: Read the updated key map, depending on the flavor of DORA used 
 *         (plain, plp*)
 *         - If PLP*, from the sm::range_map
 *         - If DORA, from the dkey_map
 *
 ******************************************************************/
 
rangemap_smt_t::rangemap_smt_t(ShoreEnv* env, table_desc_t* tabledesc, uint dtype) 
    : thread_t(c_str("RMS")), 
      _env(env), _table(tabledesc), _dtype(dtype),
      _drm(NULL), _rc(RCOK)
{
}

rangemap_smt_t::~rangemap_smt_t() 
{
}

void rangemap_smt_t::work()
{
    assert (_env);

    tid_t atid;
    _rc = _env->db()->begin_xct(atid);
    
    if (!_rc.is_error()) {

        if (_dtype & DT_PLAIN) {
            // Plain DORA. Need to populate a dkey_map, given the boundaries
            // of the _table and the number of partitions
        
            stid_t stid = _table->get_primary_stid();
            cvec_t mincv(_table->getMinKey(),_table->getMinKeyLen());
            cvec_t maxcv(_table->getMaxKey(),_table->getMaxKeyLen());
            uint pcnt = _table->pcnt();

            _drm = new dkey_ranges_map(stid,mincv,maxcv,pcnt);
        }
        else {
            if (_dtype & DT_PLP) {
                // A PLP flavor. Read the possibly updated RangeMap from sm and 
                // populate the corresponding dkey_map

                key_ranges_map* arm;
                _rc = _table->get_main_rangemap(arm);
                stid_t stid = _table->get_primary_stid();

                _drm = new dkey_ranges_map(stid,arm);
            }
        }
    }

    if (_rc.is_error()) {
        _rc = _env->db()->abort_xct();
    }
    else {
        _rc = _env->db()->commit_xct();
    }
}    




EXIT_NAMESPACE(dora);
