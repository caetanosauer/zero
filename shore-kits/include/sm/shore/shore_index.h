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

/** @file:   shore_index.h
 *
 *  @brief:  Description of an index.
 *
 *  All the secondary indexes on the table are linked together.  
 *  An index is described by an array of serial number of fields.
 *
 *  @author: Mengzhi Wang, April 2001
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __SHORE_INDEX_H
#define __SHORE_INDEX_H

#include "sm_vas.h"
#include "util.h"

#include "sm/shore/shore_error.h"
#include "sm/shore/shore_file_desc.h"
#include "sm/shore/shore_iter.h"


ENTER_NAMESPACE(shore);


class table_desc_t;


/******************************************************************
 *
 *  @class: index_desc_t
 *
 *  @brief: Description of a Shore index.
 *
 *  @note:  Even the variable length fields are treated as fixed 
 *          length, with their maximum possible size.
 *
 ******************************************************************/

class index_desc_t 
{
    friend class table_desc_t;

private:
    file_desc_t _base;

    uint*           _key;                      /* index of fields in the base table */
    bool            _unique;                   /* whether allow duplicates or not */
    bool            _primary;                  /* is it primary or not */
    bool            _nolock;                   /* is it using locking or not */ 
    uint            _mr;                       /* is it multi-rooted */ 
    bool            _latchless;                /* does it use any latches at all */ 
    bool            _rmapholder;               /* it is used only for the range mapping */

    index_desc_t*   _next;                     /* linked list of all indices */

    char            _keydesc[MAX_KEYDESC_LEN]; /* buffer for the index key description */
    tatas_lock      _keydesc_lock;             /* lock for the key desc */
    volatile uint_t _maxkeysize;               /* maximum key size */

    /* if _partition_count > 1, _partition_stids contains stids for
       all N partitions, otherwise it's NULL
     */
    int		  _partition_count;
    stid_t*	  _partition_stids;

public:

    /* ------------------- */
    /* --- constructor --- */
    /* ------------------- */

    index_desc_t(const char* name, const int fieldcnt, 
                 int partitions, const uint* fields,
                 bool unique=true, bool primary=false, 
                 const uint4_t& pd=PD_NORMAL,
                 bool rmapholder=false);

    ~index_desc_t();


    /* --------------------------------- */
    /* --- exposed inherited methods --- */
    /* --------------------------------- */

    const char*  name() const { return _base.name(); }
    uint_t field_count() const { return _base.field_count(); }
    


    /* -------------------------- */
    /* --- overridden methods --- */
    /* -------------------------- */

    inline w_rc_t check_fid(ss_m* db) {
	if(!is_partitioned())
	    return _base.check_fid(db);
	
	// check all the partition fids...
	for(int i=0; i < _partition_count; i++) {
	    if (!is_fid_valid(i)) {
		if (!_base.is_root_valid())
		    W_DO(_base.find_root_iid(db));
		W_DO(find_fid(db, i));
	    }
        }
        return (RCOK);
    }

    stid_t&	fid(int const pnum) {
	assert(pnum >= 0 && pnum < _partition_count);
	return is_partitioned()? _partition_stids[pnum] : _base.fid();
    }	
    w_rc_t	find_fid(ss_m* db, int pnum);
    
    bool is_fid_valid(int pnum) const {
	assert(pnum >= 0 && pnum < _partition_count);
	return is_partitioned()? _partition_stids[pnum] != stid_t::null : _base.is_fid_valid();
    }
    void set_fid(int const pnum, stid_t const &fid);	
    
    /* ---------------------- */
    /* --- access methods --- */
    /* ---------------------- */

    uint4_t get_pd() const { return (_base.get_pd()); }
    inline bool is_unique() const { return (_unique); }
    inline bool is_primary() const { return (_primary); }
    inline bool is_relaxed() const { return (_nolock); }
    inline bool is_mr() const { return (_mr); }
    inline bool is_latchless() const { return (_latchless); }
    inline bool is_rmapholder() const { return (_rmapholder); }
    inline bool is_partitioned() const { return _partition_count > 1; }

    inline int  get_partition_count() const { return _partition_count; }
    inline int  get_keysize() { return (*&_maxkeysize); }
    inline void set_keysize(const uint_t sz) { atomic_swap_uint(&_maxkeysize, sz); }


    /* ---------------------------------- */
    /* --- index link list operations --- */
    /* ---------------------------------- */

    index_desc_t* next() const;

    // total number of indexes on the table
    int index_count() const;

    // insert a new index after the current index
    void insert(index_desc_t* new_node);

    // find the index_desc_t by name
    index_desc_t* find_by_name(const char* name);

    int key_index(const uint_t index) const;


    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    void print_desc(ostream& os);

}; // EOF: index_desc_t



EXIT_NAMESPACE(shore);

#endif /* __SHORE_INDEX_H */
