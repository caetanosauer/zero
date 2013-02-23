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

/** @file shore_file_desc.cpp
 *
 *  @brief Implementation of base class for describing
 *         Shore files (table/index).
 *
 *  @author: Mengzhi Wang, April 2001
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#include "sm/shore/shore_file_desc.h"
#include "sm/shore/shore_index.h"

ENTER_NAMESPACE(shore);



/******************************************************************** 
 *
 *  @class: file_desc_t
 *
 *  @brief: Contains the information needed to access a file in SM
 *
 ********************************************************************/

file_desc_t::file_desc_t(const char* name, 
                         const uint_t fcnt,
                         const uint4_t& apd)
    : _field_count(fcnt), _vid(vid_t::null), 
      _root_iid(stid_t::null), _pd(apd), _fid(stid_t::null)
{
    assert (fcnt>0);

    pthread_mutex_init(&_fschema_mutex, NULL);

    // Copy name
    memset(_name,0,MAX_FNAME_LEN);
#ifndef __GNUC__
    if(strlcpy(_name, name, MAX_FNAME_LEN) >= MAX_FNAME_LEN)
#else
    if(w_strlcpy(_name, name, MAX_FNAME_LEN) >= MAX_FNAME_LEN)
#endif
        {
            throw "file_desc_t::_name too long!\n";
        }
}

file_desc_t::~file_desc_t() 
{ 
    pthread_mutex_destroy(&_fschema_mutex);
}



/*********************************************************************
 *
 *  @fn:    find_root_iid 
 *
 *  @brief: Sets the volume fid and root iid.
 *
 *  @note:  Since it sets the static variables this function
 *          most likely will be called only once
 *
 *********************************************************************/
 
w_rc_t file_desc_t::find_root_iid(ss_m* /* db */)
{
#if 0 
    // Disabled returns the single vid
    vid_t  vid;    
    vid_t* vid_list = NULL;;
    u_int  vid_cnt = 0;
    
    assert (false);
    W_DO(db->list_volumes("device_name", vid_list, vid_cnt));

    if (vid_cnt == 0)
        return RC(se_VOLUME_NOT_FOUND);

    vid = vid_list[0];
    delete [] vid_list;
    _vid = vid;
#else

#warning IP: Still not multi-volume ready
    _vid = 1; /* explicitly set volume id = 1 */

#endif

    W_DO(ss_m::vol_root_index(_vid, _root_iid));
    return RCOK;
}


/*********************************************************************
 *
 *  @fn:    find_fid 
 *
 *  @brief: Sets the file fid given the file name
 *
 *********************************************************************/

w_rc_t file_desc_t::find_fid(ss_m* db)
{
    // if valid fid don't bother to lookup
    if (is_fid_valid())
        return RCOK;

    file_info_t   info;
    bool          found = false;
    smsize_t      infosize = sizeof(file_info_t);

    if (!is_root_valid()) W_DO(find_root_iid(db));
    
    W_DO(ss_m::find_assoc(root_iid(),
			  vec_t(_name, strlen(_name)),
			  &info, infosize,
			  found));
    _fid = info.fid();
    
    if (!found) {
        cerr << "Problem finding table " << _name << endl;
        return RC(se_TABLE_NOT_FOUND);
    }
    
    return RCOK;
}


w_rc_t index_desc_t::find_fid(ss_m* db, int pnum) 
{
    assert(pnum >= 0 && pnum < _partition_count);
    if(is_partitioned()) {
	// if valid fid don't bother to lookup
	if (is_fid_valid(pnum))
	    return RCOK;

	file_info_t   info;
	bool          found = false;
	smsize_t      infosize = sizeof(file_info_t);

	if (!_base.is_root_valid()) W_DO(_base.find_root_iid(db));

	char tmp[100];
	sprintf(tmp, "%s_%d", _base._name, pnum);
	W_DO(ss_m::find_assoc(_base.root_iid(),
			      vec_t(tmp, strlen(tmp)),
			      &info, infosize,
			      found));
	_partition_stids[pnum] = info.fid();
        
	if (!found) {
	    cerr << "Problem finding index " << tmp << endl;
	    return RC(se_TABLE_NOT_FOUND);
	}

	return RCOK;
    }
    else {
	return _base.find_fid(db);
    }
}




/******************************************************************** 
 *
 *  @class: file_info_t
 *
 *  @brief: Structure that representsa SM file in a volume
 *
 ********************************************************************/
 

file_info_t::file_info_t(const stid_t& fid,
                         const char* fname, 
                         const file_type_t ftype)
    : _ftype(ftype),
      _record_size(std::pair<int,int>(0,0)),
      _first_rid(0, shrid_t(0,0,0)),
      _fid(fid)
{

#ifndef __GNUC__
    if(strlcpy(_fname, fname, MAX_FNAME_LEN) >= MAX_FNAME_LEN)
#else
        if(w_strlcpy(_fname, fname, MAX_FNAME_LEN) >= MAX_FNAME_LEN)
#endif
            {
                throw "file_info_t::_fname too long!\n";
            }
}

file_info_t::file_info_t()
    : _ftype(FT_HEAP),
      _record_size(std::pair<int,int>(0,0)),
      _first_rid(0, shrid_t(0,0,0)),
      _cur_rid(0, shrid_t(0,0,0))
{ }



EXIT_NAMESPACE(shore);
