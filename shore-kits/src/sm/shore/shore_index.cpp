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

/** @file shore_index.cpp
 *
 *  @brief Implementation of shore_index class
 *
 *  @author: Ippokratis Pandis, Oct 2010
 *
 */

#include "sm/shore/shore_index.h"

using namespace shore;


/****************************************************************** 
 *
 *  class index_desc_t methods 
 *
 ******************************************************************/


index_desc_t::index_desc_t(const char* name, const int fieldcnt, 
                           int partitions, const uint* fields,
                           bool unique, bool primary, 
                           const uint4_t& pd,
                           bool rmapholder)
    : _base(name, fieldcnt, pd),
      _unique(unique), _primary(primary),
      _rmapholder(rmapholder),
      _next(NULL), _maxkeysize(0),
      _partition_count((partitions > 0)? partitions : 1), _partition_stids(0)
{
    // Copy the indexes of keys
    _key = new uint[_base._field_count];
    for (uint_t i=0; i<_base._field_count; i++) _key[i] = fields[i];

    memset(_keydesc, 0, MAX_KEYDESC_LEN);

    // start setting up partitioning
    if(is_partitioned()) {
        _partition_stids = new stid_t[_partition_count];
        for(int i=0; i < _partition_count; i++)
            _partition_stids[i] = stid_t::null;
    }


    // Update the flags depending on the physical type information

    // Check if MR (frequently asked)
    _mr = (pd & (PD_MRBT_NORMAL | PD_MRBT_PART | PD_MRBT_LEAF));
    // if (pd & (PD_MRBT_NORMAL | PD_MRBT_PART | PD_MRBT_LEAF)) {
    //     _mr = true;
    // }
    // else {
    //     _mr = false;
    // }

    // Check if NoLock
    _nolock = (pd & PD_NOLOCK);
    // if (pd & PD_NOLOCK) {
    //     _nolock = true;
    // }
    // else {
    //     _nolock = false;
    // }

    // Check if Latch-less
    _latchless = (pd & PD_NOLATCH);
    // if (pd & PD_NOLATCH) {
    //     _latchless = true;
    // }
    // else {
    //     _latchless = false;
    // }

    // If it is a RangeMap holder, then this empty index should be
    // MRBT-* and not manually partitioned
    w_assert0( (!_rmapholder) || (_mr && !is_partitioned()));

    // Set the flag that it is only a RangeMap holder
    // if (rmapholder) {
    //     if (!(_mr) || is_partitioned()) {
    //         // This empty index should be MRBT-* and not manually partitioned
    //         assert(0);
    //     }
    //     else {
    //         _rmapholder = true;
    //     }
    // }           
}


index_desc_t::~index_desc_t() 
{ 
    if (_key) {
        delete [] _key; 
        _key = NULL;
    }

    // The deletes propagate to the next
    if (_next) {
        delete _next;
        _next = NULL;
    }
	
    if(_partition_stids) {
        delete [] _partition_stids;
        _partition_stids = NULL;
    }
}



/****************************************************************** 
 *  
 *  @fn:    set_fid
 *
 *  @brief: Sets a particular fid to the index (or index partition)
 *
 ******************************************************************/

void index_desc_t::set_fid(int const pnum, stid_t const &fid) 
{
    assert(pnum >= 0 && pnum < _partition_count);
    if (is_partitioned()) {
        _partition_stids[pnum] = fid;
    }
    else {
        _base.set_fid(fid);
    }
}



/****************************************************************** 
 *  
 *  Index linked list operations
 *
 ******************************************************************/

index_desc_t* index_desc_t::next() const 
{ 
    return _next; 
}


// total number of indexes on the table
int index_desc_t::index_count() const 
{ 
    return (_next ? _next->index_count()+1 : 1); 
}


// insert a new index after the current index
void index_desc_t::insert(index_desc_t* new_node) 
{
    new_node->_next = _next;
    _next = new_node;
}


// find the index_desc_t by name
index_desc_t* index_desc_t::find_by_name(const char* name) 
{
    if (strcmp(name, _base._name) == 0) return (this);
    if (_next) return _next->find_by_name(name);
    return (NULL);
}

int index_desc_t::key_index(const uint_t index) const 
{
    assert (index < _base._field_count);
    return (_key[index]);
}



/****************************************************************** 
 *  
 *  Debugging
 *
 ******************************************************************/

// For debug use only: print the description for all the field
void index_desc_t::print_desc(ostream& os)
{
    os << "Schema for index " << name() << endl;
    uint fc = field_count();
    os << "Numer of fields: " << fc << endl;
    for (uint_t i=0; i< fc; i++) {
	os << _keydesc[i] << "|";
    }
    os << endl;
}


#include <sstream>
char const* db_pretty_print(index_desc_t const* ptdesc, int /* i=0 */, char const* /* s=0 */) 
{
    static char data[1024];
    std::stringstream inout(data, stringstream::in | stringstream::out);
    //std::strstream inout(data, sizeof(data));
    ((index_desc_t*)ptdesc)->print_desc(inout);
    inout << std::ends;
    return data;
}


