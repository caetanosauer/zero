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
 *  @author: Caetano Sauer, April 2015
 *
 */

#include "index_desc.h"
#include "btree.h"

/******************************************************************
 *
 *  class index_desc_t methods
 *
 ******************************************************************/


index_desc_t::index_desc_t(table_desc_t* table,
                           string name,
                           const int fieldcnt,
                           const unsigned* fields,
                           bool unique, bool primary,
                           const uint32_t& pd,
                           bool rmapholder)
    : _table(table), _name(name), _field_count(fieldcnt),
      _unique(unique), _primary(primary),
      _rmapholder(rmapholder),
      _maxkeysize(0)
{
    // Copy the indexes of keys
    _key = new unsigned[_field_count];
    for (unsigned i=0; i< _field_count; i++) _key[i] = fields[i];

    memset(_keydesc, 0, MAX_KEYDESC_LEN);

    // Check if NoLock
    _nolock = (pd & PD_NOLOCK);

    // Check if Latch-less
    _latchless = (pd & PD_NOLATCH);
}


index_desc_t::~index_desc_t()
{
    if (_key) {
        delete [] _key;
        _key = NULL;
    }
}

// find the index_desc_t by name
bool index_desc_t::matches_name(const char* name)
{
    return (strcmp(name, _name.c_str()) == 0);
}

int index_desc_t::key_index(const unsigned index) const
{
    assert (index < _field_count);
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
    os << "Schema for index " << _name << endl;
    os << "Numer of fields: " << _field_count << endl;
    for (unsigned i=0; i< _field_count; i++) {
	os << _keydesc[i] << "|";
    }
    os << endl;
}

w_rc_t index_desc_t::load_stid(StoreID cat_stid)
{
    smsize_t size = sizeof(StoreID);
    w_keystr_t kstr;
    kstr.construct_regularkey(_name.c_str(), _name.length());
    StoreID stid;
    bool found;
    W_DO(btree_m::lookup(cat_stid, kstr, &stid, size, found));

    if (!found) {
        return RC(eNOTFOUND);
    }
    set_stid(stid);

    return RCOK;
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


