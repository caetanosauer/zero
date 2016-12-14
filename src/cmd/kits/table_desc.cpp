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

/** @file shore_table.cpp
 *
 *  @brief Implementation of shore_table class
 *
 *  @author: Ippokratis Pandis, January 2008
 *  @author: Caetano Sauer, April 2015
 *
 */

#include "table_desc.h"

#include "w_key.h"
#include "btree.h"

table_desc_t::table_desc_t(const char* name, int fieldcnt, uint32_t pd)
    : _name(name), _field_count(fieldcnt), _pd(pd), _db(NULL), _primary_idx(NULL),
    _maxsize(0)
{
    assert (fieldcnt>0);

    pthread_mutex_init(&_fschema_mutex, NULL);

    // Create placeholders for the field descriptors
    _desc = new field_desc_t[fieldcnt];
}

table_desc_t::~table_desc_t()
{
    if (_desc) {
        delete [] _desc;
        _desc = NULL;
    }

    pthread_mutex_destroy(&_fschema_mutex);

    if (_primary_idx) {
        delete _primary_idx;
    }

    for (size_t i = 0; i < _indexes.size(); i++) {
        delete _indexes[i];
    }
    _indexes.clear();
}


/* ----------------------------------------- */
/* --- create physical table and indexes --- */
/* ----------------------------------------- */


/*********************************************************************
 *
 *  @fn:    create_physical_table
 *
 *  @brief: Creates the physical table and calls for the (physical) creation of
 *          all the corresponding indexes
 *
 *********************************************************************/

w_rc_t table_desc_t::create_physical_table(ss_m* db)
{
    assert (db);
    _db = db;

    W_DO(create_physical_index(db, _primary_idx));
    w_assert0(_primary_idx);

    for (size_t i = 0; i < _indexes.size(); i++) {
        W_DO(create_physical_index(db, _indexes[i]));
    }

    return (RCOK);
}



/*********************************************************************
 *
 *  @fn:    create_physical_index
 *
 *  @brief: Creates the physical index
 *
 *********************************************************************/

w_rc_t table_desc_t::create_physical_index(ss_m* db, index_desc_t* index)
{
    // Create all the indexes of the table
    StoreID stid = 0;

    W_DO(btree_m::create(stid));
    w_assert0(index);
    index->set_stid(stid);

    // Add entry on catalog
    w_keystr_t kstr;
    kstr.construct_regularkey(index->name().c_str(), index->name().length());
    W_DO(db->create_assoc(get_catalog_stid(), kstr,
                vec_t(&stid, sizeof(StoreID))));

    // Print info
    TRACE( TRACE_STATISTICS, "%s %d (%s) (%s) (%s)\n",
           index->name().c_str(), stid,
           (index->is_latchless() ? "no latch" : "latch"),
           (index->is_relaxed() ? "relaxed" : "no relaxed"),
           (index->is_unique() ? "unique" : "no unique"));

    return (RCOK);
}

/******************************************************************
 *
 *  @fn:    create_index_desc
 *
 *  @brief: Create the description of a regular or primary index on the table
 *
 *  @note:  This only creates the index decription for the index in memory.
 *
 ******************************************************************/

// Cannot update fields included at indexes - delete and insert again

// Only the last field of an index can be of variable length

bool table_desc_t::create_index_desc(const char* name,
                                     const unsigned* fields,
                                     const unsigned num,
                                     const bool unique,
                                     const bool primary,
                                     const uint32_t& pd)
{
    index_desc_t* p_index = new index_desc_t(this, name, num, fields,
                                             unique, primary, pd);

    // check the validity of the index
    for (unsigned i=0; i<num; i++)  {
        assert(fields[i] < _field_count);

        // only the last field in the index can be variable lengthed
        // IP: I am not sure if still only the last field in the index can be variable lengthed

        if (_desc[fields[i]].is_variable_length() && i != num-1) {
            assert(false);
        }
    }

    // add as primary
    if (p_index->is_unique() && p_index->is_primary()) {
        _primary_idx = p_index;
    }
    else {
        _indexes.push_back(p_index);
    }

    return true;
}


bool table_desc_t::create_primary_idx_desc(const unsigned* fields,
                                           const unsigned num,
                                           const uint32_t& pd)
{

    index_desc_t* p_index = new index_desc_t(this,
            _name, num, fields, true, true, pd);

    // check the validity of the index
    for (unsigned i=0; i<num; i++) {
        assert(fields[i] < _field_count);

        // only the last field in the index can be variable lengthed
        if (_desc[fields[i]].is_variable_length() && i != num-1) {
            assert(false);
        }
    }

    // make it the primary index
    _primary_idx = p_index;

    return (true);
}


// Returns the stid of the primary index. If no primary index exists it
// returns the stid of the table
StoreID table_desc_t::get_primary_stid()
{
    w_assert0(_primary_idx);
    return _primary_idx->stid();
}

w_rc_t table_desc_t::load_stids()
{
    w_assert0(_db);
    StoreID cat_stid = get_catalog_stid();
    W_DO(_primary_idx->load_stid(_db, cat_stid));
    for (size_t i = 0; i < _indexes.size(); i++) {
        W_DO(_indexes[i]->load_stid(_db, cat_stid));
    }
    return RCOK;
}

/* ----------------- */
/* --- debugging --- */
/* ----------------- */


// For debug use only: print the description for all the field
void table_desc_t::print_desc(ostream& os)
{
    os << "Schema for table " << _name << endl;
    os << "Numer of fields: " << _field_count << endl;
    for (unsigned i=0; i<_field_count; i++) {
	_desc[i].print_desc(os);
    }
}
