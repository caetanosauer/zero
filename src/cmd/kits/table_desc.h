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

/** @file:   shore_table.h
 *
 *  @brief:  Base class for tables stored in Shore
 *
 *  @note:   table_desc_t - table abstraction
 *
 *  @author: Ippokratis Pandis, January 2008
 *  @author: Caetano Sauer, April 2015
 *
 */


/* shore_table.h contains the base class (table_desc_t) for tables stored in
 * Shore. Each table consists of several parts:
 *
 * 1. An array of field_desc, which contains the decription of the
 *    fields.  The number of fields is set by the constructor. The schema
 *    of the table is not written to the disk.
 *
 * 2. The primary index of the table.
 *
 * 3. Secondary indices on the table.  All the secondary indices created
 *    on the table are stored as a linked list.
 *
 *
 * FUNCTIONALITY
 *
 * There are methods in (table_desc_t) for creating, the table
 * and indexes.
 *
 *
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * @note  Modifications to the schema need rebuilding the whole
 *        database.
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 *
 *
 * USAGE:
 *
 * To create a new table, create a class for the table by inheriting
 * publicly from class tuple_desc_t to take advantage of all the
 * built-in tools. The schema of the table should be set at the
 * constructor of the table.  (See shore_tpcc_schema.h for examples.)
 *
 *
 * NOTE:
 *
 * Due to limitation of Shore implementation, only the last field
 * in indexes can be variable length.
 *
 *
 * BUGS:
 *
 * If a new index is created on an existing table, explicit call to
 * load the index is needed.
 *
 * Timestamp field is not fully implemented: no set function.
 *
 *
 * EXTENSIONS:
 *
 * The mapping between SQL types and C++ types are defined in
 * (field_desc_t).  Modify the class to support more SQL types or
 * change the mapping.  The NUMERIC type is currently stored as string;
 * no further understanding is provided yet.
 *
 */

#ifndef __TABLE_DESC_H
#define __TABLE_DESC_H


#include "sm_vas.h"
#include "mcs_lock.h"

//#include "shore_msg.h"
#include "util/guard.h"

#include "file_desc.h"
#include "field.h"
#include "index_desc.h"
#include "row.h"

// Shore -> Zero compatibility
typedef okvl_mode::element_lock_mode lock_mode_t;

#define DECLARE_TABLE_SCHEMA(tablename)         \
    class tablename : public table_desc_t {     \
    public: tablename(string sysname); }


#define DECLARE_TABLE_SCHEMA_PD(tablename)              \
    class tablename : public table_desc_t {             \
    public: tablename(const uint32_t& pd); }

/* ---------------------------------------------------------------
 *
 * @class: table_desc_t
 *
 * @brief: Description of a Shore table. Gives access to the fields,
 *         and indexes of the table.
 *
 * --------------------------------------------------------------- */

class table_desc_t
{
protected:

    pthread_mutex_t   _fschema_mutex;        // file schema mutex
    string              _name;  // file name
    unsigned            _field_count;          // # of fields
    uint32_t           _pd;                   // info about the physical design

    /* ------------------- */
    /* --- table schema -- */
    /* ------------------- */

    field_desc_t*   _desc;               // schema - set of field descriptors

    // primary index for index-organized table (replaces Heap of Shore-MT)
    index_desc_t*   _primary_idx;

    // secondary indexes
    std::vector<index_desc_t*>   _indexes;

    unsigned _maxsize;            // max tuple size for this table, shortcut

public:

    /* ------------------- */
    /* --- Constructor --- */
    /* ------------------- */

    table_desc_t(const char* name, int fieldcnt, uint32_t pd);
    virtual ~table_desc_t();


    /* ----------------------------------------- */
    /* --- create physical table and indexes --- */
    /* ----------------------------------------- */

    w_rc_t create_physical_table();

    w_rc_t create_physical_index(index_desc_t* index);

    StoreID get_catalog_stid()
    {
        // using fixed stid=1 for catalog (enforced when creating)
        return 1;
    }

    w_rc_t load_stids();


    /* ----------------------------------------------------- */
    /* --- create the logical description of the indexes --- */
    /* ----------------------------------------------------- */

    // create an index on the table
    bool   create_index_desc(const char* name,
                             const unsigned* fields,
                             const unsigned num,
                             const bool unique=true,
                             const bool primary=false,
                             const uint32_t& pd=PD_NORMAL);

    bool   create_primary_idx_desc(const unsigned* fields,
                                   const unsigned num,
                                   const uint32_t& pd=PD_NORMAL);



    /* ------------------------ */
    /* --- index facilities --- */
    /* ------------------------ */

    // index by name
    index_desc_t* find_index(const char* index_name)
    {
        if (_primary_idx->matches_name(index_name)) {
            return _primary_idx;
        }
        for (size_t i = 0; i < _indexes.size(); i++) {
            if (_indexes[i]->matches_name(index_name)) {
                return _indexes[i];
            }
        }
        return NULL;
    }

    std::vector<index_desc_t*>& get_indexes()
    {
        return _indexes;
    }

    // # of indexes
    int index_count() { return _indexes.size(); }

    index_desc_t* primary_idx() { return (_primary_idx); }
    StoreID get_primary_stid();

    /* sets primary index, the index itself should be already set to
     * primary and unique */
    void set_primary(index_desc_t* idx) {
        assert (idx->is_primary() && idx->is_unique());
        _primary_idx = idx;
    }

    char* index_keydesc(index_desc_t* idx);
    int   index_maxkeysize(index_desc_t* index) const; /* max index key size */

    /* ---------------------------------------------------------------- */
    /* --- for the conversion between disk format and memory format --- */
    /* ---------------------------------------------------------------- */

    unsigned maxsize(); /* maximum requirement for disk format */

    inline field_desc_t* desc(const unsigned descidx) {
        assert (descidx<_field_count);
        assert (_desc);
        return (&(_desc[descidx]));
    }

    const char*   name() const { return _name.c_str(); }
    unsigned        field_count() const { return _field_count; }
    uint32_t       get_pd() const { return _pd; }

    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    void print_desc(ostream & os = cout);  /* print the schema */

protected:
    int find_field_by_name(const char* field_name) const;

    srwlock_t _mutex;

}; // EOF: table_desc_t


typedef std::list<table_desc_t*> table_list_t;

/******************************************************************
 *
 *  class table_desc_t methods
 *
 ******************************************************************/



/******************************************************************
 *
 * @fn:    find_field_by_name
 *
 * @brief: Returns the field index, given its name. If no such field
 *         name exists it returns -1.
 *
 ******************************************************************/

inline int table_desc_t::find_field_by_name(const char* field_name) const
{
    for (unsigned i=0; i<_field_count; i++) {
        if (strcmp(field_name, _desc[i].name())==0)
            return (i);
    }
    return (-1);
}



/******************************************************************
 *
 * @fn:    index_keydesc
 *
 * @brief: Iterates over all the fields of a selected index and returns
 *         on a single string the corresponding key description
 *
 ******************************************************************/

inline char* table_desc_t::index_keydesc(index_desc_t* idx)
{
    CRITICAL_SECTION(idx_kd_cs, idx->_keydesc_lock);
    if (strlen(idx->_keydesc)>1) // is key_desc is already set
        return (idx->_keydesc);

    // else set the index keydesc
    for (unsigned i=0; i<idx->field_count(); i++) {
        strcat(idx->_keydesc, _desc[idx->key_index(i)].keydesc());
    }
    return (idx->_keydesc);
}



/******************************************************************
 *
 *  @fn:    index_maxkeysize
 *
 *  @brief: For an index it returns the maximum size of the index key
 *
 *  @note:  !!! Now that key_size() Uses the maxsize() of each field,
 *              key_size() == maxkeysize()
 *
 ******************************************************************/

inline int table_desc_t::index_maxkeysize(index_desc_t* idx) const
{
    unsigned size = 0;
    if ((size = idx->get_keysize()) > 0) {
        // keysize has already been calculated
        // just return that value
        return (size);
    }

    // needs to calculate the (max)key for that index
    unsigned ix = 0;
    for (unsigned i=0; i<idx->field_count(); i++) {
        ix = idx->key_index(i);
        size += _desc[ix].fieldmaxsize();
    }
    // set it for the index, for future invokations
    idx->set_keysize(size);
    return(size);
}



/******************************************************************
 *
 *  @fn:    maxsize()
 *
 *  @brief: Return the maximum size requirement for a tuple in disk format.
 *          Normally, it will be calculated only once.
 *
 ******************************************************************/

inline unsigned table_desc_t::maxsize()
{
    // shortcut not to re-compute maxsize
    if (*&_maxsize)
        return (*&_maxsize);

    // calculate maximum size required
    unsigned size = 0;
    unsigned var_count = 0;
    unsigned null_count = 0;
    for (unsigned i=0; i<_field_count; i++) {
        size += _desc[i].fieldmaxsize();
        if (_desc[i].allow_null()) null_count++;
        if (_desc[i].is_variable_length()) var_count++;
    }

    size += (var_count*sizeof(offset_t)) + (null_count>>3) + 1;

    // There is a small window from the time it checks if maxsize is already set,
    // until the time it tries to set it up. In the meantime, another thread may
    // has done the calculation already. If that had happened, the two threads
    // should have calculated the same maxsize, since it is the same table desc.
    // In other words, the maxsize should be either 0 or equal to the size.
    assert ((*&_maxsize==0) || (*&_maxsize==size));

    // atomic_swap_uint(&_maxsize, size);
    _maxsize = size;
    // add an offset for each field, which is used to serialize
    _maxsize += sizeof(offset_t) * _field_count;
    return (*&_maxsize);
}

#endif /* __TABLE_DESC_H */
