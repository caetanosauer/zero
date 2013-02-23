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

/** @file:   shore_row.cpp
 *
 *  @brief:  Implementation of the base class for records (rows) of tables in Shore
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#include "sm/shore/shore_row.h"
#include "sm/shore/shore_table.h"


ENTER_NAMESPACE(shore);


/******************************************************************
 * 
 * @struct: rep_row_t
 *
 * @brief:  A scratchpad for writing the disk format of a tuple
 *
 ******************************************************************/

rep_row_t::rep_row_t()
    : _dest(NULL), _bufsz(0), _pts(NULL)
{ }

rep_row_t::rep_row_t(ats_char_t* apts) 
    : _dest(NULL), _bufsz(0), _pts(apts)
{ 
    assert (_pts);
}

rep_row_t::~rep_row_t() 
{
    if (_dest) {
        _pts->destroy(_dest);
        _dest = NULL;
    }
}


/******************************************************************
 * 
 * @fn:    set
 *
 * @brief: Set new buffer size
 *
 ******************************************************************/

void rep_row_t::set(const uint nsz)
{
    if ((!_dest) || (_bufsz < nsz)) {

        char* tmp = _dest;

        // Using the trash stack
        assert (_pts);

        //_dest = new(*_pts) char(nsz);
        w_assert1(nsz <= _pts->nbytes());
        _dest = (char*)_pts->acquire();
        assert (_dest); // Failed to allocate such a big buffer

        if (tmp) {
            //            delete [] tmp;
            _pts->destroy(tmp);
            tmp = NULL;
        } 
        _bufsz = _pts->nbytes();
    }

    // in any case, clean up the buffer
    memset (_dest, 0, nsz);
}


/******************************************************************
 * 
 * @fn:    set_ts
 *
 * @brief: Set new trash stack and buffer size
 *
 ******************************************************************/

void rep_row_t::set_ts(ats_char_t* apts, const uint nsz)
{
    assert(apts);
    _pts = apts;
    set(nsz);
}




/******************************************************************
 * 
 * @class: table_row_t 
 *
 * @brief: The (main-memory) record representation in kits  
 *
 ******************************************************************/


table_row_t::table_row_t() 
    : _ptable(NULL),
      _field_cnt(0), _is_setup(false), 
      _rid(rid_t::null), _pvalues(NULL), 
      _fixed_offset(0),_var_slot_offset(0),_var_offset(0),_null_count(0),
      _rep(NULL), _rep_key(NULL)
{ 
}
        
table_row_t::~table_row_t() 
{
    freevalues();
}



/****************************************************************** 
 *
 *  @fn:    setup()
 *
 *  @brief: Setups the row (tuple main-memory representation) according 
 *          to its table description. This setup will be done only 
 *          *once*. When this row will be initialized in the row cache.
 *
 ******************************************************************/

int table_row_t::setup(table_desc_t* ptd) 
{
    assert (ptd);

    // if it is already setup for this table just reset it
    if ((_ptable == ptd) && (_pvalues != NULL) && (_is_setup)) {
        reset();
        return (1);
    }

    // else do the normal setup
    _ptable = ptd;
    _field_cnt = ptd->field_count();
    assert (_field_cnt>0);
    _pvalues = new field_value_t[_field_cnt];

    uint var_count  = 0;
    uint fixed_size = 0;

    // setup each field and calculate offsets along the way
    for (uint i=0; i<_field_cnt; i++) {
        _pvalues[i].setup(ptd->desc(i));

        // count variable- and fixed-sized fields
        if (_pvalues[i].is_variable_length())
            var_count++;
        else
            fixed_size += _pvalues[i].maxsize();

        // count null-able fields
        if (_pvalues[i].field_desc()->allow_null())
            _null_count++;            
    }

    // offset for fixed length field values
    _fixed_offset = 0;
    if (_null_count) _fixed_offset = ((_null_count-1) >> 3) + 1;
    // offset for variable length field slots
    _var_slot_offset = _fixed_offset + fixed_size; 
    // offset for variable length field values
    _var_offset = _var_slot_offset + sizeof(offset_t)*var_count;

    _is_setup = true;
    return (0);
}


/****************************************************************** 
 *
 *  @fn:    size()
 *
 *  @brief: Return the actual size of the tuple in disk format
 *
 ******************************************************************/

uint table_row_t::size() const
{
    assert (_is_setup);

    uint size = 0;

    /* for a fixed length field, it just takes as much as the
     * space for the value itself to store.
     * for a variable length field, we store as much as the data
     * and the offset to tell the length of the data.
     * Of course, there is a bit for each nullable field.
     */

    for (uint i=0; i<_field_cnt; i++) {
	if (_pvalues[i]._pfield_desc->allow_null()) {
	    if (_pvalues[i].is_null()) continue;
	}
	if (_pvalues[i].is_variable_length()) {
	    size += _pvalues[i].realsize();
	    size += sizeof(offset_t);
	}
	else size += _pvalues[i].maxsize();
    }
    if (_null_count) size += (_null_count >> 3) + 1;
    return (size);
}






/* ----------------- */
/* --- debugging --- */
/* ----------------- */

/* For debug use only: print the value of all the fields of the tuple */
void table_row_t::print_values(ostream& os)
{
    assert (_is_setup);
    //  cout << "Number of fields: " << _field_count << endl;
    for (uint i=0; i<_field_cnt; i++) {
	_pvalues[i].print_value(os);
	if (i != _field_cnt-1) os << DELIM_CHAR;
    }
    os << ROWEND_CHAR << endl;
}



/* For debug use only: print the tuple */
void table_row_t::print_tuple()
{
    assert (_is_setup);
    
    char* sbuf = NULL;
    int sz = 0;
    for (uint i=0; i<_field_cnt; i++) {
        sz = _pvalues[i].get_debug_str(sbuf);
        if (sbuf) {
            TRACE( TRACE_TRX_FLOW, "%d. %s (%d)\n", i, sbuf, sz);
            delete [] sbuf;
            sbuf = NULL;
        }
    }
}


/* For debug use only: print the tuple without tracing */
void table_row_t::print_tuple_no_tracing()
{
    assert (_is_setup);
    
    char* sbuf = NULL;
    int sz = 0;
    for (uint i=0; i<_field_cnt; i++) {
        sz = _pvalues[i].get_debug_str(sbuf);
        if (sbuf) {
            fprintf( stderr, "%d. %s (%d)\n", i, sbuf, sz);
            delete [] sbuf;
            sbuf = NULL;
        }
    }
}


EXIT_NAMESPACE(shore);



#include <sstream>
char const* db_pretty_print(shore::table_row_t const* rec, int /* i=0 */, char const* /* s=0 */)
{
    static char data[1024];
    std::stringstream inout(data,stringstream::in | stringstream::out);
    //std::strstream inout(data, sizeof(data));
    ((shore::table_row_t*)rec)->print_values(inout);
    inout << std::ends;
    return data;
}
