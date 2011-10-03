/*<std-header orig-src='shore'>

 $Id: vtable.cpp,v 1.2 2010/05/26 01:20:23 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <w.h>
#include <vtable.h>

#include <w_strstream.h>


void 
vtable_row_t::set_uint(int a, unsigned int v) {
    // check for reasonable attribute #
    w_assert1(a < N);
    // check attribute not already set
    w_assert3(strlen(_get_const(a)) == 0); 
    // Create a buffered ostrstream that will write to this
    // entry for 'a'
    w_ostrstream o(_insert_attribute(a), value_size());
    // Create the entry for a
    o << v << ends;
    _inserted(a);
}

void 
vtable_row_t::set_base(int a, w_base_t::base_float_t v) {
    // check for reasonable attribute #
    w_assert1(a < N);
    // check attribute not already set
    w_assert9(strlen(_get_const(a)) == 0); 
    w_ostrstream o(_insert_attribute(a), value_size());
    o << v << ends;
    _inserted(a);
}

void 
vtable_row_t::set_base(int a,  w_base_t::base_stat_t v) {
    // check for reasonable attribute #
    w_assert1(a < N);
    // check attribute not already set
    w_assert9(strlen(_get_const(a)) == 0); 
    w_ostrstream o(_insert_attribute(a), value_size());
    o << v << ends;
    _inserted(a);
}

void 
vtable_row_t::set_int(int a, int v) {
    w_assert1(a < N);
    w_assert9(strlen(_get_const(a)) == 0); 
    w_ostrstream o(_insert_attribute(a), value_size());
    o << v << ends;
    _inserted(a);
}

void 
vtable_row_t::set_string(int a, const char *v) {
    // check for reasonable attribute #
    w_assert1(a < N);
    w_assert1((int)strlen(v) < value_size());
    // check attribute not already set
    w_assert9(strlen(_get_const(a)) == 0); 
    strcpy(_insert_attribute(a), v);
    w_assert9(_get_const(a)[strlen(v)] == '\0'); 
    _inserted(a);
}

void
vtable_row_t::dump(const char *msg) const
{
    fprintf(stderr, "vtable_row_t (%s) at %p {\n", msg, this );
    fprintf(stderr, "N %d M %d _in_use %d\n", N, M, _in_use);
    fprintf(stderr, "_list %p -> _list_end %p\n", _list, _list_end);

    for(int i=0; i<=_in_use; i++) {
        const char *not_yet_set = "Not yet set";
        const char *p = _entry[i];
        const char *A = "";
        if(p < _list) {
            A = "BOGUS: addr too low";
        }
        const char *B = "";
        if(p > _list_end) {
            B = "BOGUS: addr too high";
        }
        fprintf(stderr, "%s %s _entry[%d] = %p, string= %s \n" ,
                A, B, i, p, (i==_in_use?not_yet_set:p));
    }
    fprintf(stderr, "}\n");
}

void 
vtable_row_t::_inserted(int a) {
    w_assert0(a < N);
    w_assert0(a == _in_use); // assume we insert in order always.
    int l = strlen(_entry[a]);
    w_assert0(*(_entry[a] + l +1) == '\0');
    if(a == N-1) {
        // it's full
        if(l==0) l++; // this is a null string. Must add at least one.
        _list_end = _entry[a] + l;
    } else {
        _entry[a+1] = _entry[a] + l + 1;
        _list_end = _entry[a+1];
    }


    _in_use++;
    w_assert0(_in_use <= N); // still. Origin 0
}

ostream& 
vtable_row_t::operator<<(ostream &o) 
{

    for(int i=0; i<_in_use; i++) {
        if(strlen(_get_const(i)) > 0) {
            o <<  i << ": " << _get_const(i) <<endl;
        }
    }
    o <<  endl;
    return o;
}


int 
vtable_t::init(int R, int A, int S) {

    _rows_filled = 0;
    _rows = R;
    _rowsize_attributes = A;

    // how many bytes are needed for a single row of N attributes with
    // maximum attribute size of S?
    // Align it
    _rowsize_bytes = align(vtable_row_t::bytes_required_for(A, S));

    _array_alias = new char [_rows * _rowsize_bytes];

    if(_array_alias) {
        // initialize the array
        memset(_array_alias, '\0', _rows * _rowsize_bytes);

        for(int i=0; i<_rows; i++) {
            vtable_row_t *row = _get_row(i);
            row->init_for_attributes(_rowsize_attributes, S);
            w_assert1(row->size_in_bytes() <= _rowsize_bytes);
        }
        return 0;
    } else {
        return -1;
    }
}

void           
vtable_t::filled_one() 
{ 
    W_IFDEBUG1(vtable_row_t *t = _get_row(_rows_filled);)
    w_assert1(t->size_in_bytes() <= _rowsize_bytes);

    _rows_filled++; 
    w_assert9(_rows_filled <= _rows);
}

ostream& 
vtable_t::operator<<(ostream &o) const {

    for(int i=0; i<_rows; i++) {
        _get_row(i)->operator<<(o) ;
    }
    o <<  endl;
    return o;
}

vtable_row_t* 
vtable_t::_get_row(int i) const {
    w_assert9(i >= 0);
    w_assert9(i <= _rows);
    w_assert9(_rows_filled <= _rows);
    vtable_row_t* v =
        (vtable_row_t *)&_array_alias[i * _rowsize_bytes];
    return v;
}

int            
vtable_t::realloc() // doubles the size
{
    W_FATAL_MSG(fcINTERNAL, <<"vtable_t::realloc is not implemented");
    return 0;
}
