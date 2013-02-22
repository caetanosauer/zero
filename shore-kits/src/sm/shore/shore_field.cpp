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

/** @file:   shore_field.cpp
 *
 *  @brief:  Implementation of the table field description and value
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#include "sm/shore/shore_field.h"

using namespace shore;



/*********************************************************************
 *
 *  field_desc_t methods
 *
 *********************************************************************/

void  field_desc_t::print_desc(ostream & os)
{
    os << "Field " << _name << "\t";
    switch (_type) {
    case SQL_BIT:
	os << "Type: BIT \t size: " << sizeof(bool) << endl;
	break;
    case SQL_SMALLINT:
	os << "Type: SMALLINT \t size: " << sizeof(short) << endl;
	break;
    case SQL_CHAR:
	os << "Type: CHAR \t size: " << sizeof(char) << endl;
	break;
    case SQL_INT:
	os << "Type: INT \t size: " << sizeof(int) << endl;
	break;
    case SQL_FLOAT:
	os << "Type: FLOAT \t size: " << sizeof(double) << endl;
	break;
    case SQL_LONG:
	os << "Type: LONG \t size: " << sizeof(long long) << endl;
	break;
    case SQL_TIME:
	os << "Type: TIMESTAMP \t size: " << timestamp_t::size() << endl;
	break;
    case  SQL_VARCHAR:
	os << "Type: VARCHAR \t size: " << _size << endl;
	break;
    case SQL_FIXCHAR:
	os << "Type: CHAR \t size: " << _size << endl;
	break;
    case SQL_NUMERIC:
	os << "Type: NUMERIC \t size: " << _size << endl;
	break;
    case SQL_SNUMERIC:
	os << "Type: SNUMERIC \t size: " << _size << endl;
	break;
    }
} 



/*********************************************************************
 *
 *  field_value_t methods
 *
 *********************************************************************/


/*********************************************************************
 *
 *  @fn:    load_value_from_file
 *
 *  @brief: Return a string with the value of the specific type and value. 
 *          Used for debugging purposes.
 *  
 *  @note:  Deprecated
 *
 *********************************************************************/

/** DEPRECATED: slow */
bool field_value_t::load_value_from_file(ifstream & is,
                                         const char delim)
{
    assert (_pfield_desc);

    char* string = new char [10*_pfield_desc->fieldmaxsize()];
    is.get(string, 10*_pfield_desc->fieldmaxsize(), delim);
    if (strlen(string) == 0) {
        delete [] string;
        return false;
    }

    if (strcmp(string, "(null)") == 0) {
        assert(_pfield_desc->allow_null());
        _null_flag = true;
        delete [] string;
        return true;
    }

    _null_flag = false;

    switch (_pfield_desc->type()) {
    case SQL_BIT:       _value._bit = atoi(string); break;
    case SQL_SMALLINT:  _value._smallint = atoi(string); break;
    case SQL_CHAR:      _value._char = atoi(string); break;
    case SQL_INT:       _value._int = atoi(string); break;
    case SQL_FLOAT:     _value._float = atof(string); break;
    case SQL_LONG:     _value._float = atol(string); break;
    case SQL_TIME:      break;
    case SQL_VARCHAR:   {
        if (string[0] == '\"') string[strlen(string)-1] = '\0';
        set_var_string_value(string+1, strlen(string)-1);
        break;
    }
    case SQL_FIXCHAR:  {
        if (string[0] == '\"') string[strlen(string)-1] = '\0';
        set_fixed_string_value(string+1, strlen(string)-1);
        break;
    } 
    case SQL_NUMERIC:
    case SQL_SNUMERIC:
        set_fixed_string_value(string, strlen(string));
        break;
    }
    delete [] string;
    return true;
}


/*********************************************************************
 *
 *  @fn:    print_value
 *
 *  @brief: Output the value to the passed output stream
 *
 *********************************************************************/

void  field_value_t::print_value(ostream & os)
{
    assert (_pfield_desc);

    if (_null_flag) {
	os << "(null)";
	return;
    }

    switch (_pfield_desc->type()) {
    case SQL_BIT:
	os <<_value._bit;
	break;
    case SQL_SMALLINT:
	os <<_value._smallint;
	break;
    case SQL_CHAR:
	os <<_value._char;
	break;
    case SQL_INT:
	os << _value._int;
	break;
    case SQL_FLOAT:
	os << fixed;
	os.precision(2);
	os << _value._float;
	break;
    case SQL_LONG:
	os << _value._long;
	break;
    case SQL_TIME:
        char mstr[32];
        _value._time->string(mstr,32);
	os << mstr;
	break;
    case SQL_VARCHAR:
    case SQL_FIXCHAR:
	//os << "\"";
	for (uint i=0; i<_real_size; i++) {
	    if (_value._string[i]) os << _value._string[i];
	}
	//os << "\"";
	break;
    case SQL_NUMERIC:
    case SQL_SNUMERIC: {
	for (uint i=0; i<_real_size; i++) {
	    if (_value._string[i]) os << _value._string[i];
	}
	break;
    }
    }
}



/*********************************************************************
 *
 *  @fn:    get_debug_str
 *
 *  @brief: Return a string with the value of the specific type and value. 
 *          Used for debugging purposes.
 *
 *********************************************************************/

int field_value_t::get_debug_str(char* &buf)
{
    assert (_pfield_desc);

    uint_t sz = _max_size;
    buf = new char[MAX_LINE_LENGTH];
    memset(buf, '\0', MAX_LINE_LENGTH);

    if (_null_flag) {
	sprintf(buf, "(null)");
        return (0);
    }

    switch (_pfield_desc->type()) {
    case SQL_BIT:
        sprintf(buf, "SQL_BIT: \t%d", _value._bit);
	break;
    case SQL_SMALLINT:
        sprintf(buf, "SQL_SMALLINT: \t%d", _value._smallint);
	break;
    case SQL_CHAR:
        sprintf(buf, "SQL_CHAR: \t%d", _value._char);
	break;
    case SQL_INT:
        sprintf(buf, "SQL_INT:      \t%d", _value._int);
	break;
    case SQL_FLOAT:
        sprintf(buf, "SQL_FLOAT:    \t%.2f", _value._float);
	break;
    case SQL_LONG:
        sprintf(buf, "SQL_LONG:    \t%lld", _value._long);
	break;
    case SQL_TIME:
        char mstr[32];
        _value._time->string(mstr,32);
        sprintf(buf, "SQL_TIME:     \t%s", mstr);
	break;
    case SQL_VARCHAR:
        strcat(buf, "SQL_VARCHAR:  \t");
        strncat(buf, _value._string, _real_size);
        break;
    case SQL_FIXCHAR:
        strcat(buf, "SQL_CHAR:     \t");
        strncat(buf, _value._string, _real_size);
	break;
    case SQL_NUMERIC:
        strcat(buf, "SQL_NUMERIC:  \t");
        strncat(buf, _value._string, _real_size);
        break;
    case SQL_SNUMERIC: {
        strcat(buf, "SQL_sNUMERIC: \t");
        strncat(buf, _value._string, _real_size);
	break;
    }
    }

    return (sz);
}
