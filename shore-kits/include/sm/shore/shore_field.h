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

/** @file:   shore_field.h
 *
 *  @brief:  Description and current value of a field (column)
 *
 *  @note:   field_desc_t  - the description of a field
 *           field_value_t - the value of a field
 *
 *  The description of the field includes type, size, and whether it
 *  allows null values. The value of the field is stored in a union.
 *  If the type is SQL_TIME or strings, the data is stored in _data and
 *  the union contains the pointer to it.  The space of _data is
 *  allocated at setup time for fixed length fields and at set_value
 *  time for variable length fields.
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __SHORE_FIELD_H
#define __SHORE_FIELD_H

#include "util.h"

#include "sm/shore/shore_file_desc.h"


ENTER_NAMESPACE(shore);


/*--------------------------------------------------------------------
 *
 * @class: timestamp_t
 *
 * @brief: Helper for the SQL_TIME type. Value for timestamp field.
 *
 * @note:  Deprecated  
 *
 *--------------------------------------------------------------------*/

class timestamp_t 
{
private:
    time_t _time;

public:

    timestamp_t() { _time = time(NULL); }
    ~timestamp_t() { }

    static int  size() { return sizeof(time_t); }
    void string(char* dest, const int len) const {
#ifdef _POSIX_PTHREAD_SEMANTICS
	ctime_r(&_time, dest);
#elif defined(SOLARIS2)
	ctime_r(&_time, dest, len);
#else
	ctime_r(&_time, dest);
#endif
	dest[len-1] = '\0';
    }

}; // EOF: timestamp_t



/*---------------------------------------------------------------------
 * 
 * @enum:  sql_type_t
 *
 * @brief: Enumuration of the supported sql types
 *
 *--------------------------------------------------------------------*/

enum  sqltype_t 
{
    SQL_BIT,        /* BIT == BOOL */
    SQL_SMALLINT,   /* SMALLINT */
    SQL_INT,        /* INTEGER */
    SQL_FLOAT,      /* FLOAT */
    SQL_LONG,       /* LONG */
    SQL_CHAR,       /* CHAR */           // A single char, could have been a smallint
    SQL_FIXCHAR,    /* FIXCHAR */        // Fixed size string
    SQL_VARCHAR,    /* VARCHAR */        // Variable size string
    SQL_TIME,       /* TIMESTAMP */      // Deprecated, use SQL_FLOAT instead

    SQL_NUMERIC,    /* NUMERIC */        /* Not tested */
    SQL_SNUMERIC    /* SIGNED NUMERIC */ /* Not tested */

}; // EOF: sqltype_t



/*********************************************************************
 *
 * @class: field_desc_t
 *
 * @brief: Description of a table field (column)
 *
 *********************************************************************/

class field_desc_t {
private:

    tatas_lock _fielddesc_lock;           /* lock for the modifying methods */
    char       _name[MAX_FIELDNAME_LEN];  /* field name */
    char       _keydesc[MAX_KEYDESC_LEN]; /* buffer for key description */

    sqltype_t  _type;                     /* type */
    short      _size;                     /* max_size */
    bool       _allow_null;               /* allow null? */
    bool       _is_setup;                 /* is setup? */

    const char* _set_keydesc();

public:

    /* -------------------- */
    /* --- construction --- */
    /* -------------------- */

    field_desc_t()
	:  _type(SQL_SMALLINT), _size(0), 
          _allow_null(true), _is_setup(false)
    { 
        memset(_name, 0, MAX_FIELDNAME_LEN);
        memset(_keydesc, 0, MAX_KEYDESC_LEN);
    }
    
    ~field_desc_t() 
    { 
    }


    /* ---------------------- */
    /* --- access methods --- */
    /* ---------------------- */

    const char* name() const { 
        //assert (_is_setup); 
        return (_name); 
    }

    inline bool is_variable_length(sqltype_t type) const { 
        //assert (_is_setup);
        return (type == SQL_VARCHAR); 
    }

    inline bool is_variable_length() const { 
        return (is_variable_length(_type)); 
    }

    inline uint_t fieldmaxsize() const { 
        //assert (_is_setup);
        return (_size); 
    }

    inline sqltype_t type() const { 
        //assert (_is_setup);
        return (_type); 
    }

    inline bool allow_null() const { 
        //assert (_is_setup);
        return (_allow_null); 
    }

    /* return key description for index creation */
    const char* keydesc() {
        //assert (_is_setup);
        CRITICAL_SECTION(fkd_cs, _fielddesc_lock);
        return (_set_keydesc());
    }

    /* set the type description for the field. */
    void setup(const sqltype_t type,
               const char* name,
               const short size = 0,
               const bool allow_null = false);

    /* for debugging */
    void print_desc(ostream& os = cout);

}; // EOF: field_desc_t



/*********************************************************************
 *
 * @struct:  field_value_t
 *
 * @brief:   Value of a table field
 * 
 * @warning: !!! NOT-THREAD SAFE !!!
 *
 *********************************************************************/

struct field_value_t 
{
    /** if set it shows that the field_value is setup */
    field_desc_t* _pfield_desc; /* pointer to the description of the field */
    bool          _null_flag;   /* null value? */

    /* value of a field */
    union s_field_value_t {
	bool         _bit;      /* BIT */
	short        _smallint; /* SMALLINT */
	char         _char;     /* CHAR */
	int          _int;      /* INT */
	double       _float;    /* FLOAT */
	long long    _long;     /* LONG */
	timestamp_t* _time;     /* TIME or DATE */
	char*        _string;   /* FIXCHAR, VARCHAR, NUMERIC */
    }   _value;   

    char* _data;      /* buffer for _value._time or _value._string */
    uint_t   _data_size; /* allocated size of the data buffer (watermark) */
    uint_t   _real_size; /* current size of the value */
    uint_t   _max_size;  /* maximum possible size of the buffer (shortcut) */


    /* -------------------- */
    /* --- construction --- */
    /* -------------------- */

    field_value_t() 
        :  _pfield_desc(NULL), _null_flag(true), _data(NULL), 
           _data_size(0), _real_size(0), _max_size(0)
    { 
    }


    field_value_t(field_desc_t* pfd) 
        : _pfield_desc(pfd), _null_flag(true), _data(NULL), 
          _data_size(0), _real_size(0), _max_size(0)
    { 
        setup(pfd); /* It will assert if pfd = NULL */
    }


    ~field_value_t() {
        if (_data) {
            free (_data);
            _data = NULL;
        }
    }


    /* ------------------------- */
    /* --- setup field value --- */
    /* ------------------------- */

    /* setup according to the given field_desc_t */
    void setup(field_desc_t* pfd);

    /* clear value */
    void reset();

    inline bool is_setup() { return ( _pfield_desc ? true : false); }

    /* access field description */
    inline field_desc_t* field_desc() { return (_pfield_desc); }
    inline void set_field_desc(field_desc_t* fd) { 
        assert (fd); 
        _pfield_desc = fd; 
    }

    /* return realsize of value */
    inline uint_t realsize() const { 
        assert (_pfield_desc);
        return (_real_size);
    }

    /* return maxsize of value */
    inline uint_t maxsize() const { 
        assert (_pfield_desc);
        return (_max_size);
    }


    /* ------------------------------- */
    /* --- value related functions --- */
    /* ------------------------------- */

   
    /* allocate the space for _data */
    void alloc_space(const uint_t size);

    /* set min/max allowed value */
    void set_min_value();
    void set_max_value();    

    /* null field */
    inline bool is_null() const { 
        assert (_pfield_desc);
        return (_null_flag); 
    }

    inline void set_null() { 
        assert (_pfield_desc);
        assert (_pfield_desc->allow_null()); 
        _null_flag = true; 
    }

    /* var length */
    inline bool is_variable_length() { 
        assert (_pfield_desc); 
        return (_pfield_desc->is_variable_length()); 
    }

    /* copy current value out */
    bool   copy_value(void* data) const;


    // Set current value
    void   set_value(const void* data, const uint length);
    void   set_int_value(const int data);
    void   set_bit_value(const bool data);
    void   set_smallint_value(const short data);
    void   set_float_value(const double data);
    void   set_long_value(const long long data);
    void   set_decimal_value(const decimal data);
    void   set_time_value(const time_t data);
    void   set_tstamp_value(const timestamp_t& data);
    void   set_char_value(const char data);
    void   set_fixed_string_value(const char* string, const uint len);
    void   set_var_string_value(const char* string, const uint len);


    // Get current value
    int          get_int_value() const;
    short        get_smallint_value() const;
    bool         get_bit_value() const;
    char         get_char_value() const;
    void         get_string_value(char* string, const uint bufsize) const;
    double       get_float_value() const;
    long long    get_long_value() const;
    decimal      get_decimal_value() const;
    time_t       get_time_value() const;
    timestamp_t& get_tstamp_value() const;

    bool load_value_from_file(ifstream& is, const char delim);


    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    void      print_value(ostream& os = cout);
    int get_debug_str(char* &buf);

}; // EOF: field_value_t



/*********************************************************************
 *
 *  class field_desc_t functions
 *
 *********************************************************************/


/*********************************************************************
 *
 *  @fn:    setup
 *
 *  @brief: Sqltype specific setup
 *
 *********************************************************************/

inline void field_desc_t::setup(sqltype_t type,
                                const char* name,
                                short size,
                                bool allow_null)
{
    CRITICAL_SECTION(setup_cs, _fielddesc_lock);

    // name of field
    strncpy(_name, name, MAX_FIELDNAME_LEN);
    _type = type;

    // size of field
    switch (_type) {
    case SQL_BIT:
        _size = sizeof(bool);
        break;
    case SQL_SMALLINT:
        _size = sizeof(short);
        break;
    case SQL_CHAR:
        _size = sizeof(char);
        break;
    case SQL_INT:
        _size = sizeof(int);
        break;
    case SQL_FLOAT:
        _size = sizeof(double);
        break;
    case SQL_LONG:
        _size = sizeof(long long);
        break;
    case SQL_TIME:
        _size = sizeof(timestamp_t);
        break;    
    case SQL_FIXCHAR:
    case SQL_NUMERIC:
    case SQL_SNUMERIC:
    case SQL_VARCHAR:
        _size = size * sizeof(char);
        break;
    }

    // set the key desc
    _set_keydesc();

    // set if this field can contain null values
    _allow_null = allow_null;

    // now let the rest functionality to be accessed
    _is_setup = true;
}



/*********************************************************************
 *
 *  @fn:    _set_keydesc
 *
 *  @brief: Private function. Returns a string with the key description
 *
 *  @note:  Private function, it does not lock the key desc lock. The 
 *          public function only locks.
 *
 *********************************************************************/

inline const char* field_desc_t::_set_keydesc()
{
    if (strlen(_keydesc)>1)
        return (&_keydesc[0]);

    // else construct the _keydesc
    //if (!_keydesc) _keydesc = (char*)malloc( MAX_KEYDESC_LEN );
  
    switch (_type) {
    case SQL_BIT:
    case SQL_SMALLINT:  
    case SQL_CHAR:  
    case SQL_INT:       
        sprintf(_keydesc, "i%d", _size); break;

    case SQL_FLOAT:     
    case SQL_LONG:     
        sprintf(_keydesc, "f%d", _size); break;
    
    case SQL_VARCHAR:   
        sprintf(_keydesc, "b*%d", _size); break;

    case SQL_TIME:
    case SQL_FIXCHAR:
    case SQL_NUMERIC:
    case SQL_SNUMERIC:
        sprintf(_keydesc, "b%d", _size);  break;
    }
    return (&_keydesc[0]);
}



/*********************************************************************
 *
 *  class field_value_t functions
 *
 *********************************************************************/


/*********************************************************************
 *
 *  @fn:    setup
 *
 *  @brief: Field specific setup for the value
 *
 *********************************************************************/

inline void field_value_t::setup(field_desc_t* pfd)
{
    assert (pfd);

    // if it is already setup for this field do nothing
    if (_pfield_desc == pfd)
        return;

    _pfield_desc = pfd;
    uint_t sz = 0;

    switch (_pfield_desc->type()) {
    case SQL_BIT:
        _max_size = sizeof(bool);
        break;
    case SQL_SMALLINT:
        _max_size = sizeof(short);
        break;
    case SQL_CHAR:
        _max_size = sizeof(char);
        break;
    case SQL_INT:
        _max_size = sizeof(int);
        break;
    case SQL_FLOAT:
        _max_size = sizeof(double);
        break;    
    case SQL_LONG:
        _max_size = sizeof(long long);
        break;    
    case SQL_TIME:
        sz = sizeof(timestamp_t);
        _data_size = sz;
        _real_size = sz; 
        _max_size = sz;
        if (_data)
            free (_data);
        _data = (char*)malloc(sz);
        _value._time = (timestamp_t*)_data;
        break;    
    case SQL_VARCHAR:
        _max_size  = _pfield_desc->fieldmaxsize();
        /* real_size is re-set at runtime, at the set_value() function */       
        _real_size = 0;
        /* we don't know how much space is already allocated for data
         * thus, we are not changing its value 
         */
        _value._string = _data;
        break;
    case SQL_FIXCHAR:
    case SQL_NUMERIC:
    case SQL_SNUMERIC:
        sz = _pfield_desc->fieldmaxsize(); 
        _real_size = sz;
        _max_size = sz;

        if ((_data_size>=sz) && (_data)) {
            // if already _data has enough allocated space
            // just memset the area
            memset(_data, 0, _data_size);
        }
        else {
            // else allocate the buffer
            if (_data)
                free (_data);
            _data = (char*)malloc(sz);
            memset(_data, 0, sz);
            _data_size = sz;
        }

        _value._string = _data;
        break;
    }    
}



/*********************************************************************
 *
 *  @fn:    reset
 *
 *  @brief: Field specific reset for the value
 *
 *********************************************************************/

inline void field_value_t::reset()
{
    //assert (_pfield_desc);

    _null_flag = true;
    switch (_pfield_desc->type()) {
    case SQL_BIT:
        _value._bit = false;
        break;
    case SQL_SMALLINT:
        _value._smallint = 0;
        break;
    case SQL_CHAR:
        _value._char = 0;
        break;
    case SQL_INT:
        _value._int = 0;
        break;
    case SQL_FLOAT:
        _value._float = 0;
        break;    
    case SQL_LONG:
        _value._long = 0;
        break;    
    case SQL_TIME:
    case SQL_VARCHAR:
    case SQL_FIXCHAR:
    case SQL_NUMERIC:
    case SQL_SNUMERIC:
        if (_data && _data_size) memset(_data, 0, _data_size);
    }
}



/*********************************************************************
 *
 *  @fn:     alloc_space
 * 
 *  @brief:  Allocates the requested space (param len). If it has already
 *           allocated enough returns immediately.
 *
 *  @note:   It will asserts if the requested space is larger than the
 *           realsize. 
 *
 *********************************************************************/

inline void field_value_t::alloc_space(const uint_t len)
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_VARCHAR);
    assert (len <= _real_size);

    // check if already enough space
    if (_data_size >= len) 
        return;

    // if not, release previously allocated space and allocate new
    if (_data) { 
	free(_data);
    }
    _data = (char*)malloc(len);
    _data_size = len;

    // clear the contents of the allocated buffer
    memset(_data, 0, _data_size);

    // the string value points to the allocated buffer
    _value._string = _data;
}



/*********************************************************************
 *
 *  @fn:    set_value
 *
 *  @brief: Sets the current value to a (void*) buffer
 *
 *********************************************************************/

inline void field_value_t::set_value(const void* data,
                                     const uint length)
{
    assert (_pfield_desc);
    _null_flag = false;

    switch (_pfield_desc->type()) {
    case SQL_BIT:
    case SQL_SMALLINT:
    case SQL_CHAR:
    case SQL_INT:
    case SQL_FLOAT:
    case SQL_LONG:
	memcpy(&_value, data, _max_size); 
        break;
    case SQL_TIME:
	memcpy(_value._time, data, MIN(length, _real_size)); 
        break;
    case SQL_VARCHAR:
	set_var_string_value((const char*)data, length); 
        break;
    case SQL_FIXCHAR:
    case SQL_NUMERIC:
    case SQL_SNUMERIC:
	_real_size = MIN(length, _max_size);
	assert(_data_size >= _real_size);
        //	memset(_data, '\0', _data_size);
	memcpy(_value._string, data, _real_size); 
        break;
    }
}



/*********************************************************************
 *
 *  @fn:    set min/max value function
 *
 *  @brief: Set the min/max possible value for this field type
 *
 *********************************************************************/

inline void field_value_t::set_min_value()
{
    assert (_pfield_desc);
    _null_flag = false;

    switch (_pfield_desc->type()) {
    case SQL_BIT:
	_value._bit = false;
	break;
    case SQL_SMALLINT:
	_value._smallint = MIN_SMALLINT;
	break;
    case SQL_CHAR:
	_value._char = MIN_SMALLINT;
	break;
    case SQL_INT:
	_value._int = MIN_INT;
	break;
    case SQL_FLOAT:
	_value._float = MIN_FLOAT;
	break;
    case SQL_LONG:
	_value._long = MIN_FLOAT;
	break;
    case SQL_VARCHAR:
    case SQL_FIXCHAR:
    case SQL_NUMERIC:
    case SQL_SNUMERIC:
	_data[0] = '\0';
	_value._string = _data;
	break;
    case SQL_TIME:
	break;
    }
}


inline void field_value_t::set_max_value()
{
    assert (_pfield_desc);

    _null_flag = false;

    switch (_pfield_desc->type()) {
    case SQL_BIT:
	_value._bit = true;
	break;
    case SQL_SMALLINT:
	_value._smallint = MAX_SMALLINT;
	break;
    case SQL_CHAR:
	_value._char = 'z';
	break;
    case SQL_INT:
	_value._int = MAX_INT;
	break;
    case SQL_FLOAT:
	_value._float = MAX_FLOAT;
	break;
    case SQL_LONG:
	_value._long = MAX_FLOAT;
	break;
    case SQL_VARCHAR:
    case SQL_FIXCHAR:
	memset(_data, 'z', _data_size);
	_value._string = _data;
	break;
    case SQL_NUMERIC:
    case SQL_SNUMERIC:
	memset(_data, '9', _data_size);
	_value._string = _data;
	break;
    case SQL_TIME:
	break;
    }
}



/*********************************************************************
 *
 *  @fn:    copy_value
 *
 *  @brief: Copy the 'current' value of of the field to an address
 *
 *********************************************************************/

inline bool field_value_t::copy_value(void* data) const
{
    assert (_pfield_desc);
    assert (!_null_flag);

    switch (_pfield_desc->type()) {
    case SQL_BIT:
        memcpy(data, &_value._bit, _max_size);
        break;
    case SQL_SMALLINT:
        memcpy(data, &_value._smallint, _max_size);
        break;
    case SQL_CHAR:
        memcpy(data, &_value._char, _max_size);
        break;
    case SQL_INT:
        memcpy(data, &_value._int, _max_size);
        break;
    case SQL_FLOAT:
        memcpy(data, &_value._float, _max_size);
        break;
    case SQL_LONG:
        memcpy(data, &_value._long, _max_size);
        break;
    case SQL_TIME:
        memcpy(data, _value._time, _real_size);
        break;
    case SQL_VARCHAR:
        memcpy(data, _value._string, _real_size);
        break;
    case SQL_FIXCHAR:
    case SQL_NUMERIC:
    case SQL_SNUMERIC:
        memcpy(data, _value._string, _real_size);
        break;
    }

    return (true);
}



/*********************************************************************
 *
 *  @fn:    set_XXX_value
 *
 *  @brief: Type-specific set of value 
 *
 *********************************************************************/

inline void field_value_t::set_int_value(const int data)
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_INT);
    _null_flag = false;
    _value._int = data;
}


inline void field_value_t::set_bit_value(const bool data)
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_BIT);
    _null_flag = false;
    _value._bit = data;
}

inline void field_value_t::set_smallint_value(const short data)
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_SMALLINT);
    _null_flag = false;
    _value._smallint = data;
}

inline void field_value_t::set_char_value(const char data)
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_CHAR);
    _null_flag = false;
    _value._char = data;
}

inline void field_value_t::set_float_value(const double data)
{ 
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_FLOAT);
    _null_flag = false;
    _value._float = data;
}

inline void field_value_t::set_long_value(const long long data)
{ 
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_LONG);
    _null_flag = false;
    _value._long = data;
}

inline void field_value_t::set_decimal_value(const decimal data)
{ 
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_FLOAT);
    _null_flag = false;
    _value._float = data.to_double();
}

inline void field_value_t::set_time_value(const time_t data)
{ 
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_FLOAT);
    _null_flag = false;
    _value._float = data;
}

inline void field_value_t::set_tstamp_value(const timestamp_t& data)
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_TIME);
    _null_flag = false;
    memcpy(_value._time, &data, _real_size);
}


/*********************************************************************
 *
 *  @fn:    set_fixed_string_value
 *
 *  @brief: Copy the string to the data buffer using fixed lengths
 *
 *********************************************************************/

inline void field_value_t::set_fixed_string_value(const char* string,
                                                  const uint len)
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_FIXCHAR || 
            _pfield_desc->type() == SQL_NUMERIC || 
            _pfield_desc->type() == SQL_SNUMERIC);
    /** if fixed length string then the data buffer has already 
     *  at least _data_size bits allocated */
    _real_size = MIN(len, _max_size);
    assert (_data_size >= _real_size);
    _null_flag = false;
    //    memset(_value._string, '\0', _data_size);
    memcpy(_value._string, string, _real_size);
}


/*********************************************************************
 *
 *  @fn:    set_var_string_value
 *
 *  @brief: Copy the string to the data buffer and set real_size
 *
 *  @note:  Only len chars are copied. If len > field->fieldsize() then only
 *          fieldsize() chars are copied.
 *
 *********************************************************************/

inline void field_value_t::set_var_string_value(const char* string,
                                                const uint len)
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_VARCHAR);
    _real_size = MIN(len, _max_size);
    alloc_space(_real_size);
    assert (_data_size >= _real_size);
    _null_flag = false;
    memcpy(_value._string, string, _real_size);
}



/*********************************************************************
 *
 *  @fn:    get_XXX_value
 *
 *  @brief: Type-specific return of value 
 *
 *********************************************************************/


inline int field_value_t::get_int_value() const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_INT);
    return (_value._int);
}

inline bool field_value_t::get_bit_value() const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_BIT);
    return (_value._bit);
}

inline short field_value_t::get_smallint_value() const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_SMALLINT);
    return (_value._smallint);
}

inline char field_value_t::get_char_value() const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_CHAR);
    return (_value._char);
}

inline void field_value_t::get_string_value(char* buffer,
                                            const uint bufsize) const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_FIXCHAR || 
            _pfield_desc->type() == SQL_VARCHAR ||
            _pfield_desc->type() == SQL_NUMERIC || 
            _pfield_desc->type() == SQL_SNUMERIC);
    memset(buffer, '\0', bufsize);
    memcpy(buffer, _value._string, MIN(bufsize, _real_size));
}

inline double field_value_t::get_float_value() const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_FLOAT);
    return (_value._float);
}

inline long long field_value_t::get_long_value() const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_LONG);
    return (_value._long);
}

inline decimal field_value_t::get_decimal_value() const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_FLOAT);
    return (decimal(_value._float));
}

inline time_t field_value_t::get_time_value() const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_FLOAT);
    return ((time_t)_value._float);
}

inline timestamp_t& field_value_t::get_tstamp_value() const
{
    assert (_pfield_desc);
    assert (_pfield_desc->type() == SQL_TIME);
    return *(_value._time);
}



EXIT_NAMESPACE(shore);

#endif /* __SHORE_FIELD_H */
