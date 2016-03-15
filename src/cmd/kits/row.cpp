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

/** @file:   row.cpp
 *
 *  @brief:  Implementation of the base class for records (rows) of tables in Shore
 *
 *  @author: Ippokratis Pandis, January 2008
 *  @author: Caetano Sauer, April 2015
 *
 */

#include "row.h"
#include "table_desc.h"
#include "index_desc.h"


#define VAR_SLOT(start, offset)   ((start)+(offset))
#define SET_NULL_FLAG(start, offset)                            \
    (*(char*)((start)+((offset)>>3))) &= (1<<((offset)>>3))
#define IS_NULL_FLAG(start, offset)                     \
    (*(char*)((start)+((offset)>>3)))&(1<<((offset)>>3))


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

rep_row_t::rep_row_t(blob_pool* apts)
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

void rep_row_t::set(const unsigned nsz)
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

void rep_row_t::set_ts(blob_pool* apts, const unsigned nsz)
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
      _pvalues(NULL),
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

    unsigned var_count  = 0;
    unsigned fixed_size = 0;

    // setup each field and calculate offsets along the way
    for (unsigned i=0; i<_field_cnt; i++) {
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

unsigned table_row_t::size() const
{
    assert (_is_setup);

    unsigned size = 0;

    /* for a fixed length field, it just takes as much as the
     * space for the value itself to store.
     * for a variable length field, we store as much as the data
     * and the offset to tell the length of the data.
     * Of course, there is a bit for each nullable field.
     */

    for (unsigned i=0; i<_field_cnt; i++) {
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


void _load_signed_int(char* dest, char* src, size_t nbytes)
{
    // invert sign bit and reverse endianness
    size_t lsb = nbytes - 1;
    for (size_t i = 0; i < nbytes; i++) {
        dest[i] = src[lsb - i];
    }
    dest[lsb] ^= 0x80;
}

void table_row_t::load_key(char* data, index_desc_t* pindex)
{
    char buffer[8];
    char* pos = data;
    unsigned field_cnt = pindex ? pindex->field_count() : _field_cnt;
    for (unsigned j = 0; j < field_cnt; j++) {
        unsigned i = pindex ? pindex->key_index(j) : j;
        field_value_t& field = _pvalues[i];
        field_desc_t* fdesc = field._pfield_desc;

        w_assert1(field.is_setup());

        if (fdesc->allow_null()) {
            bool is_null = (*pos == true);
            field.set_null(is_null);
            pos++;
            if (is_null) {
                continue;
            }
        }

        switch (fdesc->type()) {
            case SQL_BIT: {
                field.set_bit_value(*pos);
                pos++;
                break;
            }
            case SQL_SMALLINT: {
                memcpy(buffer, pos, 2);
                _load_signed_int(buffer, pos, 2);
                field.set_smallint_value(*(int16_t*) buffer);
                pos += 2;
                break;
            }
            case SQL_INT: {
                memcpy(buffer, pos, 4);
                _load_signed_int(buffer, pos, 4);
                field.set_int_value(*(int32_t*) buffer);
                pos += 4;
                break;
            }
            case SQL_LONG: {
                memcpy(buffer, pos, 8);
                _load_signed_int(buffer, pos, 8);
                field.set_long_value(*(int64_t*) buffer);
                pos += 8;
                break;
            }
            case SQL_FLOAT: {
                memcpy(buffer, pos, 8);
                // invert endianness and handle sign bit
                if (buffer[0] & 0x80) {
                    // inverted sign bit == 1 -> positive
                    // invert only sign bit
                    for (int i = 0; i < 8; i++) {
                        buffer[i] = pos[7 - i];
                    }
                    buffer[7] ^= 0x80;
                }
                else {
                    // otherwise invert all bits
                    for (int i = 0; i < 8; i++) {
                        buffer[i] = pos[7 - i] ^ 0x80;
                    }
                }
                field.set_float_value(*(double*) buffer);
                pos += 8;
                break;
            }
            case SQL_CHAR:
                field.set_char_value(*pos);
                pos++;
                break;
            case SQL_FIXCHAR: {
                size_t len = strlen(pos);
                field.set_fixed_string_value(pos, len);
                pos += len + 1;
                break;
            }
            case SQL_VARCHAR: {
                size_t len = strlen(pos);
                field.set_var_string_value(pos, len);
                pos += len + 1;
                break;
            }
            default:
                throw runtime_error("Serialization not supported for the \
                        given type");
        }
    }
}

void table_row_t::load_value(char* data, index_desc_t* pindex)
{
    // Read the data field by field
    assert (data);

    // 1. Get the pre-calculated offsets

    // current offset for fixed length field values
    offset_t fixed_offset = get_fixed_offset();

    // current offset for variable length field slots
    offset_t var_slot_offset = get_var_slot_offset();

    // current offset for variable length field values
    offset_t var_offset = get_var_offset();


    // 2. Read the data field by field
    int null_index = -1;
    for (unsigned i=0; i<_ptable->field_count(); i++) {

        if (pindex) {
            bool skip = false;
            // skip key fields
            for (unsigned j = 0; j < pindex->field_count(); j++) {
                if ((int) i == pindex->key_index(j)) {
                    skip = true;
                    break;
                }
            }
            if (skip) continue;
        }

        // Check if the field can be NULL.
        // If it can be NULL, increase the null_index,
        // and check if the bit in the null_flags bitmap is set.
        // If it is set, set the corresponding value in the tuple
        // as null, and go to the next field, ignoring the rest
	if (_pvalues[i].field_desc()->allow_null()) {
	    null_index++;
	    if (IS_NULL_FLAG(data, null_index)) {
		_pvalues[i].set_null();
		continue;
	    }
	}

        // Check if the field is of VARIABLE length.
        // If it is, copy the offset of the value from the offset part of the
        // buffer (pointed by var_slot_offset). Then, copy that many chars from
        // the variable length part of the buffer (pointed by var_offset).
        // Then increase by one offset index, and offset of the pointer of the
        // next variable value
	if (_pvalues[i].is_variable_length()) {
	    offset_t var_len;
	    memcpy(&var_len,  VAR_SLOT(data, var_slot_offset), sizeof(offset_t));
            _pvalues[i].set_value(data+var_offset, var_len);
	    var_offset += var_len;
	    var_slot_offset += sizeof(offset_t);
	}
	else {
            // If it is of FIXED length, copy the data from the fixed length
            // part of the buffer (pointed by fixed_offset), and the increase
            // the fixed offset by the (fixed) size of the field
            _pvalues[i].set_value(data+fixed_offset,
                    _pvalues[i].maxsize());
	    fixed_offset += _pvalues[i].maxsize();
	}
    }
}

void _store_signed_int(char* dest, char* src, size_t nbytes)
{
    // reverse endianness and invert sign bit
    size_t lsb = nbytes - 1;
    for (size_t i = 0; i < nbytes; i++) {
        dest[i] = src[lsb - i];
    }
    dest[0] ^= 0x80;
}

void table_row_t::store_key(char* data, size_t& length, index_desc_t* pindex)
{
    size_t req_size = 0;
    char buffer[8];
    char* pos = data;
    unsigned field_cnt = pindex ? pindex->field_count() : _field_cnt;
    for (unsigned j = 0; j < field_cnt; j++) {
        unsigned i = pindex ? pindex->key_index(j) : j;
        field_value_t& field = _pvalues[i];
        field_desc_t* fdesc = field._pfield_desc;
        w_assert1(field.is_setup());

        req_size += field.realsize();
        if (fdesc->allow_null()) { req_size++; }
        if (length < req_size) {
            throw runtime_error("Tuple does not fit on given buffer");
        }

        if (fdesc->allow_null()) {
            if (field.is_null()) {
                // copy a zero byte to the output and proceed
                *pos = 0x00;
                pos++;
                continue;
            }
            else {
                // non-null nullable fields require a 1 prefix
                *pos = 0xFF;
                pos++;
            }
        }

        switch (fdesc->type()) {
            case SQL_BIT: {
                bool v = field.get_bit_value();
                *pos = v ? 0xFF : 0x00;
                break;
            }
            case SQL_SMALLINT: {
                field.copy_value(buffer);
                _store_signed_int(pos, buffer, 2);
                pos += 2;
                break;
            }
            case SQL_INT: {
                field.copy_value(buffer);
                _store_signed_int(pos, buffer, 4);
                pos += 4;
                break;
            }
            case SQL_LONG: {
                field.copy_value(buffer);
                _store_signed_int(pos, buffer, 8);
                pos += 8;
                break;
            }
            case SQL_FLOAT: {
                field.copy_value(buffer);
                // invert endianness and handle sign bit
                if (buffer[0] & 0x80) {
                    // if negative, invert all bits
                    for (int i = 0; i < 8; i++) {
                        pos[i] = buffer[7 - i] ^ 0xFF;
                    }
                }
                else {
                    // otherwise invert only sign bit
                    for (int i = 0; i < 8; i++) {
                        pos[i] = buffer[7 - i];
                    }
                    pos[0] ^= 0x80;
                }
                pos += 8;
                break;
            }
            case SQL_CHAR:
                *pos = field.get_char_value();
                pos++;
                break;
            case SQL_FIXCHAR:
            case SQL_VARCHAR:
            // Assumption is that strings already include the terminating zero
                field.copy_value(pos);
                pos += field.realsize();
                w_assert1(*(pos - 1) == 0);
                break;
            default:
                throw runtime_error("Serialization not supported for the \
                        given type");
        }
    }
    length = req_size;
    w_assert1(pos - data == (long) length);
}

void table_row_t::store_value(char* data, size_t& length, index_desc_t* pindex)
{
    // 1. Get the pre-calculated offsets

    // current offset for fixed length field values
    offset_t fixed_offset = get_fixed_offset();

    // current offset for variable length field slots
    offset_t var_slot_offset = get_var_slot_offset();

    // current offset for variable length field values
    offset_t var_offset = get_var_offset();



    // 2. calculate the total space of the tuple
    //   (tupsize)    : total space of the tuple

    int tupsize    = 0;

    int null_count = get_null_count();
    int fixed_size = get_var_slot_offset() - get_fixed_offset();

    // loop over all the variable-sized fields and add their real size (set at ::set())
    for (unsigned i=0; i<_ptable->field_count(); i++) {
	if (_pvalues[i].is_variable_length()) {
            // If it is of VARIABLE length, then if the value is null
            // do nothing, else add to the total tuple length the (real)
            // size of the value plus the size of an offset.

            if (_pvalues[i].is_null()) continue;
            tupsize += _pvalues[i].realsize();
            tupsize += sizeof(offset_t);
	}

        // If it is of FIXED length, then increase the total tuple
        // length, as well as, the size of the fixed length part of
        // the tuple by the fixed size of this type of field.

        // IP: The length of the fixed-sized fields is added after the loop
    }

    // Add up the length of the fixed-sized fields
    tupsize += fixed_size;

    // In the total tuple length add the size of the bitmap that
    // shows which fields can be NULL
    if (null_count) tupsize += (null_count >> 3) + 1;
    assert (tupsize);

    if ((long) length < tupsize) {
        throw runtime_error("Tuple does not fit on allocated buffer");
    }
    length = tupsize;

    // 4. Copy the fields to the array, field by field

    int null_index = -1;
    // iterate over all fields
    for (unsigned i=0; i<_ptable->field_count(); i++) {

        // skip fields which are part of the given index
        if (pindex) {
            bool skip = false;
            for (unsigned j=0; j<pindex->field_count(); j++) {
                if ((int) i == pindex->key_index(j)) {
                    skip = true;
                    break;
                }
            }
            if (skip) continue;
        }

        // Check if the field can be NULL.
        // If it can be NULL, increase the null_index, and
        // if it is indeed NULL set the corresponding bit
	if (_pvalues[i].field_desc()->allow_null()) {
	    null_index++;
	    if (_pvalues[i].is_null()) {
		SET_NULL_FLAG(data, null_index);
	    }
	}

        // Check if the field is of VARIABLE length.
        // If it is, copy the field value to the variable length part of the
        // buffer, to position  (buffer + var_offset)
        // and increase the var_offset.
	if (_pvalues[i].is_variable_length()) {
            _pvalues[i].copy_value(data + var_offset);
            int offset = _pvalues[i].realsize();
	    var_offset += offset;

            // set the offset
            offset_t len = offset;
            memcpy(VAR_SLOT(data, var_slot_offset), &len,
                    sizeof(offset_t));
	    var_slot_offset += sizeof(offset_t);
	}
	else {
            // If it is of FIXED length, then copy the field value to the
            // fixed length part of the buffer, to position
            // (buffer + fixed_offset)
            // and increase the fixed_offset
            _pvalues[i].copy_value(data + fixed_offset);
	    fixed_offset += _pvalues[i].maxsize();
	}
    }
}


/* ----------------- */
/* --- debugging --- */
/* ----------------- */

/* For debug use only: print the value of all the fields of the tuple */
void table_row_t::print_values(ostream& os)
{
    assert (_is_setup);
    //  cout << "Number of fields: " << _field_count << endl;
    for (unsigned i=0; i<_field_cnt; i++) {
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
    for (unsigned i=0; i<_field_cnt; i++) {
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
    for (unsigned i=0; i<_field_cnt; i++) {
        sz = _pvalues[i].get_debug_str(sbuf);
        if (sbuf) {
            fprintf( stderr, "%d. %s (%d)\n", i, sbuf, sz);
            delete [] sbuf;
            sbuf = NULL;
        }
    }
}



#include <sstream>
char const* db_pretty_print(table_row_t const* rec, int /* i=0 */, char const* /* s=0 */)
{
    static char data[1024];
    std::stringstream inout(data,stringstream::in | stringstream::out);
    //std::strstream inout(data, sizeof(data));
    ((table_row_t*)rec)->print_values(inout);
    inout << std::ends;
    return data;
}
