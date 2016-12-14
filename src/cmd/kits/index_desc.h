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

/** @file:   shore_index.h
 *
 *  @brief:  Description of an index.
 *
 *  All the secondary indexes on the table are linked together.
 *  An index is described by an array of serial number of fields.
 *
 *  @author: Mengzhi Wang, April 2001
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __INDEX_DESC_H
#define __INDEX_DESC_H

#include "sm_vas.h"

#include "error.h"
#include "file_desc.h"
//#include "iter.h"

#include "AtomicCounter.hpp"


class table_desc_t;


/******************************************************************
 *
 *  @class: index_desc_t
 *
 *  @brief: Description of a Shore index.
 *
 *  @note:  Even the variable length fields are treated as fixed
 *          length, with their maximum possible size.
 *
 ******************************************************************/

class index_desc_t
{
    // this is needed at least for accessing the lock
    friend class table_desc_t;
private:
    table_desc_t*   _table;

    StoreID _stid;
    string _name;

    unsigned*       _key;                      /* index of fields in the base table */
    unsigned        _field_count;
    bool            _unique;                   /* whether allow duplicates or not */
    bool            _primary;                  /* is it primary or not */
    bool            _nolock;                   /* is it using locking or not */
    bool            _latchless;                /* does it use any latches at all */
    bool            _rmapholder;               /* it is used only for the range mapping */

    // CS: removed volatile, which has nothing to do with thread safety!
    unsigned _maxkeysize;               /* maximum key size */

    char            _keydesc[MAX_KEYDESC_LEN]; /* buffer for the index key description */
    tatas_lock      _keydesc_lock;             /* lock for the key desc */


public:

    /* ------------------- */
    /* --- constructor --- */
    /* ------------------- */

    index_desc_t(table_desc_t* table,
                 string name, const int fieldcnt,
                 const unsigned* fields,
                 bool unique=true, bool primary=false,
                 const uint32_t& pd=PD_NORMAL,
                 bool rmapholder=false);

    ~index_desc_t();

    string  name() const { return _name; }
    unsigned field_count() const { return _field_count; }
    table_desc_t* table() const { return _table; }


    bool          is_fid_valid() const { return (_stid != 0); }

    StoreID& stid() { return _stid; }
    void set_stid(StoreID const &stid) { _stid = stid; }

    w_rc_t load_stid(StoreID cat_stid);

    /* ---------------------- */
    /* --- access methods --- */
    /* ---------------------- */

    inline bool is_unique() const { return (_unique); }
    inline bool is_primary() const { return (_primary); }
    inline bool is_relaxed() const { return (_nolock); }
    inline bool is_latchless() const { return (_latchless); }
    inline bool is_rmapholder() const { return (_rmapholder); }

    inline int  get_keysize() { return (*&_maxkeysize); }
    inline void set_keysize(const unsigned sz)
    {
        //atomic_swap_uint(&_maxkeysize, sz);
        lintel::unsafe::atomic_exchange(&_maxkeysize, sz);
    }

    bool is_key_index(unsigned i)
    {
        for (unsigned j = 0; j < _field_count; j++) {
            if (_key[j] == i) return true;
        }
        return false;
    }

    // find the index_desc_t by name
    bool matches_name(const char* name);

    int key_index(const unsigned index) const;


    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    void print_desc(ostream& os);

}; // EOF: index_desc_t



#endif /* __INDEX_DESC_H */
