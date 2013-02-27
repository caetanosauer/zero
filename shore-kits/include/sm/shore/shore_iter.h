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

/** @file:   shore_iter.h
 *
 *  @brief:  Base class for iterators over certain set of tuples
 *
 *  @author: Ippokratis Pandis, January 2008
 *  @author: Mengzhi Wang, April 2001
 *
 */

#ifndef __SHORE_ITER_H
#define __SHORE_ITER_H

#include "sm_vas.h"
#include "util.h"

#include "sm/shore/shore_error.h"
#include "sm/shore/shore_file_desc.h"
#include "sm/shore/shore_index.h"


ENTER_NAMESPACE(shore);


/****************************************************************** 
 *
 *  @class: simple_table_iter_t
 *
 *  @brief: Class that can be used for Shore table scans
 *
 ******************************************************************/

class simple_table_iter_t 
{
protected:
    ss_m*         _db;
    bool          _opened;  // whether the init is successful
    file_desc_t*  _file;
    lock_mode_t   _lm;

    guard<scan_file_i>  _scanner;
    
public:

    simple_table_iter_t(ss_m* db, file_desc_t* file, lock_mode_t alm);
    ~simple_table_iter_t();

    bool opened() const { return (_opened); }

    w_rc_t open_scan(); 

    w_rc_t next(bool& eof, pin_i*& handle);

    w_rc_t close_scan();

    pin_i* cursor();

}; // EOF: simple_table_iter_t



/****************************************************************** 
 *
 *  @class: simple_index_iter_t
 *
 *  @brief: Class that can be used for Shore index scans
 *
 ******************************************************************/

class index_desc_t;

class simple_index_iter_t 
{
protected:
    ss_m*         _db;
    bool          _opened;  // whether the init is successful
    index_desc_t* _idx;
    lock_mode_t   _lm;

    uint                 _pnum;

    guard<scan_index_i>  _scanner;
    
public:

    simple_index_iter_t(ss_m* db, index_desc_t* file, lock_mode_t alm);
    ~simple_index_iter_t();

    bool opened() const { return (_opened); }

    w_rc_t open_scan(uint pnum,
                     scan_index_i::cmp_t c1, const cvec_t& bound1,
                     scan_index_i::cmp_t c2, const cvec_t& bound2);

    w_rc_t next(bool& eof, rid_t& rid);

    w_rc_t close_scan();

}; // EOF: simple_index_iter_t



/****************************************************************** 
 *
 *  @class: tuple_iter
 *
 *  @brief: Abstract class which is the base for table and index 
 *          scan iterators (tscan - iscan).
 *
 ******************************************************************/

template <class file_desc, class file_scanner, class rowpointer>
class tuple_iter_t 
{
protected:
    ss_m*         _db;
    bool          _opened;  /* whether the init is successful */
    file_desc*    _file;
    file_scanner* _scan;

    lock_mode_t   _lm;
    bool          _shoulddelete;    
    
public:
    // Constructor
    tuple_iter_t(ss_m* db, file_desc* file, 
                 lock_mode_t alm, bool shoulddelete)
        //                 lock_mode_t alm = SH, bool shoulddelete = false)
        : _db(db), _opened(false), _file(file), _scan(NULL), 
          _lm(alm), _shoulddelete(shoulddelete)
    { 
        assert (_db);
    }

    virtual ~tuple_iter_t() { close_scan(); }

    // Access methods
    bool opened() const { return (_opened); }


    /* ------------------------- */
    /* --- iteration methods --- */
    /* ------------------------- */

    // virtual w_rc_t open_scan()=0; 

    virtual w_rc_t next(ss_m* db, bool& eof, rowpointer& tuple)=0;

    w_rc_t close_scan() {
        if ((_opened) && (_shoulddelete)) { 
            // be careful: if <file_scanner> is scalar, it should NOT be deleted!
            assert (_scan);
            delete (_scan);
            _scan = NULL;
        }
        _opened = false;
        return (RCOK);
    }    

}; // EOF: tuple_iter_t



EXIT_NAMESPACE(shore);

#endif /* __SHORE_ITER_H */
