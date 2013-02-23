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

/** @file:   shore_iter.cpp
 *
 *  @brief:  Simple file and index scanners 
 *
 *  @author: Ippokratis Pandis, May 2010
 *
 */

#include "sm/shore/shore_iter.h"

ENTER_NAMESPACE(shore);


/******************************************************************
 * 
 * @class: simple_table_iter_t
 *
 ******************************************************************/

simple_table_iter_t::simple_table_iter_t(ss_m* db, file_desc_t* file, 
                                         lock_mode_t alm)
    : _db(db), _opened(false), _file(file), _lm(alm)
{
    assert (_db);
}

simple_table_iter_t::~simple_table_iter_t()
{
    close_scan();
}


w_rc_t simple_table_iter_t::open_scan()
{
    if (!_opened) {
        assert (_db);
        W_DO(_file->check_fid(_db));
        _scanner = new scan_file_i(_file->fid(), 
                                   ss_m::t_cc_record, 
                                   false, _lm);
        _opened = true;
    }
    return (RCOK);
}


w_rc_t simple_table_iter_t::next(bool& eof, pin_i*& handle)
{
    if (!_opened) open_scan();
    W_DO(_scanner->next(handle, 0, eof));
    return (RCOK);
}


w_rc_t simple_table_iter_t::close_scan()
{
    _opened = false;
    return (RCOK);
}


pin_i* simple_table_iter_t::cursor() 
{
    pin_i *rval;
    bool eof;
    _scanner->cursor(rval, eof);
    return (eof? NULL : rval);
}



/******************************************************************
 * 
 * @class: simple_index_iter_t
 *
 ******************************************************************/

simple_index_iter_t::simple_index_iter_t(ss_m* db, index_desc_t* index, 
                                         lock_mode_t alm)
    : _db(db), _opened(false), _idx(index), _lm(alm)
{
    assert (_db);
}

simple_index_iter_t::~simple_index_iter_t()
{
    close_scan();
}


// To index scan all the entries use the vec_t::neg_inf and vec_t::pos_inf 
//
// scan_index_i scan(fid,             
//                   scan_index_i::ge, vec_t::neg_inf,
//                   scan_index_i::le, vec_t::pos_inf, false,
//                   ss_m::t_cc_kvl);

w_rc_t simple_index_iter_t::open_scan(uint pnum, 
                                      scan_index_i::cmp_t c1, const cvec_t& bound1,
                                      scan_index_i::cmp_t c2, const cvec_t& bound2)
{
    _pnum = pnum;

    if (!_opened) {
            
        // 1. figure out what concurrency will be used
        // !! according to shore-mt/src/scan.h:82 
        //    t_cc_kvl  - IS lock on the index and SH key-value locks on every entry encountered
        //    t_cc_none - IS lock on the index and no other locks
        ss_m::concurrency_t cc = ss_m::t_cc_im;
        if (_lm==NL) cc = ss_m::t_cc_none;

        // 2. open the cursor
        W_DO(_idx->check_fid(_db));
        _scanner = new scan_index_i(_idx->fid(_pnum), 
                                    c1, bound1, c2, bound2,
                                    false, cc, 
                                    _lm);
        _opened = true;
    }
    return (RCOK);
}


w_rc_t simple_index_iter_t::next(bool& eof, rid_t& rid)
{
    assert (_opened);

    W_DO(_scanner->next(eof));

    if (!eof) {        
        rid_t    tmprid;
        vec_t    record(&tmprid, sizeof(rid_t));
        smsize_t elen = sizeof(rid_t);
        
        vec_t tmpvec;
        smsize_t tmpsz=0;

        W_DO(_scanner->curr(&tmpvec, tmpsz, &record, elen));
        rid = tmprid;
    }    
    return (RCOK);
}


w_rc_t simple_index_iter_t::close_scan()
{
    _opened = false;
    return (RCOK);
}


EXIT_NAMESPACE(shore);
