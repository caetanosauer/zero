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

#include "table_man.h"
#include "table_desc.h"
#include "scan.h"
#include "xct.h"
#include "w_key.h"

/*********************************************************************
 *
 *  @fn:    load_and_register_fid
 *
 *  @brief: filling fid values of this table and its indexes
 *          as well as registering it
 *
 *********************************************************************/

template<class T>
w_rc_t table_man_t<T>::load_and_register_fid()
{
    assert (_ptable);

    // fetch stid of all indexes from catalog index
    W_DO(_ptable->load_stids());

    return (RCOK);
}



/*********************************************************************
 *
 *  @fn:    index_probe
 *
 *  @brief: Finds the rid of the specified key using a certain index
 *
 *  @note:  The key is parsed from the tuple that it is passed as parameter
 *
 *********************************************************************/

template<class T>
w_rc_t table_man_t<T>::index_probe(
                                index_desc_t* pindex,
                                table_row_t*  ptuple,
                                lock_mode_t   /* lock_mode */,
                                const PageID& /* root */)
{
    assert (_ptable);
    assert (pindex);
    assert (ptuple);
    assert (ptuple->_rep);

    bool     found = false;
    // smsize_t len = sizeof(rid_t);

    // if index created with NO-LOCK option (e.g., DORA) then:
    // - ignore lock mode (use NL)
    // - find_assoc ignoring any locks
    // if (pindex->is_relaxed()) {
    //     lock_mode   = NL;
    // }

    // extract serialized key into _rep_key
    size_t key_sz = ptuple->_rep_key->_bufsz;
    ptuple->store_key(ptuple->_rep_key->_dest, key_sz, pindex);
    assert (ptuple->_rep_key->_dest); // if NULL invalid key
    w_keystr_t kstr;
    kstr.construct_regularkey(ptuple->_rep_key->_dest, key_sz);

    if (pindex == table()->primary_idx()) {
        // If we are probing the primary index, then we just need
        // to fetch the non-key fields into the tuple
        int fields_sz = ptuple->_ptable->maxsize();
        ptuple->_rep->set(fields_sz);

        smsize_t len = ptuple->_rep->_bufsz;
        W_DO(btree_m::lookup(pindex->stid(), kstr, ptuple->_rep->_dest, len,
                    found));
        if (!found) return RC(se_TUPLE_NOT_FOUND);

        // load the non-key fields into the tuple
        ptuple->load_value(ptuple->_rep->_dest, pindex);
    }
    else {
        // get (max) length of the reference key
        // place ref key into tuple buffer, overwriting previous key
        // CS TODO -- here we assume that there is enough space in the buffer
        // for the value. Otherwise, db returns eWONTFIT and we must expand
        // the buffer. However, this should never happen because the benchmarks
        // always allocate space for the maximum possible tuple. This stupid
        // design should be changed at some point, but it works for now.
        smsize_t len = ptuple->_rep_key->_bufsz;
        // int ref_sz = key_size(ptuple->_ptable->primary_idx());
        // ptuple->_rep_key->set(ref_sz);
        W_DO(btree_m::lookup(pindex->stid(), kstr, ptuple->_rep_key->_dest, len,
                    found));

        if (!found) return RC(se_TUPLE_NOT_FOUND);

        // read the tuple from the primary index
        kstr.construct_regularkey(ptuple->_rep_key->_dest, len);
        W_DO(btree_m::lookup(pindex->table()->get_primary_stid(), kstr,
                    ptuple->_rep->_dest, len, found));

        if (!found) return RC(se_TUPLE_NOT_FOUND);
    }

    return (RCOK);
}




/* -------------------------- */
/* --- tuple manipulation --- */
/* -------------------------- */



/*********************************************************************
 *
 *  @fn:    add_tuple
 *
 *  @brief: Inserts a tuple to a table and all the indexes of the table
 *
 *  @note:  This function should be called in the context of a trx.
 *          The passed tuple should be formed. If everything goes as
 *          expected the _rid of the tuple will be set.
 *
 *********************************************************************/

template<class T>
w_rc_t table_man_t<T>::add_tuple(
                              table_row_t* ptuple,
                              const lock_mode_t /* lock_mode */,
                              const PageID& /* primary_root */)
{
    assert (_ptable);
    assert (ptuple);
    assert (ptuple->_rep);
    assert (ptuple->_rep_key);
    // uint32_t system_mode = _ptable->get_pd();

    // figure out what mode will be used
    // bool bIgnoreLocks = false;
    // if (lock_mode==NL) bIgnoreLocks = true;

    index_desc_t* pindex = _ptable->primary_idx();

    // build tuple data without index fields
    size_t tsz = ptuple->_rep->_bufsz;
    ptuple->store_value(ptuple->_rep->_dest, tsz, pindex);
    assert (ptuple->_rep->_dest); // if NULL invalid

    // build key data
    w_keystr_t kstr;
    size_t ksz = ptuple->_rep_key->_bufsz;
    ptuple->store_key(ptuple->_rep_key->_dest, ksz, pindex);
    kstr.construct_regularkey(ptuple->_rep_key->_dest, ksz);

    W_DO(btree_m::insert(pindex->stid(), kstr, vec_t(ptuple->_rep->_dest, tsz)));

    // update the indexes
    const std::vector<index_desc_t*>& indexes = _ptable->get_indexes();
    for (size_t i = 0; i < indexes.size(); i++) {
        size_t sec_ksz = ptuple->_rep->_bufsz;
        ptuple->store_key(ptuple->_rep->_dest, sec_ksz, indexes[i]);
        w_keystr_t sec_kstr;
        sec_kstr.construct_regularkey(ptuple->_rep->_dest, sec_ksz);

        // primary key value (i.e., pointer) is stored in _rep_key
        W_DO(btree_m::insert(indexes[i]->stid(),
                    sec_kstr,
                    vec_t(ptuple->_rep_key->_dest, ksz)
                    ));
    }
    return (RCOK);
}




/*********************************************************************
 *
 *  @fn:    add_index_entry
 *
 *  @brief: Inserts a tuple's entry to the given index
 *
 *  @note:  This function should be called in the context of a trx.
 *
 *********************************************************************/

template<class T>
w_rc_t table_man_t<T>::add_index_entry(
				    const char* idx_name,
				    table_row_t* ptuple,
				    const lock_mode_t /* lock_mode */,
				    const PageID& /* primary_root */)
{
    assert (_ptable);
    assert (ptuple);
    assert (ptuple->_rep);

    // get the index
    index_desc_t* pindex = _ptable->find_index(idx_name);
    assert (pindex);

    // if (!ptuple->is_rid_valid()) return RC(se_NO_CURRENT_TUPLE);

    // uint32_t system_mode = _ptable->get_pd();

    // figure out what mode will be used
    // bool bIgnoreLocks = false;
    // if (lock_mode==NL) bIgnoreLocks = true;

    // build primary key value (i.e., pointer)
    size_t ksz = ptuple->_rep_key->_bufsz;
    ptuple->store_key(ptuple->_rep_key->_dest, ksz,
            ptuple->_ptable->primary_idx());

    // update the index
    size_t sec_ksz = ptuple->_rep->_bufsz;
    ptuple->store_key(ptuple->_rep->_dest, ksz, pindex);
    w_keystr_t sec_kstr;
    sec_kstr.construct_regularkey(ptuple->_rep->_dest, sec_ksz);
    W_DO(btree_m::insert(pindex->stid(),
                sec_kstr,
                vec_t(ptuple->_rep_key->_dest, ksz)
                ));

    return (RCOK);
}

/*********************************************************************
 *
 *  @fn:    delete_tuple
 *
 *  @brief: Deletes a tuple from a table and the corresponding entries
 *          on all the indexes of the table
 *
 *  @note:  This function should be called in the context of a trx
 *          The passed tuple should be valid.
 *
 *********************************************************************/

template<class T>
w_rc_t table_man_t<T>::delete_tuple(
                                 table_row_t* ptuple,
                                 const lock_mode_t /* lock_mode */,
                                 const PageID& /* primary_root */)
{
    assert (_ptable);
    assert (ptuple);
    assert (ptuple->_rep);

    // if (!ptuple->is_rid_valid()) return RC(se_NO_CURRENT_TUPLE);

    // uint32_t system_mode = _ptable->get_pd();

    // figure out what mode will be used
    // bool bIgnoreLocks = false;
    // if (lock_mode==NL) bIgnoreLocks = true;

    // delete all the corresponding index entries
    std::vector<index_desc_t*>& indexes = _ptable->get_indexes();
    for (size_t i = 0; i < indexes.size(); i++) {
        size_t ksz = ptuple->_rep->_bufsz;
        ptuple->store_key(ptuple->_rep->_dest, ksz, indexes[i]);

        // TODO BUG??
        // This is deleting the whole entry in the secondary index instead
        // of just the pointer to the given tuple. If a key value points to
        // multiple tuples, all tuples will be deleted instead of just one!

        w_keystr_t kstr;
        kstr.construct_regularkey(ptuple->_rep->_dest, ksz);
        W_DO(btree_m::remove(indexes[i]->stid(), kstr));
    }

    size_t ksz = ptuple->_rep_key->_bufsz;
    ptuple->store_key(ptuple->_rep_key->_dest, ksz, _ptable->primary_idx());
    w_keystr_t kstr;
    kstr.construct_regularkey(ptuple->_rep_key->_dest, ksz);
    W_DO(btree_m::remove(_ptable->primary_idx()->stid(), kstr));

    // invalidate tuple
    // ptuple->set_rid(rid_t::null);
    return (RCOK);
}




/*********************************************************************
 *
 *  @fn:    delete_index_entry
 *
 *  @brief: Deletes a tuple's entry from the given index
 *
 *  @note:  This function should be called in the context of a trx
 *          The passed tuple should be valid.
 *
 *********************************************************************/

template<class T>
w_rc_t table_man_t<T>::delete_index_entry(
				       const char* idx_name,
				       table_row_t* ptuple,
				       const lock_mode_t /* lock_mode */,
				       const PageID& /* primary_root */)
{
    assert (_ptable);
    assert (ptuple);
    assert (ptuple->_rep);

    index_desc_t* pindex = _ptable->find_index(idx_name);
    assert (pindex);

    // if (!ptuple->is_rid_valid()) return RC(se_NO_CURRENT_TUPLE);

    // uint32_t system_mode = _ptable->get_pd();
    // rid_t todelete = ptuple->rid();

    // figure out what mode will be used
    // bool bIgnoreLocks = false;
    // if (lock_mode==NL) bIgnoreLocks = true;

    // delete the index entry
    size_t ksz = ptuple->_rep->_bufsz;
    ptuple->store_key(ptuple->_rep->_dest, ksz, pindex);
    assert (ptuple->_rep->_dest); // if NULL invalid key

    w_keystr_t kstr;
    kstr.construct_regularkey(ptuple->_rep->_dest, ksz);
    W_DO(btree_m::remove(pindex->stid(), kstr));

    return (RCOK);
}




/*********************************************************************
 *
 *  @fn:    update_tuple
 *
 *  @brief: Updates a tuple from a table, using direct access through
 *          its RID
 *
 *  @note:  This function should be called in the context of a trx.
 *          The passed tuple rid() should be valid.
 *          There is no need of updating the indexes. That's why there
 *          is not parameter to primary_root.
 *
 *  !!! In order to update a field included by an index !!!
 *  !!! the tuple should be deleted and inserted again  !!!
 *
 *********************************************************************/

template<class T>
w_rc_t table_man_t<T>::update_tuple(
                                 table_row_t* ptuple,
                                 const lock_mode_t  /* lock_mode */) // physical_design_t
{
    // CS TODO -- calling overwrite directly, which only works if updated
    // tuple did not grow (shrinking should be ok).
    // Also assuming that:
    // 1) key fields didn't change
    // 2) key is serialized in _rep_key
    //
    // CS TODO (performance) -- most xcts call this after an index probe,
    // where all this format/load was already done. So there is a lot of
    // repeated work.
    size_t ksz = ptuple->_rep_key->_bufsz;
    ptuple->store_key(ptuple->_rep_key->_dest, ksz, table()->primary_idx());

    size_t elen = ptuple->_rep->_bufsz;
    ptuple->store_value(ptuple->_rep->_dest, elen, table()->primary_idx());

    w_keystr_t kstr;
    kstr.construct_regularkey(ptuple->_rep_key->_dest, ksz);
    W_DO(btree_m::overwrite(table()->primary_idx()->stid(),
                kstr, ptuple->_rep->_dest, 0, elen));

    return RCOK;


    // assert (_ptable);
    // assert (ptuple);
    // assert (ptuple->_rep);

    // if (!ptuple->is_rid_valid()) return RC(se_NO_CURRENT_TUPLE);

    // uint32_t system_mode = _ptable->get_pd();
    // bool bIgnoreLocks = false;
    // if (lock_mode==NL) bIgnoreLocks = true;

    // bool no_heap_latch = false;
    // latch_mode_t heap_latch_mode = LATCH_EX;

    // CS TODO
    // pin record
    // pin_i pin;
    // W_DO(pin.pin(ptuple->rid(), 0, lock_mode, heap_latch_mode));
    // int current_size = pin.body_size();


    // // update record
    // int tsz = format(ptuple, *ptuple->_rep);
    // assert (ptuple->_rep->_dest); // if NULL invalid

    // // a. if updated record cannot fit in the previous spot
    // w_rc_t rc;
    // if (current_size < tsz) {
    //     zvec_t azv(tsz - current_size);

    //     if (no_heap_latch) {
    //         rc = pin.append_mrbt_rec(azv,heap_latch_mode);
    //     }
    //     else {
    //         rc = pin.append_rec(azv);
    //     }

    //     // on error unpin
    //     if (rc.is_error()) {
    //         TRACE( TRACE_DEBUG, "Error updating (by append) record\n");
    //         pin.unpin();
    //     }
    //     W_DO(rc);
    // }


    // // b. else, simply update
    // if (no_heap_latch) {
    //     rc = pin.update_mrbt_rec(0, vec_t(ptuple->_rep->_dest, tsz),
    //                              bIgnoreLocks, true);
    // } else {
    //     rc = pin.update_rec(0, vec_t(ptuple->_rep->_dest, tsz), bIgnoreLocks);
    // }

    // if (rc.is_error()) TRACE( TRACE_DEBUG, "Error updating record\n");

    // // 3. unpin
    // pin.unpin();
    // return (rc);
}

template<class T>
w_rc_t table_man_t<T>::print_table(ostream& os, int num_lines)
{
    table_row_t* row = get_tuple();
    rep_row_t rep(ts());
    rep_row_t repkey(ts());
    rep.set(table()->maxsize());
    repkey.set(table()->maxsize());
    row->_rep = &rep;
    row->_rep_key = &repkey;

    table_scan_iter_impl<T> scanner(this);
    scanner.open_scan();
    bool eof = false;
    size_t i = 0;

    while (true) {
        scanner.next(eof, *row);
        if (eof) break;
        if (num_lines > 0 && i++ > num_lines) break;
        row->print_values(os);
    }

    give_tuple(row);

    return RCOK;
}

template<class T>
w_rc_t table_man_t<T>::print_index(unsigned ind, ostream& os,
        int num_lines, bool need_tuple)
{
    table_row_t* row = get_tuple();
    rep_row_t rep(ts());
    rep_row_t repkey(ts());
    rep.set(table()->maxsize());
    repkey.set(table()->maxsize());
    row->_rep = &rep;
    row->_rep_key = &repkey;

    index_desc_t* pindex = table()->get_indexes()[ind];
    w_assert0(pindex);
    index_scan_iter_impl<T> scanner(pindex, this, need_tuple);
    scanner.open_scan();
    bool eof = false;
    size_t i = 0;

    while (true) {
        scanner.next(eof, *row);
        if (eof) break;
        if (num_lines > 0 && i++ > num_lines) break;
        row->print_values(os);
    }

    give_tuple(row);

    return RCOK;
}


/* ---------------- */
/* --- caching  --- */
/* ---------------- */


/*********************************************************************
 *
 *  @fn:    fetch_table
 *
 *  @brief: Fetch all the pages of the table and its indexes to buffer pool
 *
 *  @note:  This function should be called in the context of a trx.
 *
 *********************************************************************/

template<class T>
w_rc_t table_man_t<T>::fetch_table(lock_mode_t /* alm */)
{
    assert (_ptable);

    bool eof = false;
    int counter = -1;

    table_row_t* tuple = get_tuple();
    rep_row_t areprow(ts());
    rep_row_t areprow_key(ts());
    areprow.set(_ptable->maxsize());
    areprow_key.set(_ptable->maxsize());
    tuple->_rep = &areprow;
    tuple->_rep_key = &areprow_key;

    xct_t::begin();

    // 1. scan the table
    table_scan_iter_impl<T> t_scan(this);
    while(!eof) {
	W_DO(t_scan.next(eof, *tuple));
	counter++;
    }
    TRACE( TRACE_ALWAYS, "%s:%d pages\n", _ptable->name(), counter);

    // 2. scan the indexes
    for (auto index : _ptable->get_indexes()) {
        index_scan_iter_impl<T> i_scan(index, this);
        eof = false;
        counter = -1;
        while(!eof) {
            W_DO(i_scan.next(eof, *tuple));
            counter++;
        }
        TRACE( TRACE_ALWAYS, "\t%s:%d pages\n", index->name().c_str(), counter);
    }

    W_DO(xct_t::commit());
    give_tuple(tuple);

    return RCOK;
}

#if 0 // CS -- TODO migrate to other file

/* ------------------- */
/* --- db fetcher  --- */
/* ------------------- */


table_fetcher_t::table_fetcher_t(ShoreEnv* env)
    : thread_t("DB_FETCHER"), _env(env)
{
}

table_fetcher_t::~table_fetcher_t()
{
}

void table_fetcher_t::work()
{
    assert(_env);
    w_rc_t e = _env->db_fetch();
    if(e.is_error()) {
	cerr << "Error while fetching db!" << endl << e << endl;
    }
}




/* ------------------- */
/* --- db printer  --- */
/* ------------------- */


table_printer_t::table_printer_t(ShoreEnv* env, int lines)
    : thread_t("DB_PRINTER"),
      _env(env), _lines(lines)
{
}

table_printer_t::~table_printer_t()
{
}

void table_printer_t::work()
{
    assert(_env);
    w_rc_t e = _env->db_print(_lines);
    if(e.is_error()) {
        cerr << "Error while printing db!" << endl << e << endl;
    }
}







#include <sstream>
char const* db_pretty_print(table_desc_t const* ptdesc, int /* i=0 */, char const* /* s=0 */)
{
    static char data[1024];
    std::stringstream inout(data, stringstream::in | stringstream::out);
    //std::strstream inout(data, sizeof(data));
    ((table_desc_t*)ptdesc)->print_desc(inout);
    inout << std::ends;
    return data;
}

#endif // 0


