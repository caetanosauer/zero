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

/** @file:   shore_table_man.h
 *
 *  @brief:  Base classes for tables and iterators over tables and indexes
 *           stored in Shore.
 *
 *  @note:   table_man_impl       - class for table-related operations
 *           table_scan_iter_impl - table scanner
 *           index_scan_iter_impl - index scanner
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

/* shore_table_man.h contains the template-based base classes (table_man_impl) 
 * for operations on tables stored in Shore. It also contains the template-based
 * classes (table_scan_iter_impl) and (index_scan_iter_impl) for iteration 
 * operations.
 *
 *
 * FUNCTIONALITY 
 *
 * Operations on single tuples, including adding, updating, and index probe are
 * provided as well, as part of either the (table_man_impl) class.
 */

#ifndef __SHORE_TABLE_MANAGER_H
#define __SHORE_TABLE_MANAGER_H


#include "sm_vas.h"
#include "util.h"

#ifdef CFG_SHORE_6
#include "block_alloc.h"
#else
#include "atomic_trash_stack.h"
#endif

#include "shore_table.h"
#include "shore_row_cache.h"


ENTER_NAMESPACE(shore);



#define DECLARE_TABLE_SCHEMA(tablename)         \
    class tablename : public table_desc_t {     \
    public: tablename(string sysname); }


#define DECLARE_TABLE_SCHEMA_PD(tablename)              \
    class tablename : public table_desc_t {             \
    public: tablename(const uint4_t& pd); }


/* ---------------------------------------------------------------
 *
 * @brief: Forward declarations
 *
 * --------------------------------------------------------------- */

template <class TableDesc>
class table_scan_iter_impl;

template <class TableDesc>
class index_scan_iter_impl;



/* ---------------------------------------------------------------
 *
 * @class: table_man_impl
 *
 * @brief: Template-based class that operates on a Shore table. 
 *
 * --------------------------------------------------------------- */

template <class TableDesc>
class table_man_impl : public table_man_t
{
public:
    typedef table_scan_iter_impl<TableDesc> table_iter;
    typedef index_scan_iter_impl<TableDesc> index_iter;
    typedef row_cache_t<TableDesc> row_cache;

protected:
    
    /* Place-holder until we clean up the code

       WARNING: forward decl only... must be specialized manually for
       each instance we create
    */
    struct pcache_link {
	static row_cache* tls_get();
	operator row_cache*() { return tls_get(); }
	row_cache* operator->() { return tls_get(); }
    };


#define _DEFINE_ROW_CACHE_TLS(table_man, tls_name) \
    DECLARE_TLS(table_man::row_cache, tls_name);   \
    template<> table_man::row_cache* table_man::pcache_link::tls_get() { return tls_name; }
#define DEFINE_ROW_CACHE_TLS(ns, name)		\
    _DEFINE_ROW_CACHE_TLS(ns::name##_man_impl, ns##name##_cache)

    TableDesc* _pspecifictable;
    pcache_link _pcache; /* pointer to a tuple cache */
    

public:

    /* ------------------- */
    /* --- Constructor --- */
    /* ------------------- */

    table_man_impl(TableDesc* aTableDesc, 
                   bool construct_cache=true)
        : table_man_t(aTableDesc, construct_cache),
          _pspecifictable(aTableDesc)
    {
        assert (_ptable);
	row_cache::tuple_factory::ptable() = aTableDesc;
    }


    /* ------------------------------------------- */
    /* --- iterators for index and table scans --- */
    /* ------------------------------------------- */

    w_rc_t get_iter_for_file_scan(ss_m* db,
				  table_iter* &iter,
                                  lock_mode_t alm = SH);

    w_rc_t get_iter_for_index_scan(ss_m* db,
				   index_desc_t* pindex,
				   index_iter* &iter,
                                   lock_mode_t alm,
                                   bool need_tuple,
				   scan_index_i::cmp_t c1,
				   const cvec_t & bound1,
				   scan_index_i::cmp_t c2,
				   const cvec_t & bound2);


    /* ------------------------------------------------------- */
    /* --- check consistency between the indexes and table --- */
    /* ------------------------------------------------------- */

    /**  true:  consistent
     *   false: inconsistent */
    w_rc_t check_all_indexes_together(ss_m* db);
    bool   check_all_indexes(ss_m* db);
    w_rc_t check_index(ss_m* db, index_desc_t* pidx);
    w_rc_t scan_all_indexes(ss_m* db);
    w_rc_t scan_index(ss_m* db, index_desc_t* pidx);


    /* ------------------------------ */
    /* --- tuple cache operations --- */
    /* ------------------------------ */

    row_cache* get_cache() { assert (_pcache); return (_pcache); }

    inline table_tuple* get_tuple() 
    {
        return (_pcache->borrow());
    }
    

    inline void give_tuple(table_tuple* ptt) 
    {
        _pcache->giveback(ptt);
    }
    
    
    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    w_rc_t print_table(ss_m* db, int num_lines);

    
}; // EOF: table_man_impl



/* ---------------------------------------------------------------
 *
 * @class: table_scan_iter_impl
 *
 * @brief: Declaration of a table (file) scan iterator
 *
 * --------------------------------------------------------------- */

template <class TableDesc>
class table_scan_iter_impl : 
    public tuple_iter_t<TableDesc, scan_file_i, table_row_t >
{
public:
    typedef table_row_t table_tuple;
    typedef table_man_impl<TableDesc> table_manager;
    typedef tuple_iter_t<TableDesc, scan_file_i, table_row_t > table_iter;

private:

    table_manager* _pmanager;

public:

    /* -------------------- */
    /* --- construction --- */
    /* -------------------- */

    table_scan_iter_impl(ss_m* db, 
                         TableDesc* ptable,
                         table_manager* pmanager,
                         lock_mode_t alm) 
        : table_iter(db, ptable, alm, true), _pmanager(pmanager)
    { 
        assert (_pmanager);
        W_COERCE(open_scan(db));
    }
        
    ~table_scan_iter_impl() { 
        tuple_iter_t<TableDesc, scan_file_i, table_row_t >::close_scan(); 
    }


    /* ------------------------ */
    /* --- fscan operations --- */
    /* ------------------------ */

    w_rc_t open_scan(ss_m* db) {
        if (!table_iter::_opened) {
            assert (db);
            W_DO(table_iter::_file->check_fid(db));
            bool bIgnoreLatches = (table_iter::_file->get_pd() & (PD_MRBT_LEAF | PD_MRBT_PART) ? true : false);
            table_iter::_scan = new scan_file_i(table_iter::_file->fid(), 
                                                ss_m::t_cc_record, 
                                                false, 
                                                table_iter::_lm,
                                                bIgnoreLatches);
            table_iter::_opened = true;
        }
        return (RCOK);
    }


    pin_i* cursor() {
        pin_i *rval;
        bool eof;
        table_iter::_scan->cursor(rval, eof);
        return (eof? NULL : rval);
    }


    w_rc_t next(ss_m* db, bool& eof, table_tuple& tuple) {
        assert (_pmanager);
        if (!table_iter::_opened) open_scan(db);
        pin_i* handle;
        W_DO(table_iter::_scan->next(handle, 0, eof));
        if (!eof) {
            if (!_pmanager->load(&tuple, handle->body()))
                return RC(se_WRONG_DISK_DATA);
            tuple.set_rid(handle->rid());
        }
        return (RCOK);
    }

}; // EOF: table_scan_iter_impl



/* ---------------------------------------------------------------------
 *
 * @class: index_scan_iter_impl
 *
 * @brief: Declaration of a index scan iterator
 *
 * --------------------------------------------------------------------- */


template <class TableDesc>
class index_scan_iter_impl : 
    public tuple_iter_t<index_desc_t, scan_index_i, table_row_t >
{
public:
    typedef table_row_t table_tuple;
    typedef table_man_impl<TableDesc> table_manager;
    typedef tuple_iter_t<index_desc_t, scan_index_i, table_row_t > index_iter;

private:
    table_manager* _pmanager;
    bool           _need_tuple;

public:

    /* -------------------- */
    /* --- construction --- */
    /* -------------------- */
        
    index_scan_iter_impl(ss_m* db,
                         index_desc_t* pindex,
                         table_manager* pmanager,
                         lock_mode_t alm,    // alm = SH
                         bool need_tuple)    // need_tuple = false
        : index_iter(db, pindex, alm, true), 
          _pmanager(pmanager), _need_tuple(need_tuple)
    { 
        assert (_pmanager);
        /** @note: We need to know the bounds of the iscan before
         *         opening the iterator. That's why we cannot open
         *         the iterator upon construction.
         *         Needs explicit call to open_scan(...)
         */        
    }

    ~index_scan_iter_impl() { 
        index_iter::close_scan(); 
    };


    /* ------------------------ */        
    /* --- iscan operations --- */
    /* ------------------------ */

    w_rc_t open_scan(ss_m* db, int pnum,
                     scan_index_i::cmp_t c1, const cvec_t& bound1,
                     scan_index_i::cmp_t c2, const cvec_t& bound2)
    {                    
        if (!index_iter::_opened) {

            // 1. figure out what concurrency will be used
            // !! according to shore-mt/src/scan.h:82 
            //    t_cc_kvl  - IS lock on the index and SH key-value locks on every entry encountered
            //    t_cc_none - IS lock on the index and no other locks
            ss_m::concurrency_t cc = ss_m::t_cc_im;
            if (index_iter::_lm==NL) cc = ss_m::t_cc_none;

            // 2. open the cursor
            W_DO(index_iter::_file->check_fid(db));
            index_iter::_scan = new scan_index_i(index_iter::_file->fid(pnum), 
                                                 c1, bound1, c2, bound2,
                                                 false, cc, 
                                                 index_iter::_lm,
                                                 index_iter::_file->is_latchless());
            index_iter::_opened = true;
        }

        return (RCOK);
    }

    w_rc_t next(ss_m* /* db */, bool& eof, table_tuple& tuple) 
    {
        assert (index_iter::_opened);
        assert (_pmanager);
        assert (tuple._rep);

        W_DO(index_iter::_scan->next(eof));

        if (!eof) {
            int key_sz = _pmanager->format_key(index_iter::_file, 
                                               &tuple, *tuple._rep);
            assert (tuple._rep->_dest); // if dest == NULL there is an invalid key

            vec_t    key(tuple._rep->_dest, key_sz);

            rid_t    rid;
            vec_t    record(&rid, sizeof(rid_t));
            smsize_t klen = 0;
            smsize_t elen = sizeof(rid_t);

            W_DO(index_iter::_scan->curr(&key, klen, &record, elen));
            tuple.set_rid(rid);
            
            _pmanager->load_key((const char*)key.ptr(0), 
                                index_iter::_file, &tuple);
            //tuple.load_key(key.ptr(0), _file);

            if (_need_tuple) {
                pin_i  pin;
                W_DO(pin.pin(rid, 0, index_iter::_lm, index_iter::_file->is_latchless()));
                if (!_pmanager->load(&tuple, pin.body())) {
                    pin.unpin();
                    return RC(se_WRONG_DISK_DATA);
                }
                pin.unpin();
            }
        }    
        return (RCOK);
    }

}; // EOF: index_scan_iter_impl




/* ------------------------------------------- */
/* --- iterators for index and table scans --- */
/* ------------------------------------------- */


/********************************************************************* 
 *
 *  @fn:    get_iter_for_scan (table/index)
 *  
 *  @brief: Returns and opens an (table/index) scan iterator.
 *
 *  @note:  If it fails to open the iterator it retuns an error. 
 *
 *********************************************************************/

template <class TableDesc>
w_rc_t table_man_impl<TableDesc>::get_iter_for_file_scan(ss_m* db,
                                                         table_iter* &iter,
                                                         lock_mode_t alm)
{
    assert (_ptable);
    iter = new table_scan_iter_impl<TableDesc>(db, _pspecifictable, this, alm);
    if (iter->opened()) return (RCOK);
    return RC(se_OPEN_SCAN_ERROR);
}


template <class TableDesc>
w_rc_t table_man_impl<TableDesc>::get_iter_for_index_scan(ss_m* db,
                                                          index_desc_t* index,
                                                          index_iter* &iter,
                                                          lock_mode_t alm,
                                                          bool need_tuple,
                                                          scan_index_i::cmp_t c1,
                                                          const cvec_t& bound1,
                                                          scan_index_i::cmp_t c2,
                                                          const cvec_t& bound2)
{
    assert (_ptable);
    int pnum = 0;
    if(index->is_partitioned()) {
	int key0 = 0;
	int cnt = bound1.copy_to(&key0, sizeof(int));
	assert(cnt == sizeof(int));
	int other_key0;
	cnt = bound2.copy_to(&other_key0, sizeof(int));
	assert(cnt == sizeof(int));
	assert(key0 == other_key0);
	pnum = key0 % index->get_partition_count();
    }
    iter = new index_scan_iter_impl<TableDesc>(db, index, this, alm, need_tuple);
    W_DO(iter->open_scan(db, pnum, c1, bound1, c2, bound2));
    if (iter->opened())  return (RCOK);
    return RC(se_OPEN_SCAN_ERROR);
}



#define CHECK_FOR_DEADLOCK(action, on_deadlock)				\
    do {								\
	w_rc_t rc = action;						\
	if(rc.is_error()) {						\
	    W_COERCE(db->abort_xct());					\
	    if(rc.err_num() == smlevel_0::eDEADLOCK) {			\
		TRACE( TRACE_ALWAYS, "load(%s): %d: deadlock detected. Retrying.n", _ptable->name(), tuple_count); \
		W_DO(db->begin_xct());					\
		on_deadlock;						\
	    }								\
	    W_DO(rc);							\
	}								\
	else {								\
	    break;							\
	}								\
    } while(1) 

struct table_creation_lock {
    static pthread_mutex_t* get_lock() {
	static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
	return &lock;
    }
};



EXIT_NAMESPACE(shore);
#include "shore_helper_loader.h"
ENTER_NAMESPACE(shore);


/******************************************************************** 
 *
 *  @fn:    check_all_indexes_together
 *
 *  @brief: Check all indexes with a single file scan
 *
 *  @note:  Can be used for warm-up for memory-fitting databases
 *
 ********************************************************************/

template <class TableDesc>
w_rc_t table_man_impl<TableDesc>::check_all_indexes_together(ss_m* db)
{
    assert (_ptable);

    TRACE( TRACE_DEBUG, "Checking consistency of the indexes on table (%s)\n",
           _ptable->name());

    time_t tstart = time(NULL);
    
    W_DO(db->begin_xct());
    index_desc_t* pindex = NULL;

    // get a table iterator
    table_iter* iter;
    W_DO(get_iter_for_file_scan(db, iter));

    // scan the entire file    
    bool eof = false;
    table_tuple tuple(_ptable);
    W_DO(iter->next(db, eof, tuple));

    int ituple_cnt=0;
    int idx_cnt=0;

    while (!eof) {
        // remember the rid just scanned
        rid_t tablerid = tuple.rid();

        ituple_cnt++;

        // probe all indexes
        pindex = _ptable->indexes();
        while (pindex) {

            idx_cnt++;

            w_rc_t rc = index_probe(db, pindex, &tuple);

            if (rc.is_error()) {
                TRACE( TRACE_ALWAYS, "Index probe error in (%s) (%s) (%d)\n", 
                       _ptable->name(), pindex->name(), idx_cnt);
                cerr << "Due to " << rc << endl;
                return RC(se_INCONSISTENT_INDEX);
            }            

            if (tablerid != tuple.rid()) {
                TRACE( TRACE_ALWAYS, "Inconsistent index... (%d)",
                       idx_cnt);
                return RC(se_INCONSISTENT_INDEX);
            }
            pindex = pindex->next();
        }

	W_DO(iter->next(db, eof, tuple));
    }
    delete (iter);

    W_DO(db->commit_xct());
    time_t tstop = time(NULL);

    TRACE( TRACE_DEBUG, "Indexes on table (%s) found consistent in (%d) secs...\n",
           _ptable->name(), tstop-tstart);
    
    return (RCOK);

}


/******************************************************************** 
 *
 *  @fn:    check_all_indexes
 *
 *  @brief: Check all indexes
 *
 ********************************************************************/

template <class TableDesc>
bool table_man_impl<TableDesc>::check_all_indexes(ss_m* db)
{
    assert (_ptable);

    index_desc_t* pindex = _ptable->indexes();

    TRACE( TRACE_DEBUG, "Checking consistency of the indexes on table (%s)\n",
           _ptable->name());

    while (pindex) {
	w_rc_t rc = check_index(db, pindex);
	if (rc.is_error()) {
            TRACE( TRACE_ALWAYS, "Index checking error in (%s) (%s)\n", 
                   _ptable->name(), pindex->name());
	    cerr << "Due to " << rc << endl;
	    return (false);
	}
	pindex = pindex->next();
    }
    return (true);
}


/********************************************************************* 
 *
 *  @fn:    check_index
 *
 *  @brief: Checks all the values on an index. It first gets the rid from
 *          the table (by scanning) and then probes the index for the same
 *          tuple. It reports error if the two rids do not match.
 *
 *********************************************************************/

template <class TableDesc>
w_rc_t table_man_impl<TableDesc>::check_index(ss_m* db,
                                              index_desc_t* pindex)
{
    assert (_ptable);

    TRACE( TRACE_DEBUG, "Start to check index (%s)\n", pindex->name());

    W_DO(db->begin_xct());

    table_iter* iter;
    W_DO(get_iter_for_file_scan(db, iter));

    bool eof = false;
    table_tuple tuple(_ptable);
    W_DO(iter->next(db, eof, tuple));
    uint_t tcount=0;
    while (!eof) {
        // remember the rid just scanned
        rid_t tablerid = tuple.rid();
	W_DO(index_probe(db, pindex, &tuple));
        ++tcount;
	if (tablerid != tuple.rid()) {
            TRACE( TRACE_ALWAYS, "Inconsistent index... (%d)",
                   tcount);
            return RC(se_INCONSISTENT_INDEX);
	}
	W_DO(iter->next(db, eof, tuple));
    }
    delete (iter);

    W_DO(db->commit_xct());
    return (RCOK);
}



/* ------------------ */
/* --- scan index --- */
/* ------------------ */


/********************************************************************* 
 *
 *  @fn:    scan_all_indexes
 *
 *  @brief: Scan all indexes
 *
 *********************************************************************/

template <class TableDesc>
w_rc_t table_man_impl<TableDesc>::scan_all_indexes(ss_m* db)
{
    assert (_ptable);

    index_desc_t* pindex = _ptable->indexes();
    while (pindex) {
	W_DO(scan_index(db, pindex));
	pindex = pindex->next();
    }
    return (RCOK);
}


/********************************************************************* 
 *
 *  @fn:    scan_index
 *
 *  @brief: Iterates over all the values on an index
 *
 *********************************************************************/

template <class TableDesc>
w_rc_t table_man_impl<TableDesc>::scan_index(ss_m* db, 
                                             index_desc_t* pindex)
{
    assert (_ptable);
    assert (pindex);
    assert (!pindex->is_partitioned());

    TRACE( TRACE_DEBUG, "Scanning index (%s) for table (%s)\n", 
           pindex->name(), _ptable->name());

    /* 1. open a index scanner */
    index_iter* iter;

    table_tuple lowtuple(_ptable);
    rep_row_t lowrep(_pts);
    int lowsz = min_key(pindex, &lowtuple, lowrep);
    assert (lowrep._dest);

    table_tuple hightuple(_ptable);
    rep_row_t highrep(_pts);
    int highsz = max_key(pindex, &hightuple, highrep);
    assert (highrep._dest);

    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 SH, 
                                 false,
				 scan_index_i::ge,
				 vec_t(lowrep._dest, lowsz),
				 scan_index_i::le,
				 vec_t(highrep._dest, highsz)));

    /* 2. iterate over all index records */
    bool        eof;
    int         count = 0;    
    table_tuple row(_ptable);

    W_DO(iter->next(db, eof, row));
    while (!eof) {	
	pin_i  pin;
	W_DO(pin.pin(row.rid(), 0));
	if (!load(&row, pin.body())) {
            pin.unpin();
            return RC(se_WRONG_DISK_DATA);
        }
	pin.unpin();
        row.print_values();

	W_DO(iter->next(db, eof, row));
	count++;
    }
    delete iter;

    /* 3. print out some statistics */
    TRACE( TRACE_DEBUG, "%d tuples found!\n", count);
    TRACE( TRACE_DEBUG, "Scan finished!\n");

    return (RCOK);
}


/* ----------------- */
/* --- debugging --- */
/* ----------------- */

/********************************************************************* 
 *
 *  @fn:    print_table
 *
 *  @brief: prints the table to a file
 *          the user can specify the number of lines written to a file
 *          with "num_lines" argument if s/he wants to write the contents
 *          to multiple files for parallel loading
 *          if this argument is passed as 0 or something smaller than that
 *          then all the table is going to be written to a single file
 *
 *********************************************************************/

template <class TableDesc>
w_rc_t table_man_impl<TableDesc>::print_table(ss_m* db, int num_lines)
{
    assert (_ptable);

    char   filename[MAX_FILENAME_LEN];
    strcpy(filename, _ptable->name());
    if(num_lines > 0) {
	strcat(filename, c_str("_%d",0));
    }
    strcat(filename, ".tbl");
    ofstream fout(filename);

    W_DO(db->begin_xct());

    table_iter* iter;
    int cardinality = 0;
    int lines = 0;
    int num_files = 1;
    W_DO(get_iter_for_file_scan(db, iter));

    bool eof = false;
    table_tuple row(_ptable);
    W_DO(iter->next(db, eof, row));
    while (!eof) {
	if(num_lines != 0 && lines == num_lines) {
	    fout.flush();
	    fout.close();
	    TRACE( TRACE_ALWAYS, "%s closed!\n", filename);
	    char* pos = strrchr(filename, '_');
	    pos++;
	    strcpy(pos, c_str("%d.tbl",num_files));
	    fout.open(filename);
	    num_files++;
	    lines = 0;
	}
	row.print_values(fout);
        // row.print_tuple();
        cardinality++;
	lines++;
	W_DO(iter->next(db, eof, row));
    }
    delete iter;

    W_DO(db->commit_xct());

    TRACE( TRACE_ALWAYS, "%s closed!\n", filename);

    //fout << "Table : " << _ptable->name() << endl;
    //fout << "Tuples: " << cardinality << endl;

    TRACE( TRACE_ALWAYS, "Table (%s) printed (%d) tuples\n",
           _ptable->name(), cardinality);

    fout.flush();
    fout.close();
    
    return (RCOK);
}





EXIT_NAMESPACE(shore);

#endif /* __SHORE_TABLE_MANAGER_H */
