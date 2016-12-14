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

/** @file:   shore_table.h
 *
 *  @brief:  Base class for tables stored in Shore
 *
 *  @note:   table_desc_t - table abstraction
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */


/* shore_table.h contains the base class (table_desc_t) for tables stored in
 * Shore. Each table consists of several parts:
 *
 * 1. An array of field_desc, which contains the decription of the
 *    fields.  The number of fields is set by the constructor. The schema
 *    of the table is not written to the disk.
 *
 * 2. The primary index of the table.
 *
 * 3. Secondary indices on the table.  All the secondary indices created
 *    on the table are stored as a linked list.
 *
 *
 * FUNCTIONALITY
 *
 * There are methods in (table_desc_t) for creating, the table
 * and indexes.
 *
 *
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * @note  Modifications to the schema need rebuilding the whole
 *        database.
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 *
 *
 * USAGE:
 *
 * To create a new table, create a class for the table by inheriting
 * publicly from class tuple_desc_t to take advantage of all the
 * built-in tools. The schema of the table should be set at the
 * constructor of the table.  (See shore_tpcc_schema.h for examples.)
 *
 *
 * NOTE:
 *
 * Due to limitation of Shore implementation, only the last field
 * in indexes can be variable length.
 *
 *
 * BUGS:
 *
 * If a new index is created on an existing table, explicit call to
 * load the index is needed.
 *
 * Timestamp field is not fully implemented: no set function.
 *
 *
 * EXTENSIONS:
 *
 * The mapping between SQL types and C++ types are defined in
 * (field_desc_t).  Modify the class to support more SQL types or
 * change the mapping.  The NUMERIC type is currently stored as string;
 * no further understanding is provided yet.
 *
 */

#ifndef __TABLE_MAN_H
#define __TABLE_MAN_H

#include "tls.h"
#include "sm_vas.h"
#include "latches.h"
#include "btcursor.h"

//#include "shore_msg.h"
#include "util/guard.h"

#include "table_desc.h"
#include "field.h"
#include "index_desc.h"
#include "row.h"
#include "row_cache.h"

/* ---------------------------------------------------------------
 *
 * @class: table_man_t
 *
 * @brief: Base class for operations on a Shore table.
 *
 * --------------------------------------------------------------- */

template<class T> // T is a table_desc_t implementation
class table_man_t
{
protected:

    T* _ptable;       /* pointer back to the table description */

    guard<blob_pool> _pts;   /* trash stack */

public:

    table_man_t(T* aTableDesc,
		bool construct_cache=true)
        : _ptable(aTableDesc)
    {
	// init tuple cache
        if (construct_cache) {
            // init trash stack
            _pts = new blob_pool(_ptable->maxsize());
            // CS TODO: implement new row cache!
            row_cache_t<T>::tuple_factory::ptable() = aTableDesc;
        }
    }

    virtual ~table_man_t() {}

    T* table() { return (_ptable); }

    // loads store id values in fid field for this table and its indexes
    w_rc_t load_and_register_fid();

    /* ------------------------------ */
    /* --- trash stack operations --- */
    /* ------------------------------ */

    blob_pool* ts() { assert (_pts); return (_pts); }


    /* ---------------------------- */
    /* --- access through index --- */
    /* ---------------------------- */

    // idx probe
    w_rc_t index_probe(
                       index_desc_t* pidx,
                       table_row_t*  ptuple,
                       const lock_mode_t lock_mode = okvl_mode::S,     /* One of: N, S, X */
                       const PageID& root = 0);   /* Start of the search */

    // probe idx in X (& LATCH_EX) mode
    inline w_rc_t   index_probe_forupdate(
                                          index_desc_t* pidx,
                                          table_row_t*  ptuple,
                                          const PageID& root = 0)
    {
        return (index_probe(pidx, ptuple, okvl_mode::X, root));
    }

    // probe idx in N (& LATCH_SH) mode
    inline w_rc_t   index_probe_nl(
                                   index_desc_t* pidx,
                                   table_row_t*  ptuple,
                                   const PageID& root = 0)
    {
        return (index_probe(pidx, ptuple, okvl_mode::N, root));
    }

    // probe primary idx
    inline w_rc_t   index_probe_primary(
                                        table_row_t* ptuple,
                                        lock_mode_t  lock_mode = okvl_mode::S,
                                        const PageID& root = 0)
    {
        assert (_ptable && _ptable->primary_idx());
        return (index_probe(_ptable->primary_idx(), ptuple, lock_mode, root));
    }

    // idx probe - based on idx name //
    inline w_rc_t   index_probe_by_name(
                                        const char*  idx_name,
                                        table_row_t* ptuple,
                                        lock_mode_t  lock_mode = okvl_mode::S,
                                        const PageID& root = 0)
    {
        index_desc_t* pindex = _ptable->find_index(idx_name);
        return (index_probe(pindex, ptuple, lock_mode, root));
    }

    // probe idx in EX (& LATCH_EX) mode - based on idx name //
    inline w_rc_t   index_probe_forupdate_by_name(
                                                  const char* idx_name,
                                                  table_row_t* ptuple,
                                                  const PageID& root = 0)
    {
	index_desc_t* pindex = _ptable->find_index(idx_name);
        w_assert0(pindex);
	return (index_probe_forupdate(pindex, ptuple, root));
    }

    // probe idx in N (& LATCH_NL) mode - based on idx name //
    inline w_rc_t   index_probe_nl_by_name(
                                           const char* idx_name,
                                           table_row_t* ptuple,
                                           const PageID& root = 0)
    {
	index_desc_t* pindex = _ptable->find_index(idx_name);
	return (index_probe_nl(pindex, ptuple, root));
    }


    /* -------------------------- */
    /* --- tuple manipulation --- */
    /* -------------------------- */

    w_rc_t    add_tuple(
                        table_row_t*  ptuple,
                        const lock_mode_t   lock_mode = okvl_mode::X,
                        const PageID& primary_root = 0);

    w_rc_t    add_index_entry(
			      const char* idx_name,
			      table_row_t* ptuple,
			      const lock_mode_t lock_mode = okvl_mode::X,
			      const PageID& primary_root = 0);

    w_rc_t    delete_tuple(
                           table_row_t* ptuple,
                           const lock_mode_t lock_mode = okvl_mode::X,
                           const PageID& primary_root = 0);

    w_rc_t    delete_index_entry(
				 const char* idx_name,
				 table_row_t* ptuple,
				 const lock_mode_t lock_mode = okvl_mode::X,
				 const PageID& primary_root = 0);

    w_rc_t    update_tuple(
                           table_row_t* ptuple,
                           const lock_mode_t lock_mode = okvl_mode::X);


    // set indexed fields of the row to minimum
    int  min_key(index_desc_t* pindex,
                 table_row_t* ptuple,
                 rep_row_t &arep);

    // set indexed fields of the row to maximum
    int  max_key(index_desc_t* pindex,
                 table_row_t* ptuple,
                 rep_row_t &arep);

    // length of the formatted key
    int  key_size(index_desc_t* pindex) const;





    /* ------------------------------------------------------- */
    /* --- check consistency between the indexes and table --- */
    /* ------------------------------------------------------- */

    // CS TODO
    // virtual w_rc_t check_all_indexes_together(ss_m* db)=0;
    // virtual bool   check_all_indexes(ss_m* db)=0;
    // virtual w_rc_t check_index(ss_m* db, index_desc_t* pidx)=0;
    // virtual w_rc_t scan_all_indexes(ss_m* db)=0;
    // virtual w_rc_t scan_index(ss_m* db, index_desc_t* pidx)=0;


    /* -------------------------------- */
    /* - population related if needed - */
    /* -------------------------------- */
    virtual w_rc_t populate(bool& /* hasNext */)
    {
        return (RCOK);
    }


    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    /*
     * print the table on screen or to files
     */
    virtual w_rc_t print_table(ostream& os, int num_lines = 0);
    virtual w_rc_t print_index(unsigned i, ostream& os,
            int num_lines = 0, bool need_tuple = false);


    /* --------------- */
    /* --- caching --- */
    /* --------------- */

    /* fetch the pages of the table and its indexes to buffer pool */
    virtual w_rc_t fetch_table(lock_mode_t alm = okvl_mode::S);

// Row cache
protected:

    /* Place-holder until we clean up the code

       WARNING: forward decl only... must be specialized manually for
       each instance we create
    */
    struct pcache_link {
	static row_cache_t<T>* tls_get();
	operator row_cache_t<T>*() { return tls_get(); }
	row_cache_t<T>* operator->() { return tls_get(); }
    };

    pcache_link _pcache; /* pointer to a tuple cache */

public:

    row_cache_t<T>* get_cache() { assert (_pcache); return (_pcache); }

    inline table_row_t* get_tuple()
    {
        return (_pcache->borrow());
    }


    inline void give_tuple(table_row_t* ptt)
    {
        _pcache->giveback(ptt);
    }

}; // EOF: table_man_t

#define DEFINE_ROW_CACHE_TLS(bench, table) \
    DECLARE_TLS(row_cache_t<bench::table##_t>, bench##_##table##_cache); \
    template<> row_cache_t<bench::table##_t>* \
        table_man_t<bench::table##_t>::pcache_link::tls_get() \
            { return bench##_##table##_cache; }


#if 0 // CS: disabled for now -- should be moved to other file anyway

/* ---------------------------------------------------------------
 *
 * @class: table_printer_t
 *
 * @brief: Thread to print the table contents
 *
 * --------------------------------------------------------------- */

class table_printer_t : public thread_t
{
private:

    ShoreEnv* _env;
    int _lines;

public:

    table_printer_t(ShoreEnv* _env, int lines);
    ~table_printer_t();
    void work();

}; // EOF: table_printer_t







/* ---------------------------------------------------------------
 *
 * @class: table_fetcher_t
 *
 * @brief: Thread to fetch the pages of the table and its indexes
 *
 * --------------------------------------------------------------- */

class table_fetcher_t : public thread_t
{
private:

    ShoreEnv* _env;

public:

    table_fetcher_t(ShoreEnv* _env);
    ~table_fetcher_t();
    void work();

}; // EOF: table_fetcher_t
#endif // 0

#endif /* __TABLE_MAN_H */
