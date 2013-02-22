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

#ifndef __SHORE_TABLE_H
#define __SHORE_TABLE_H


#include "util.h"
#include "sm_vas.h"

#include "shore_msg.h"
#include "shore_error.h"
#include "shore_file_desc.h"
#include "shore_tools.h"
#include "shore_field.h"
#include "shore_index.h"
#include "shore_row.h"


ENTER_NAMESPACE(shore);



/* ---------------------------------------------------------------
 *
 * @class: table_desc_t
 *
 * @brief: Description of a Shore table. Gives access to the fields,
 *         and indexes of the table.
 *
 * --------------------------------------------------------------- */

class table_desc_t : public file_desc_t 
{
protected:

    /* ------------------- */
    /* --- table schema -- */
    /* ------------------- */

    ss_m*           _db;                 // the SM
    
    field_desc_t*   _desc;               // schema - set of field descriptors
    
    index_desc_t*   _indexes;            // indexes on the table
    index_desc_t*   _primary_idx;        // pointer to primary idx
  
    volatile uint_t _maxsize;            // max tuple size for this table, shortcut
    
    // Partitioning info (for MRBTrees)
    char*  _sMinKey;
    uint   _sMinKeyLen;
    char*  _sMaxKey;
    uint   _sMaxKeyLen;
    uint  _numParts;

    int find_field_by_name(const char* field_name) const;

public:

    /* ------------------- */
    /* --- Constructor --- */
    /* ------------------- */

    table_desc_t(const char* name, int fieldcnt, uint4_t pd);
    virtual ~table_desc_t();


    /* ---------------------------------------------------- */
    /* --- partitioning information, used with MRBTrees --- */
    /* ---------------------------------------------------- */

    // @note: If the partitioning is set *BEFORE* the indexes have been 
    //        created (which is done at the create_table()) then this
    //        is the partitioning which will be used.
    
    w_rc_t set_partitioning(const char* sMinKey, uint len1, 
                            const char* sMaxKey, uint len2, 
                            uint numParts);

    // @note: Accessing information about the main partitioning scheme
    uint  pcnt() const;
    char* getMinKey() const;
    uint  getMinKeyLen() const;
    char* getMaxKey() const;
    uint  getMaxKeyLen() const;

    w_rc_t get_main_rangemap(key_ranges_map*& prangemap);


    /* ----------------------------------------- */
    /* --- create physical table and indexes --- */
    /* ----------------------------------------- */

    w_rc_t create_physical_table(ss_m* db);

    w_rc_t create_physical_index(ss_m* db, index_desc_t* index);

    w_rc_t create_physical_empty_primary_idx();


    /* ----------------------------------------------------- */
    /* --- create the logical description of the indexes --- */
    /* ----------------------------------------------------- */

    // create an index on the table
    bool   create_index_desc(const char* name,
                             int partitions,
                             const uint* fields,
                             const uint num,
                             const bool unique=true,
                             const bool primary=false,
                             const uint4_t& pd=PD_NORMAL);
    
    bool   create_primary_idx_desc(const char* name,
                                   int partitions,
                                   const uint* fields,
                                   const uint num,
                                   const uint4_t& pd=PD_NORMAL);



    /* ------------------------ */
    /* --- index facilities --- */
    /* ------------------------ */

    index_desc_t* indexes() { return (_indexes); }

    // index by name
    index_desc_t* find_index(const char* index_name) { 
        return (_indexes ? _indexes->find_by_name(index_name) : NULL); 
    }
    
    // # of indexes
    int index_count() { return (_indexes->index_count()); } 

    index_desc_t* primary_idx() { return (_primary_idx); }
    stid_t get_primary_stid();


    /* sets primary index, the index itself should be already set to
     * primary and unique */
    void set_primary(index_desc_t* idx) { 
        assert (idx->is_primary() && idx->is_unique());
        _primary_idx = idx; 
    }

    char* index_keydesc(index_desc_t* idx);
    int   index_maxkeysize(index_desc_t* index) const; /* max index key size */


    /* ---------------------------------------------------------------- */
    /* --- for the conversion between disk format and memory format --- */
    /* ---------------------------------------------------------------- */

    uint_t maxsize(); /* maximum requirement for disk format */

    inline field_desc_t* desc(const uint_t descidx) {
        assert (descidx<_field_count);
        assert (_desc);
        return (&(_desc[descidx]));
    }    

    /* ---------- */
    /* --- db --- */
    /* ---------- */
    ss_m* db() { return (_db); }

    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    void print_desc(ostream & os = cout);  /* print the schema */

}; // EOF: table_desc_t


typedef std::list<table_desc_t*> table_list_t;



/* ---------------------------------------------------------------
 *
 * @class: table_man_t
 *
 * @brief: Base class for operations on a Shore table. 
 *
 * --------------------------------------------------------------- */

class table_man_t
{
protected:

    table_desc_t* _ptable;       /* pointer back to the table description */

    guard<ats_char_t> _pts;   /* trash stack */

public:

    typedef table_row_t table_tuple; 

    table_man_t(table_desc_t* aTableDesc,
		bool construct_cache=true) 
        : _ptable(aTableDesc)
    {
	// init tuple cache
        if (construct_cache) {
            // init trash stack            
            _pts = new ats_char_t(_ptable->maxsize());
        }
    }

    virtual ~table_man_t() {}

    static mcs_lock register_table_lock;
    void register_table_man();
    static std::map<stid_t, table_man_t*> stid_to_tableman;

    table_desc_t* table() { return (_ptable); }

    int get_pnum(index_desc_t* pindex, table_tuple const* ptuple) const;


    /* ------------------------------ */
    /* --- trash stack operations --- */
    /* ------------------------------ */

    ats_char_t* ts() { assert (_pts); return (_pts); }


    /* ---------------------------- */
    /* --- access through index --- */
    /* ---------------------------- */

    // idx probe
    w_rc_t index_probe(ss_m* db,
                       index_desc_t* pidx,
                       table_tuple*  ptuple,
                       const lock_mode_t lock_mode = SH,     /* One of: NL, SH, EX */
                       const lpid_t& root = lpid_t::null);   /* Start of the search */
    
    // probe idx in EX (& LATCH_EX) mode
    inline w_rc_t   index_probe_forupdate(ss_m* db,
                                          index_desc_t* pidx,
                                          table_tuple*  ptuple,
                                          const lpid_t& root = lpid_t::null)
    {
        return (index_probe(db, pidx, ptuple, EX, root));
    }

    // probe idx in NL (& LATCH_SH) mode
    inline w_rc_t   index_probe_nl(ss_m* db,
                                   index_desc_t* pidx,
                                   table_tuple*  ptuple,
                                   const lpid_t& root = lpid_t::null)
    {
        return (index_probe(db, pidx, ptuple, NL, root));
    }

    // probe primary idx
    inline w_rc_t   index_probe_primary(ss_m* db, 
                                        table_tuple* ptuple, 
                                        lock_mode_t  lock_mode = SH,        
                                        const lpid_t& root = lpid_t::null)
    {
        assert (_ptable && _ptable->primary_idx());
        return (index_probe(db, _ptable->primary_idx(), ptuple, lock_mode, root));
    }


    // probes based on the name of the index


    // idx probe - based on idx name //
    inline w_rc_t   index_probe_by_name(ss_m* db, 
                                        const char*  idx_name, 
                                        table_tuple* ptuple,
                                        lock_mode_t  lock_mode = SH,      
                                        const lpid_t& root = lpid_t::null)
    {
        index_desc_t* pindex = _ptable->find_index(idx_name);
        return (index_probe(db, pindex, ptuple, lock_mode, root));
    }

    // probe idx in EX (& LATCH_EX) mode - based on idx name //
    inline w_rc_t   index_probe_forupdate_by_name(ss_m* db, 
                                                  const char* idx_name,
                                                  table_tuple* ptuple,
                                                  const lpid_t& root = lpid_t::null) 
    { 
	index_desc_t* pindex = _ptable->find_index(idx_name);
	return (index_probe_forupdate(db, pindex, ptuple, root));
    }

    // probe idx in NL (& LATCH_NL) mode - based on idx name //
    inline w_rc_t   index_probe_nl_by_name(ss_m* db, 
                                           const char* idx_name,
                                           table_tuple* ptuple,
                                           const lpid_t& root = lpid_t::null) 
    { 
	index_desc_t* pindex = _ptable->find_index(idx_name);
	return (index_probe_nl(db, pindex, ptuple, root));
    }


    /* -------------------------- */
    /* --- tuple manipulation --- */
    /* -------------------------- */

    w_rc_t    add_tuple(ss_m* db, 
                        table_tuple*  ptuple, 
                        const lock_mode_t   lock_mode = EX,
                        const lpid_t& primary_root = lpid_t::null);

    w_rc_t    add_index_entry(ss_m* db,
			      const char* idx_name,
			      table_tuple* ptuple, 
			      const lock_mode_t lock_mode = EX,
			      const lpid_t& primary_root = lpid_t::null);
    
    w_rc_t    add_plp_tuple(ss_m* db, 
                            table_tuple*  ptuple, 
                            const lock_mode_t lock_mode,
                            const uint system_mode,
                            const lpid_t& primary_root = lpid_t::null);

    w_rc_t    delete_tuple(ss_m* db, 
                           table_tuple* ptuple, 
                           const lock_mode_t lock_mode = EX,
                           const lpid_t& primary_root = lpid_t::null);

    w_rc_t    delete_index_entry(ss_m* db,
				 const char* idx_name,
				 table_tuple* ptuple, 
				 const lock_mode_t lock_mode = EX,
				 const lpid_t& primary_root = lpid_t::null);


    // Direct access through the rid
    w_rc_t    update_tuple(ss_m* db, 
                           table_tuple* ptuple, 
                           const lock_mode_t lock_mode = EX);

    // Direct access through the rid
    w_rc_t    read_tuple(table_tuple* ptuple, 
                         lock_mode_t lock_mode = SH,
			 latch_mode_t heap_latch_mode = LATCH_SH);


    
    /* ----------------------------- */
    /* --- formatting operations --- */
    /* ----------------------------- */

    // format tuple
    int  format(table_tuple* ptuple, rep_row_t &arep);

    // load tuple from input buffer
    bool load(table_tuple* ptuple, const char* string);

    // disk space needed for tuple
    int  size(table_tuple* ptuple) const; 

    // format the key value
    int  format_key(index_desc_t* pindex, 
                    table_tuple* ptuple, 
                    rep_row_t &arep);

    // load key index from input buffer 
    bool load_key(const char* string,
                  index_desc_t* pindex,
                  table_tuple* ptuple);

    // set indexed fields of the row to minimum
    int  min_key(index_desc_t* pindex, 
                 table_tuple* ptuple, 
                 rep_row_t &arep);

    // set indexed fields of the row to maximum
    int  max_key(index_desc_t* pindex, 
                 table_tuple* ptuple, 
                 rep_row_t &arep);

    // length of the formatted key
    int  key_size(index_desc_t* pindex, 
                  const table_tuple* ptuple) const;





    /* ------------------------------------------------------- */
    /* --- check consistency between the indexes and table --- */
    /* ------------------------------------------------------- */

    virtual w_rc_t check_all_indexes_together(ss_m* db)=0;
    virtual bool   check_all_indexes(ss_m* db)=0;
    virtual w_rc_t check_index(ss_m* db, index_desc_t* pidx)=0;
    virtual w_rc_t scan_all_indexes(ss_m* db)=0;
    virtual w_rc_t scan_index(ss_m* db, index_desc_t* pidx)=0;


    /* -------------------------------- */
    /* - population related if needed - */
    /* -------------------------------- */
    virtual w_rc_t populate(ss_m* db, bool& hasNext) { return (RCOK); }


    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    /*
     * print the table on screen or to files
     * @note: PIN: right now it prints to files,
     *             with slight modification it can print to the screen as well
     */
    virtual w_rc_t print_table(ss_m* db, int num_lines)=0;


    /* --------------- */
    /* --- caching --- */
    /* --------------- */
    
    /* fetch the pages of the table and its indexes to buffer pool */
    virtual w_rc_t fetch_table(ss_m* db, lock_mode_t alm = SH); 


    /* ---------------------------------------------------------------
     *
     * @fn:    relocate_records
     *
     * @brief: The registered callback function, which is called when 
     *         PLP-Leaf or PLP-Part moves records
     *
     * --------------------------------------------------------------- */
    
    static w_rc_t relocate_records(vector<rid_t>&    old_rids, 
				   vector<rid_t>&    new_rids);





}; // EOF: table_man_t


/* ---------------------------------------------------------------
 *
 * @class: el_filler_leaf
 *
 * @brief: The callback for inserting PLP tuples 
 *
 * --------------------------------------------------------------- */

struct el_filler_part : public el_filler
{
    typedef table_row_t table_tuple; 

    el_filler_part(size_t indexentrysz,
                   ss_m* db,
                   table_man_t* ptable,
                   table_tuple* ptuple,
                   index_desc_t* pindex,
                   bool bIgnoreLocks);
    ss_m* _db;
    table_man_t* _ptableman;
    table_tuple* _ptuple;
    index_desc_t* _pindex;
    bool _bIgnoreLocks;
    rc_t fill_el(vec_t& el, const lpid_t& leaf);
};


/****************************************************************** 
 *
 *  class table_desc_t methods 
 *
 ******************************************************************/



/****************************************************************** 
 *
 * @fn:    find_field_by_name
 *
 * @brief: Returns the field index, given its name. If no such field 
 *         name exists it returns -1.
 *
 ******************************************************************/

inline int table_desc_t::find_field_by_name(const char* field_name) const
{
    for (uint_t i=0; i<_field_count; i++) {
        if (strcmp(field_name, _desc[i].name())==0) 
            return (i);
    }
    return (-1);
}



/****************************************************************** 
 *
 * @fn:    index_keydesc
 *
 * @brief: Iterates over all the fields of a selected index and returns 
 *         on a single string the corresponding key description
 *
 ******************************************************************/

inline char* table_desc_t::index_keydesc(index_desc_t* idx)
{
    CRITICAL_SECTION(idx_kd_cs, idx->_keydesc_lock);
    if (strlen(idx->_keydesc)>1) // is key_desc is already set
        return (idx->_keydesc);

    // else set the index keydesc
    for (uint_t i=0; i<idx->field_count(); i++) {
        strcat(idx->_keydesc, _desc[idx->key_index(i)].keydesc());
    }
    return (idx->_keydesc);
}



/******************************************************************
 *
 *  @fn:    index_maxkeysize
 *
 *  @brief: For an index it returns the maximum size of the index key
 *
 *  @note:  !!! Now that key_size() Uses the maxsize() of each field, 
 *              key_size() == maxkeysize()
 *
 ******************************************************************/

inline int table_desc_t::index_maxkeysize(index_desc_t* idx) const
{
    uint_t size = 0;
    if ((size = idx->get_keysize()) > 0) {
        // keysize has already been calculated
        // just return that value
        return (size);
    }
    
    // needs to calculate the (max)key for that index
    uint_t ix = 0;
    for (uint_t i=0; i<idx->field_count(); i++) {
        ix = idx->key_index(i);
        size += _desc[ix].fieldmaxsize();
    }    
    // set it for the index, for future invokations
    idx->set_keysize(size);
    return(size);
}



/****************************************************************** 
 *
 *  @fn:    maxsize()
 *
 *  @brief: Return the maximum size requirement for a tuple in disk format.
 *          Normally, it will be calculated only once.
 *
 ******************************************************************/

inline uint_t table_desc_t::maxsize()
{
    // shortcut not to re-compute maxsize
    if (*&_maxsize)
        return (*&_maxsize);

    // calculate maximum size required   
    uint_t size = 0;
    uint_t var_count = 0;
    uint_t null_count = 0;
    for (uint_t i=0; i<_field_count; i++) {
        size += _desc[i].fieldmaxsize();
        if (_desc[i].allow_null()) null_count++;
        if (_desc[i].is_variable_length()) var_count++;
    }
    
    size += (var_count*sizeof(offset_t)) + (null_count>>3) + 1;

    // There is a small window from the time it checks if maxsize is already set,
    // until the time it tries to set it up. In the meantime, another thread may
    // has done the calculation already. If that had happened, the two threads 
    // should have calculated the same maxsize, since it is the same table desc.
    // In other words, the maxsize should be either 0 or equal to the size.
    assert ((*&_maxsize==0) || (*&_maxsize==size));

    atomic_swap_uint(&_maxsize, size);
    return (*&_maxsize);
}







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


EXIT_NAMESPACE(shore);

#endif /* __SHORE_TABLE_H */
