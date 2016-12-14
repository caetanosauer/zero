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

/** @file:   shore_tpcc_env.cpp
 *
 *  @brief:  Declaration of the Shore TPC-C environment (database)
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#include "tpcc_env.h"

#include "tpcc_random.h"
#include "xct.h"

DEFINE_ROW_CACHE_TLS(tpcc, warehouse);
DEFINE_ROW_CACHE_TLS(tpcc, district);
DEFINE_ROW_CACHE_TLS(tpcc, stock);
DEFINE_ROW_CACHE_TLS(tpcc, order_line);
DEFINE_ROW_CACHE_TLS(tpcc, customer);
DEFINE_ROW_CACHE_TLS(tpcc, history);
DEFINE_ROW_CACHE_TLS(tpcc, order);
DEFINE_ROW_CACHE_TLS(tpcc, new_order);
DEFINE_ROW_CACHE_TLS(tpcc, item);

namespace tpcc {

/********************************************************************
 *
 * TPC-C Parallel Loading
 *
 ********************************************************************/


class ShoreTPCCEnv::table_builder_t : public thread_t
{
    ShoreTPCCEnv* _env;
    long _start;
    long _count;
    int* _cids;
public:
    table_builder_t(ShoreTPCCEnv* env, const int id, long start, long count, int* cids)
	: thread_t(string("LD-%d", id)),
          _env(env), _start(start), _count(count), _cids(cids) { }
    virtual void work();
};


struct ShoreTPCCEnv::table_creator_t : public thread_t
{
    ShoreTPCCEnv* _env;
    int _sf;
    table_creator_t(ShoreTPCCEnv* env, int sf)
	: thread_t("CR"), _env(env), _sf(sf) { }
    virtual void work();
};


void ShoreTPCCEnv::table_creator_t::work()
{
    // Create the tables
    xct_t::begin();
    W_COERCE(_env->_pwarehouse_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pdistrict_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pcustomer_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_phistory_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pnew_order_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_porder_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_porder_line_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pitem_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pstock_desc->create_physical_table(_env->db()));
    W_COERCE(xct_t::commit());

    // do the first transaction
    populate_baseline_input_t in = {_sf};
    xct_t::begin();
    W_COERCE(_env->xct_populate_baseline(0, in));

    // W_COERCE(_env->db()->begin_xct());
    // W_COERCE(_env->_post_init_impl());
    // W_COERCE(_env->db()->commit_xct());

#if 0
    /*
      create 10k accounts in each partition to buffer workers from each other
     */
    for(long i=-1; i < _pcount; i++) {
	long a_id = i*_psize;
	populate_db_input_t in(_sf, a_id);
	fprintf(stderr, "Populating %d a_ids starting with %d\n", ACCOUNTS_CREATED_PER_POP_XCT, a_id);
	W_COERCE(_env->db()->begin_xct());
	W_COERCE(_env->xct_populate_db(&in, a_id));
    }
#endif
}
static void gen_cid_array(int* cid_array) {
    for(int i=0; i < ORDERS_PER_DIST; i++)
	cid_array[i] = i+1;
    for(int i=0; i < ORDERS_PER_DIST; i++) {
	std::swap(cid_array[i], cid_array[i+URand(0,ORDERS_PER_DIST-i-1)]);
                  //sthread_t::randn(ORDERS_PER_DIST-i)]);
    }
}


static unsigned long units_completed = 0;

void ShoreTPCCEnv::table_builder_t::work()
{
    w_rc_t e = RCOK;

    /* There are up to three parts of my job: the parts at the
       beginning and end which may overlap with other workers (need to
       coordinate cids used for orders) and those which I fully own.

       When coordination is needed we use the passed-in cid
       permutation array; otherwise we generate our own.
    */
    int cid_array[ORDERS_PER_DIST];
    gen_cid_array(cid_array);

    // CS TODO: break this loop into a batch size, so that log and buffer
    // pool space can be reclaimed.

    //int last_wh = 1;
    for(int i=0 ; i < _count; i++) {
        /*
         * CS: commented out -- MST_MEASURE was set by the old checkpointer thread.
         * But why did we need to wait for the checkpointer to start loading???
         */
//	while(_env->get_measure() != MST_MEASURE)
//	    usleep(10000);

	int tid = _start + i;
	//int my_dist = tid/UNIT_PER_DIST;
	int start_dist = (tid + UNIT_PER_DIST - 1)/UNIT_PER_DIST;
	int end_dist = (tid + UNIT_PER_DIST)/UNIT_PER_DIST;
	bool overlap = (start_dist*UNIT_PER_DIST < _start) || (end_dist*UNIT_PER_DIST >= _start+_count);
	int *cids = overlap? _cids : cid_array+0;
	populate_one_unit_input_t in = {tid, cids};
    retry:
        xct_t::begin();

	e = _env->xct_populate_one_unit(tid, in);

        CHECK_XCT_RETURN(e,retry,_env);

	long nval = lintel::unsafe::atomic_fetch_add(&units_completed, 1);
        long sofar = nval / UNIT_PER_WH;
	if(nval % UNIT_PER_WH == 0) {
	    fprintf(stderr, "%lu\n", sofar);
        }
    }
    TRACE( TRACE_ALWAYS,
           "Finished loading units %ld .. %ld \n",
           _start, _start+_count);
}




/********************************************************************
 *
 * ShoreTPCCEnv functions
 *
 ********************************************************************/

ShoreTPCCEnv::ShoreTPCCEnv(boost::program_options::variables_map map)
    : ShoreEnv(map)
{
}

ShoreTPCCEnv::~ShoreTPCCEnv()
{
}



/********************************************************************
 *
 *  @fn:    load_schema()
 *
 *  @brief: Creates the table_desc_t and table_man_impl objects for
 *          each TPC-C table
 *
 ********************************************************************/

w_rc_t ShoreTPCCEnv::load_schema()
{
    // create the schema
    _pwarehouse_desc  = new warehouse_t(get_pd());
    _pdistrict_desc   = new district_t(get_pd());
    _pcustomer_desc   = new customer_t(get_pd());
    _phistory_desc    = new history_t(get_pd());
    _pnew_order_desc  = new new_order_t(get_pd());
    _porder_desc      = new order_t(get_pd());
    _porder_line_desc = new order_line_t(get_pd());
    _pitem_desc       = new item_t(get_pd());
    _pstock_desc      = new stock_t(get_pd());


    // initiate the table managers
    _pwarehouse_man  = new warehouse_man_impl(_pwarehouse_desc.get());
    _pdistrict_man   = new district_man_impl(_pdistrict_desc.get());
    _pstock_man      = new stock_man_impl(_pstock_desc.get());
    _porder_line_man = new order_line_man_impl(_porder_line_desc.get());
    _pcustomer_man   = new customer_man_impl(_pcustomer_desc.get());
    _phistory_man    = new history_man_impl(_phistory_desc.get());
    _porder_man      = new order_man_impl(_porder_desc.get());
    _pnew_order_man  = new new_order_man_impl(_pnew_order_desc.get());
    _pitem_man       = new item_man_impl(_pitem_desc.get());

    return (RCOK);
}



/********************************************************************
 *
 *  @fn:    load_and_register_fids()
 *
 *  @brief: loads the store ids for each table and index at kits side
 *          as well as registering the tables
 *
 ********************************************************************/

w_rc_t ShoreTPCCEnv::load_and_register_fids()
{
    W_DO(_pwarehouse_man->load_and_register_fid(db()));
    W_DO(_pdistrict_man->load_and_register_fid(db()));
    W_DO(_pstock_man->load_and_register_fid(db()));
    W_DO(_porder_line_man->load_and_register_fid(db()));
    W_DO(_pcustomer_man->load_and_register_fid(db()));
    W_DO(_phistory_man->load_and_register_fid(db()));
    W_DO(_porder_man->load_and_register_fid(db()));
    W_DO(_pnew_order_man->load_and_register_fid(db()));
    W_DO(_pitem_man->load_and_register_fid(db()));
    return (RCOK);
}

/********************************************************************
 *
 *  @fn:    set_skew()
 *
 *  @brief: sets load imbalance for TPC-C
 *
 ********************************************************************/
void ShoreTPCCEnv::set_skew(int area, int load, int start_imbalance, int skew_type)
{
    ShoreEnv::set_skew(area, load, start_imbalance, skew_type);
    // for warehouses
    w_skewer.set(area, 1, _scaling_factor, load);
}


/********************************************************************
 *
 *  @fn:    start_load_imbalance()
 *
 *  @brief: sets the flag that triggers load imbalance for TPC-C
 *          resets the intervals if necessary (depending on the skew type)
 *
 ********************************************************************/
void ShoreTPCCEnv::start_load_imbalance()
{
    if(w_skewer.is_used()) {
	_change_load = false;
	// for warehouses
	w_skewer.reset(_skew_type);
    }
    if(_skew_type != SKEW_CHAOTIC || URand(1,100) > 30) {
	_change_load = true;
    }
    ShoreEnv::start_load_imbalance();
}


/********************************************************************
 *
 *  @fn:    reset_skew()
 *
 *  @brief: sets the flag that stops the load imbalance for TPC-C
 *          and cleans the intervals
 *
 ********************************************************************/
void ShoreTPCCEnv::reset_skew()
{
    ShoreEnv::reset_skew();
    _change_load = false;
    w_skewer.clear();
}


/********************************************************************
 *
 *  @fn:    info()
 *
 *  @brief: Prints information about the current db instance status
 *
 ********************************************************************/

int ShoreTPCCEnv::info() const
{
    TRACE( TRACE_ALWAYS, "SF      = (%.1f)\n", _scaling_factor);
    TRACE( TRACE_ALWAYS, "Workers = (%d)\n", _worker_cnt);
    return (0);
}



/********************************************************************
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics for TPC-C
 *
 ********************************************************************/

int ShoreTPCCEnv::statistics()
{
    // read the current trx statistics
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreTPCCTrxStats rval;
    rval -= rval; // dirty hack to set all zeros
    for (statmap_t::iterator it=_statmap.begin(); it != _statmap.end(); ++it)
	rval += *it->second;

    TRACE( TRACE_STATISTICS, "NewOrder. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.new_order,
           rval.failed.new_order,
           rval.deadlocked.new_order);

    TRACE( TRACE_STATISTICS, "Payment. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.payment,
           rval.failed.payment,
           rval.deadlocked.payment);

    TRACE( TRACE_STATISTICS, "OrderStatus. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.order_status,
           rval.failed.order_status,
           rval.deadlocked.order_status);

    TRACE( TRACE_STATISTICS, "Delivery. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.delivery,
           rval.failed.delivery,
           rval.deadlocked.delivery);

    TRACE( TRACE_STATISTICS, "StockLevel. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.stock_level,
           rval.failed.stock_level,
           rval.deadlocked.stock_level);

    TRACE( TRACE_STATISTICS, "MBenchWh. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.mbench_wh,
           rval.failed.mbench_wh,
           rval.deadlocked.mbench_wh);

    TRACE( TRACE_STATISTICS, "MBenchCust. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.mbench_cust,
           rval.failed.mbench_cust,
           rval.deadlocked.mbench_cust);

    ShoreEnv::statistics();

    return (0);
}



/********************************************************************
 *
 *  @fn:    start/stop
 *
 *  @brief: Simply call the corresponding functions of shore_env
 *
 ********************************************************************/

int ShoreTPCCEnv::start()
{
    return (ShoreEnv::start());
}

int ShoreTPCCEnv::stop()
{
    return (ShoreEnv::stop());
}




/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/******************************************************************
 *
 * @fn:    create_tables()
 *
 * @brief: Creates TPCC tables. May only be invoked from ShoreEnv::load(),
 * which aquires the necessary mutexes!
 *
 ******************************************************************/
w_rc_t ShoreTPCCEnv::create_tables()
{
	int cid_array[ORDERS_PER_DIST];
	gen_cid_array(cid_array);

	// 1. The table creator creates the tables and loads the first records per table
	{
		guard<table_creator_t> tc;
		tc = new table_creator_t(this, _scaling_factor);
		tc->fork();
		tc->join();
	}

	return RCOK;
}

/******************************************************************
 *
 * @fn:    load_data()
 *
 * @brief: Loads the data for all the TPCC tables, given the current
 *         scaling factor value. During the loading the SF cannot be
 *         changed. May only be invoked from ShoreEnv::load(), which
 *         aquires the necessary mutexes!
 *
 ******************************************************************/
w_rc_t ShoreTPCCEnv::load_data()
{
	int cid_array[ORDERS_PER_DIST];
	gen_cid_array(cid_array);

    // 3. Fire up the loader threads
    unique_ptr<table_builder_t> loaders[_loaders_to_use];
    long total_units = _scaling_factor * UNIT_PER_WH;

    // WARNING: unit_per_thread must divide this constant
    long min_chunk = ORDERS_PER_DIST/ORDERS_PER_UNIT; // 3000/30 = 100
    long chunks_per_thread = (total_units/min_chunk) / _loaders_to_use;
    long units_per_thread = chunks_per_thread * min_chunk;

    for(int i=0; i < _loaders_to_use; i++) {
	long count = units_per_thread;
	long start = i * count;
        if (i == _loaders_to_use - 1) {
            // Last loader will get the remainder
            count = total_units - start;
        }
	loaders[i].reset(new table_builder_t(this, i, start, count, cid_array));
	loaders[i]->fork();
    }

    for(int i=0; i < _loaders_to_use; i++) {
	loaders[i]->join();
    }

    return RCOK;
}



/******************************************************************
 *
 * @fn:    check_consistency()
 *
 * @brief: Iterates over all tables and checks consistency between
 *         the values stored in the base table (file) and the
 *         corresponding indexes.
 *
 ******************************************************************/

w_rc_t ShoreTPCCEnv::check_consistency()
{
    assert (0); // IP: Disabled
    return (RCOK);
}


/******************************************************************
 *
 * @fn:    warmup()
 *
 * @brief: Touches the entire database - For memory-fitting databases
 *         this is enough to bring it to load it to memory
 *
 ******************************************************************/

w_rc_t ShoreTPCCEnv::warmup()
{
    return (check_consistency());
}


/********************************************************************
 *
 *  @fn:    dump
 *
 *  @brief: Print information for all the tables in the environment
 *
 ********************************************************************/

int ShoreTPCCEnv::dump()
{
    assert (0); // IP: Not implemented yet

//     table_man_t* ptable_man = NULL;
//     for(table_man_list_iter table_man_iter = _table_man_list.begin();
//         table_man_iter != _table_man_list.end(); table_man_iter++)
//         {
//             ptable_man = *table_man_iter;
//             ptable_man->print_table(this->_pssm);
//         }
    return (0);
}


int ShoreTPCCEnv::conf()
{
    // reread the params
    ShoreEnv::conf();
    upd_worker_cnt();
    return (0);
}


/********************************************************************
 *
 * Make sure the WH table is padded to one record per page
 *
 * For the dataset sizes we can afford to run, all WH records fit on a
 * single page, leading to massive latch contention even though each
 * thread updates a different WH tuple.
 *
 * If the WH records are big enough, do nothing; otherwise replicate
 * the existing WH table and index with padding, drop the originals,
 * and install the new files in the directory.
 *
 *********************************************************************/

int ShoreTPCCEnv::post_init()
{
    conf();

    // If the database is set to be padded
    if (get_pd() == PD_PADDED) {
        // CS: not supported
        // We should probably pad it by default at creation (TODO)
        w_assert0(false);
        TRACE( TRACE_ALWAYS, "Checking for WH record padding...\n");

        // W_COERCE(db()->begin_xct());
        // w_rc_t rc = _post_init_impl();
        // if(rc.is_error()) {
        //     cerr << "-> WH padding failed with: " << rc << endl;
        //     rc = db()->abort_xct();
        //     return (rc.err_num());
        // }
        // else {
        //     TRACE( TRACE_ALWAYS, "-> Done\n");
        //     rc = db()->commit_xct();
        //     return (0);
        // }
    }

    return (0);
}


/*********************************************************************
 *
 *  @fn:    _post_init_impl
 *
 *  @brief: Makes sure the WH table is padded to one record per page
 *
 *********************************************************************/

#if 0
w_rc_t ShoreTPCCEnv::_post_init_impl()
{
#ifndef CFG_HACK
    return (RCOK);
#endif

    TRACE (TRACE_ALWAYS, "Padding WAREHOUSES");
    ss_m* db = this->db();

    // lock the WH table
    warehouse_t* wh = warehouse_desc();
    index_desc_t* idx = wh->indexes();
    int icount = wh->index_count();
    stid_t wh_fid = wh->fid();

    // lock the table and index(es) for exclusive access
    W_DO(db->lock(wh_fid, EX));
    for(int i=0; i < icount; i++) {
	for(int j=0; j < idx[i].get_partition_count(); j++)
	    W_DO(db->lock(idx[i].fid(j), EX));
    }

    guard<ats_char_t> pts = new ats_char_t(wh->maxsize());

    /* copy and pad all tuples smaller than 4k

       WARNING: this code assumes that existing tuples are packed
       densly so that all padded tuples are added after the last
       unpadded one
    */
    bool eof;
    static int const PADDED_SIZE = 4096; // we know you can't fit two 4k records on a single page
    array_guard_t<char> padding = new char[PADDED_SIZE];
    std::vector<rid_t> hit_list;
    {
	guard<warehouse_man_impl::table_iter> iter;
	{
	    warehouse_man_impl::table_iter* tmp;
	    W_DO(warehouse_man()->get_iter_for_file_scan(db, tmp));
	    iter = tmp;
	}

	int count = 0;
	table_row_t row(wh);
	rep_row_t arep(pts);
	int psize = wh->maxsize()+1;

	W_DO(iter->next(db, eof, row));
	while (1) {
	    pin_i* handle = iter->cursor();
	    if (!handle) {
		TRACE(TRACE_ALWAYS, " -> Reached EOF. Search complete (%d)\n", count);
		break;
	    }

	    // figure out how big the old record is
	    int hsize = handle->hdr_size();
	    int bsize = handle->body_size();
	    if (bsize == psize) {
		TRACE(TRACE_ALWAYS, " -> Found padded WH record. Stopping search (%d)\n", count);
		break;
	    }
	    else if (bsize > psize) {
		// too big... shrink it down to save on logging
		handle->truncate_rec(bsize - psize);
                fprintf(stderr, "+");
	    }
	    else {
		// copy and pad the record (and mark the old one for deletion)
		rid_t new_rid;
		vec_t hvec(handle->hdr(), hsize);
		vec_t dvec(handle->body(), bsize);
		vec_t pvec(padding, PADDED_SIZE-bsize);
		W_DO(db->create_rec(wh_fid, hvec, PADDED_SIZE, dvec, new_rid));
		W_DO(db->append_rec(new_rid, pvec));
                // for small databases, first padded record fits on this page
                if (not handle->up_to_date())
                    handle->repin();

                // mark the old record for deletion
		hit_list.push_back(handle->rid());

		// update the index(es)
		vec_t rvec(&row._rid, sizeof(rid_t));
		vec_t nrvec(&new_rid, sizeof(new_rid));
		for(int i=0; i < icount; i++) {
		    int key_sz = warehouse_man()->format_key(idx+i, &row, arep);
		    vec_t kvec(arep._dest, key_sz);

		    /* destroy the old mapping and replace it with the new
		       one.  If it turns out this is super-slow, we can
		       look into probing the index with a cursor and
		       updating it directly.
		    */
		    int pnum = _pwarehouse_man->get_pnum(&idx[i], &row);
		    stid_t fid = idx[i].fid(pnum);

		    if(idx[i].is_mr()) {
			W_DO(db->destroy_mr_assoc(fid, kvec, rvec));
			// now put the entry back with the new rid
			el_filler ef;
			ef._el.put(nrvec);
			W_DO(db->create_mr_assoc(fid, kvec, ef));
		    } else {
			W_DO(db->destroy_assoc(fid, kvec, rvec));
			// now put the entry back with the new rid
			W_DO(db->create_assoc(fid, kvec, nrvec));
		    }

		}
                fprintf(stderr, ".");
	    }

	    // next!
	    count++;
	    W_DO(iter->next(db, eof, row));
	}
        fprintf(stderr, "\n");

	// put the iter out of scope
    }

    // delete the old records
    int hlsize = hit_list.size();
    TRACE(TRACE_ALWAYS, "-> Deleting (%d) old unpadded records\n", hlsize);
    for(int i=0; i < hlsize; i++) {
	W_DO(db->destroy_rec(hit_list[i]));
    }

    return (RCOK);
}
#endif


/*********************************************************************
 *
 *  @fn:   db_print
 *
 *  @brief: Prints the current tpcc tables to files
 *
 *********************************************************************/

w_rc_t ShoreTPCCEnv::db_print(int /*lines*/)
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // print tables -- CS TODO
    // W_DO(_pwarehouse_man->print_table(_pssm, lines));
    // W_DO(_pdistrict_man->print_table(_pssm, lines));
    // W_DO(_pstock_man->print_table(_pssm, lines));
    // W_DO(_porder_line_man->print_table(_pssm, lines));
    // W_DO(_pcustomer_man->print_table(_pssm, lines));
    // W_DO(_phistory_man->print_table(_pssm, lines));
    // W_DO(_porder_man->print_table(_pssm, lines));
    // W_DO(_pnew_order_man->print_table(_pssm, lines));
    // W_DO(_pitem_man->print_table(_pssm, lines));

    return (RCOK);
}


/*********************************************************************
 *
 *  @fn:   db_fetch
 *
 *  @brief: Fetches the current tpcc tables to buffer pool
 *
 *********************************************************************/

w_rc_t ShoreTPCCEnv::db_fetch()
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // fetch tables
    W_DO(_pnew_order_man->fetch_table(_pssm));
    W_DO(_porder_line_man->fetch_table(_pssm));
    // W_DO(_phistory_man->fetch_table(_pssm));
    W_DO(_porder_man->fetch_table(_pssm));
    W_DO(_pitem_man->fetch_table(_pssm));
    W_DO(_pcustomer_man->fetch_table(_pssm));
    W_DO(_pwarehouse_man->fetch_table(_pssm));
    W_DO(_pdistrict_man->fetch_table(_pssm));
    W_DO(_pstock_man->fetch_table(_pssm));

    return (RCOK);
}


};
