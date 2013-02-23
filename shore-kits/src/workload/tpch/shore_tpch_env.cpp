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

/** @file:   shore_tpch_env.cpp
 *
 *  @brief:  Declaration of the Shore TPC-H environment (database)
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#include "workload/tpch/shore_tpch_env.h"
#include "sm/shore/shore_helper_loader.h"

#include "workload/tpch/tpch_random.h"

#include "workload/tpch/dbgen/dss.h"
#include "workload/tpch/dbgen/dsstypes.h"


using namespace shore;
using namespace dbgentpch;


ENTER_NAMESPACE(shore);
DEFINE_ROW_CACHE_TLS(tpch, nation);
DEFINE_ROW_CACHE_TLS(tpch, region);
DEFINE_ROW_CACHE_TLS(tpch, part);
DEFINE_ROW_CACHE_TLS(tpch, supplier);
DEFINE_ROW_CACHE_TLS(tpch, partsupp);
DEFINE_ROW_CACHE_TLS(tpch, customer);
DEFINE_ROW_CACHE_TLS(tpch, orders);
DEFINE_ROW_CACHE_TLS(tpch, lineitem);
EXIT_NAMESPACE(shore);


ENTER_NAMESPACE(tpch);



/******************************************************************** 
 *
 * TPC-H Parallel Loading
 *
 * Three classes:
 *
 * Creator      - Creates the tables and indexes, and loads first rows 
 *                at each table
 * Builder      - The parallel working loading workers
 * Checkpointer - Takes a checkpoint every 1 min
 *
 ********************************************************************/

const int PART_UNIT_PER_SF = 200000;
const int CUST_UNIT_PER_SF = 150000;
const int DIVISOR = 50;
const int PART_COUNT = 10000;
const int CUST_COUNT = 10000;

struct ShoreTPCHEnv::checkpointer_t : public thread_t 
{
    ShoreTPCHEnv* _env;
    checkpointer_t(ShoreTPCHEnv* env) 
        : thread_t("TPC-H Load Checkpointer"), _env(env) { }
    virtual void work();
};

class ShoreTPCHEnv::table_builder_t : public thread_t 
{
    ShoreTPCHEnv* _env;
    long _part_start;
    long _part_end;
    long _cust_start;
    long _cust_end;
    double _sf;
    int _loaders;
public:
    table_builder_t(ShoreTPCHEnv* env, const int id,
                    const long part_start, const long part_end,
                    const long cust_start, const long cust_end,
                    const double sf, const int loaders)
	: thread_t(c_str("TPC-H L-%d",id)), _env(env), 
          _part_start(part_start), _part_end(part_end), 
          _cust_start(cust_start), _cust_end(cust_end),
          _sf(sf), _loaders(loaders)
    { }
    virtual void work();
};

struct ShoreTPCHEnv::table_creator_t : public thread_t 
{
    ShoreTPCHEnv* _env;
    double _sf;
    int _loader_count;
    int _parts_per_thread;
    int _custs_per_thread;

    table_creator_t(ShoreTPCHEnv* env, const double sf, const int loader_count,
                    const int parts_per_thread, const int custs_per_thread)
	: thread_t("TPC-H C"), 
          _env(env), _sf(sf), 
          _loader_count(loader_count),
          _parts_per_thread(parts_per_thread),
          _custs_per_thread(custs_per_thread)
    { }
    virtual void work();
};


void ShoreTPCHEnv::checkpointer_t::work() 
{
    bool volatile* loaded = &_env->_loaded;
    while(!*loaded) {
	_env->set_measure(MST_MEASURE);
	for(int i=0; i < 60 && ! *loaded; i++)  {
	    ::sleep(1);
        }
	
        TRACE( TRACE_ALWAYS, "db checkpoint - start\n");
        _env->checkpoint();
        TRACE( TRACE_ALWAYS, "db checkpoint - end\n");
    }
    _env->set_measure(MST_PAUSE);
}


void ShoreTPCHEnv::table_creator_t::work() 
{
//     fprintf(stdout, "NATION:   %d\n", sizeof(tpch_nation_tuple));
//     fprintf(stdout, "REGION:   %d\n", sizeof(tpch_region_tuple));
//     fprintf(stdout, "SUPPLIER: %d\n", sizeof(tpch_supplier_tuple));
//     fprintf(stdout, "PART:     %d\n", sizeof(tpch_part_tuple));
//     fprintf(stdout, "PARTSUPP: %d\n", sizeof(tpch_partsupp_tuple));
//     fprintf(stdout, "CUSTOMER: %d\n", sizeof(tpch_customer_tuple));
//     fprintf(stdout, "ORDERS:   %d\n", sizeof(tpch_orders_tuple));
//     fprintf(stdout, "LINEITEM: %d\n", sizeof(tpch_lineitem_tuple));


    // Create the tables
    W_COERCE(_env->db()->begin_xct());
    W_COERCE(_env->_pnation_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pregion_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_ppart_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_psupplier_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_ppartsupp_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pcustomer_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_porders_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_plineitem_desc->create_physical_table(_env->db()));
    W_COERCE(_env->db()->commit_xct());


    // After they obtained their fid, register managers
    _env->_pnation_man->register_table_man();
    _env->_pregion_man->register_table_man();
    _env->_ppart_man->register_table_man();
    _env->_psupplier_man->register_table_man();
    _env->_ppartsupp_man->register_table_man();
    _env->_pcustomer_man->register_table_man();
    _env->_porders_man->register_table_man();
    _env->_plineitem_man->register_table_man();


    // Do the baseline transaction
    populate_baseline_input_t in = {_sf, _loader_count, DIVISOR, 
                                    _parts_per_thread, _custs_per_thread};

    w_rc_t e = RCOK;

    long log_space_needed = 0;
 retrybaseline:
    W_COERCE(_env->db()->begin_xct());
#ifdef USE_SHORE_6
    if(log_space_needed > 0) {
	W_COERCE(_env->db()->xct_reserve_log_space(log_space_needed));
    }
#endif
    e = _env->xct_populate_baseline(0, in);

    CHECK_XCT_RETURN(e,log_space_needed,retrybaseline,_env);
    
    W_COERCE(_env->db()->begin_xct());
    W_COERCE(_env->_post_init_impl());
    W_COERCE(_env->db()->commit_xct());
}


static unsigned long part_completed = 0;
static unsigned long cust_completed = 0;

void ShoreTPCHEnv::table_builder_t::work() 
{
    w_rc_t e = RCOK;

    if (_loaders==1)
    {
        TRACE( TRACE_ALWAYS, "Using simple loader with 1 thread.\n");
        return;
    }
    // 1. Load Part-related (~140MB)
    for(int i=_part_start ; i < _part_end; i+=PART_POP_UNIT) {
	while(_env->get_measure() != MST_MEASURE) {
	    usleep(1000);
	}

	long tid = i;
	populate_some_parts_input_t in = {tid};

        tid = std::min(_part_end-i,PART_POP_UNIT);

	long log_space_needed = 0;
    retrypart:
	W_COERCE(_env->db()->begin_xct());
#ifdef USE_SHORE_6
	if(log_space_needed > 0) {
	    W_COERCE(_env->db()->xct_reserve_log_space(log_space_needed));
	}
#endif
	e = _env->xct_populate_some_parts(tid, in);

        CHECK_XCT_RETURN(e,log_space_needed,retrypart,_env);

	long nval = atomic_add_64_nv(&part_completed,tid);
	if (nval % PART_COUNT == 0) {
            TRACE( TRACE_ALWAYS, "Parts %d/%d (%.0f%%)\n", 
                   nval, (int)(_sf*PART_UNIT_PER_SF),
                   (double)(100*nval)/(double)(_sf*PART_UNIT_PER_SF));
        }
    }
    
    TRACE( TRACE_ALWAYS, "Finished Parts %d .. %d \n", _part_start, _part_end);

    // 2. Load Cust-related (~825MB)
    for (uint i=_cust_start ; i < _cust_end; i+=CUST_POP_UNIT) {
	while(_env->get_measure() != MST_MEASURE) {
	    usleep(1000);
	}

	long tid = i;
	populate_some_custs_input_t in = {tid};

        tid = std::min(_cust_end-i,CUST_POP_UNIT);

	long log_space_needed = 0;
    retrycust:
	W_COERCE(_env->db()->begin_xct());
#ifdef USE_SHORE_6
	if(log_space_needed > 0) {
	    W_COERCE(_env->db()->xct_reserve_log_space(log_space_needed));
	}
#endif
	e = _env->xct_populate_some_custs(tid, in);

        CHECK_XCT_RETURN(e,log_space_needed,retrycust,_env);

	long nval = atomic_add_64_nv(&cust_completed,tid);
	if (nval % PART_COUNT == 0) {
            TRACE( TRACE_ALWAYS, "Customers %d/%d (%.0f%%)\n", 
                   nval, (int)(_sf*CUST_UNIT_PER_SF),
                   (double)(100*nval)/(double)(_sf*CUST_UNIT_PER_SF));
        }
    }
    
    TRACE( TRACE_ALWAYS, "Finished Custs %d .. %d \n", _cust_start, _cust_end);
}




/******************************************************************** 
 *
 * ShoreTPCHEnv functions
 *
 ********************************************************************/ 

ShoreTPCHEnv::ShoreTPCHEnv()
    : ShoreEnv()
{
    _scaling_factor = TPCH_SCALING_FACTOR;

#ifdef CFG_QPIPE
    // Set the default scheduling policy. We will worry later about changing
    // that, possibly through the shell
    set_sched_policy(NULL);

    // Register stage containers
    register_stage_containers();
#endif
}


ShoreTPCHEnv::~ShoreTPCHEnv() 
{
}



w_rc_t ShoreTPCHEnv::load_schema()
{
    // create the schema
    _pnation_desc   = new nation_t(get_pd());
    _pregion_desc   = new region_t(get_pd());
    _ppart_desc     = new part_t(get_pd());
    _psupplier_desc = new supplier_t(get_pd());
    _ppartsupp_desc = new partsupp_t(get_pd());
    _pcustomer_desc = new customer_t(get_pd());
    _porders_desc   = new orders_t(get_pd());
    _plineitem_desc = new lineitem_t(get_pd());

    // initiate the table managers
    _pnation_man   = new nation_man_impl(_pnation_desc.get());
    _pregion_man   = new region_man_impl(_pregion_desc.get());
    _ppart_man     = new part_man_impl(_ppart_desc.get());
    _psupplier_man = new supplier_man_impl(_psupplier_desc.get());
    _ppartsupp_man = new partsupp_man_impl(_ppartsupp_desc.get());
    _pcustomer_man = new customer_man_impl(_pcustomer_desc.get());
    _porders_man   = new orders_man_impl(_porders_desc.get());
    _plineitem_man = new lineitem_man_impl(_plineitem_desc.get());
                
    return (RCOK);
}


#ifdef CFG_QPIPE

/******************************************************************** 
 *
 *  @fn:    {set,get}_sched_policy()
 *
 ********************************************************************/

policy_t* ShoreTPCHEnv::get_sched_policy()
{
    CRITICAL_SECTION(init_cs,_load_mutex);
    return (_sched_policy.get());
}

policy_t* ShoreTPCHEnv::set_sched_policy(const char* spolicy)
{
    CRITICAL_SECTION(init_cs,_load_mutex);
    if (spolicy) {

        TRACE( TRACE_ALWAYS, "Setting policy (%s)\n", spolicy);
        
        if ( !strcmp(spolicy, "OS") ) {
            _sched_policy = new policy_os_t();
            return (_sched_policy);
        }

        if ( !strcmp(spolicy, "RR_CPU") ) {
            _sched_policy = new policy_rr_cpu_t();
            return (_sched_policy);
        }

        if ( !strcmp(spolicy, "QUERY_CPU") ) {
            _sched_policy = new policy_query_cpu_t();
            return (_sched_policy);
        }

        if ( !strcmp(spolicy, "RR_MODULE") ) {
            _sched_policy = new policy_rr_module_t();
            return (_sched_policy);
        }
    }
    // Use the default scheduling policy (let the OS choose) 
    TRACE( TRACE_ALWAYS, "Default scheduling policy (OS)\n");
    _sched_policy = new policy_os_t();
    return (_sched_policy);
}

#endif //CFG_QPIPE


/******************************************************************** 
 *
 *  @fn:    info()
 *
 *  @brief: Prints information about the current db instance status
 *
 ********************************************************************/

int ShoreTPCHEnv::info() const
{
    TRACE( TRACE_ALWAYS, "SF      = (%.1f)\n", _scaling_factor);
    return (0);
}



/******************************************************************** 
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics for TPC-H 
 *
 ********************************************************************/

int ShoreTPCHEnv::statistics() 
{
    return (0);
}



/******************************************************************** 
 *
 *  @fn:    start/stop
 *
 *  @brief: Simply call the corresponding functions of shore_env 
 *
 ********************************************************************/

int ShoreTPCHEnv::start()
{
    return (ShoreEnv::start());
}

int ShoreTPCHEnv::stop()
{
    return (ShoreEnv::stop());
}



/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/****************************************************************** 
 *
 * @fn:    loaddata()
 *
 * @brief: Loads the data for all the TPCH tables, given the current
 *         scaling factor value. During the loading the SF cannot be
 *         changed.
 *
 ******************************************************************/

w_rc_t ShoreTPCHEnv::loaddata() 
{
    // 0. lock the loading status and the scaling factor
    CRITICAL_SECTION(load_cs, _load_mutex);
    if (_loaded) {
        TRACE( TRACE_TRX_FLOW, 
               "Env already loaded. Doing nothing...\n");
        return (RCOK);
    }        
    CRITICAL_SECTION(scale_cs, _scaling_mutex);

    // 1. Call the function that initializes the dbgen
    dbgen_init();


    time_t tstart = time(NULL);

    /* partly (no) thanks to Shore's next key index locking, and
       partly due to page latch and SMO issues, we have ridiculous
       deadlock rates if we try to throw lots of threads at a small
       btree. To work around this we preload a number of records for
       each table and then fire up the (parallel) workers.
     */

    // 1. Read the number of parallel loaders and calculate ranges
    int loaders_to_use = envVar::instance()->getVarInt("db-loaders",10);
    long total_parts = _scaling_factor*PART_UNIT_PER_SF;
    long total_custs = _scaling_factor*CUST_UNIT_PER_SF;
    long parts_per_thread = total_parts/loaders_to_use;
    long custs_per_thread = total_custs/loaders_to_use;

    // 2. Fire up the table creator and baseline loader
    {
	guard<table_creator_t> tc;
	tc = new table_creator_t(this, _scaling_factor, loaders_to_use,
                                 parts_per_thread, custs_per_thread);
	tc->fork();
	tc->join();
    }

    // 3. Fire up a checkpointer 
    guard<checkpointer_t> chk(new checkpointer_t(this));
    chk->fork();

    // 4. Fire up the parallel loaders
    TRACE( TRACE_ALWAYS, "Firing up %d loaders ..\n", loaders_to_use);
    array_guard_t< guard<table_builder_t> > loaders(new guard<table_builder_t>[loaders_to_use]);
    for(int i=0; i < loaders_to_use; i++) {
	long part_start = (i*parts_per_thread) + DIVISOR;
	long part_end = ((i+1)*parts_per_thread > total_parts - 1) ? 
            total_parts : (i+1)*parts_per_thread - 1;
        assert (part_start <= part_end);

	long cust_start = (i*custs_per_thread) + DIVISOR;
	long cust_end = ((i+1)*custs_per_thread > total_custs - 1) ? 
            total_custs : (i+1)*custs_per_thread - 1;
        assert (cust_start <= cust_end);

	loaders[i] = new table_builder_t(this, i, 
                                         part_start, part_end, 
                                         cust_start, cust_end,
                                         _scaling_factor, loaders_to_use);
	loaders[i]->fork();
    }

    for(int i=0; i < loaders_to_use; i++) {
	loaders[i]->join();
    }

    time_t tstop = time(NULL);

    // 5. Print stats
    TRACE( TRACE_STATISTICS, "Loading finished. %d tables loaded in (%d) secs...\n",
           SHORE_TPCH_TABLES, (tstop - tstart));

    // 6. Notify that the env is loaded
    _loaded = true;
    chk->join();

    return (RCOK);
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

w_rc_t ShoreTPCHEnv::check_consistency()
{
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

w_rc_t ShoreTPCHEnv::warmup()
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

int ShoreTPCHEnv::dump()
{
    return (0);
}


int ShoreTPCHEnv::conf()
{
    // reread the params
    ShoreEnv::conf();
    upd_sf();
    return (0);
}



/********************************************************************
 *
 * Make sure the contented tables (like WH in TPC-C) are padded to one 
 * record per page
 *
 * Example from TPC-C: For the dataset sizes we can afford to run, all 
 * WH records fit on a single page, leading to massive latch contention 
 * even though each thread updates a different WH tuple.
 *
 * If the WH records are big enough, do nothing; otherwise replicate
 * the existing WH table and index with padding, drop the originals,
 * and install the new files in the directory.
 *
 *********************************************************************/

int ShoreTPCHEnv::post_init() 
{
    conf();

    W_COERCE(db()->begin_xct());
    w_rc_t rc = _post_init_impl();
    if(rc.is_error()) {
	db()->abort_xct();
	return (rc.err_num());
    }
    else {
	TRACE( TRACE_ALWAYS, "-> Done\n");
	db()->commit_xct();
	return (0);
    }
}


/********************************************************************* 
 *
 *  @fn:    _post_init_impl
 *
 *  @brief: No contented tables in TPC-H
 *
 *********************************************************************/ 

w_rc_t 
ShoreTPCHEnv::_post_init_impl() 
{
    TRACE( TRACE_DEBUG, "So far, nothing to pad in TPC-H..\n");
    return (RCOK);
}
  


EXIT_NAMESPACE(tpch);
