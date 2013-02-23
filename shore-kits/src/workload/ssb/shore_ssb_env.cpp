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

/** @file:   shore_ssb_env.cpp
 *
 *  @brief:  Declaration of the Shore SSB environment (database)
 *
 *  @author: Manos Athanassoulis
 */

#include "workload/ssb/shore_ssb_env.h"
#include "sm/shore/shore_helper_loader.h"

#include "workload/ssb/ssb_random.h"

#include "workload/ssb/dbgen/dss.h"
#include "workload/ssb/dbgen/dsstypes.h"


using namespace shore;
using namespace dbgenssb;


ENTER_NAMESPACE(shore);
DEFINE_ROW_CACHE_TLS(ssb, part);
DEFINE_ROW_CACHE_TLS(ssb, supplier);
DEFINE_ROW_CACHE_TLS(ssb, date);
DEFINE_ROW_CACHE_TLS(ssb, customer);
DEFINE_ROW_CACHE_TLS(ssb, lineorder);
EXIT_NAMESPACE(shore);


ENTER_NAMESPACE(ssb);



/******************************************************************** 
 *
 * SSB Parallel Loading
 *
 * Three classes:
 *
 * Creator      - Creates the tables and indexes, and loads first rows 
 *                at each table
 * Builder      - The parallel working loading workers
 * Checkpointer - Takes a checkpoint every 1 min
 *
 ********************************************************************/

//const int PART_UNIT_PER_SF = 200000;//with log growth 200000*(1+log2(SF))
//const int CUST_UNIT_PER_SF = 30000;
const int DIVISOR = 50;//???
//const int PART_COUNT = 10000;//??
//const int CUST_COUNT = 10000;//??
const int LINEORDER_COUNT = 10000;//??

const int LINEORDER_UNIT_PER_SF = LINEORDER_PER_SF;

struct ShoreSSBEnv::checkpointer_t : public thread_t 
{
    ShoreSSBEnv* _env;
    checkpointer_t(ShoreSSBEnv* env) 
        : thread_t("SSB Load Checkpointer"), _env(env) { }
    virtual void work();
};

class ShoreSSBEnv::table_builder_t : public thread_t 
{
    ShoreSSBEnv* _env;
    long _lineorder_start;
    long _lineorder_end;
    double _sf;

public:
    table_builder_t(ShoreSSBEnv* env, const int id,
                    const long lineorder_start, const long lineorder_end,
                    const double sf)
	: thread_t(c_str("SSB L-%d",id)), _env(env), 
          _lineorder_start(lineorder_start), _lineorder_end(lineorder_end),
          _sf(sf)
    { }
    virtual void work();
};

struct ShoreSSBEnv::table_creator_t : public thread_t 
{
    ShoreSSBEnv* _env;
    double _sf;
    int _loader_count;
    int _lineorder_per_thread;
    //    int _parts_per_thread;
    //    int _custs_per_thread;

    table_creator_t(ShoreSSBEnv* env, const double sf, const int loader_count,
                    const int lineorder_per_thread)
	: thread_t("SSB C"), 
          _env(env), _sf(sf), 
          _loader_count(loader_count),
          _lineorder_per_thread(lineorder_per_thread)
    { }
    virtual void work();
};


void ShoreSSBEnv::checkpointer_t::work() 
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


void ShoreSSBEnv::table_creator_t::work() 
{
    /*
     fprintf(stdout, "SUPPLIER:  %ld\n", sizeof(ssb_supplier_tuple));
     fprintf(stdout, "PART:      %ld\n", sizeof(ssb_part_tuple));
     fprintf(stdout, "DATE:      %ld\n", sizeof(ssb_date_tuple));
     fprintf(stdout, "CUSTOMER:  %ld\n", sizeof(ssb_customer_tuple));
     fprintf(stdout, "LINEORDER: %ld\n", sizeof(ssb_lineorder_tuple));
    */

    // Create the tables
    W_COERCE(_env->db()->begin_xct());
    W_COERCE(_env->_ppart_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_psupplier_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pdate_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pcustomer_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_plineorder_desc->create_physical_table(_env->db()));
    W_COERCE(_env->db()->commit_xct());

    // After they obtained their fid, register managers
    _env->_ppart_man->register_table_man();
    _env->_psupplier_man->register_table_man();
    _env->_pdate_man->register_table_man();
    _env->_pcustomer_man->register_table_man();
    _env->_plineorder_man->register_table_man();



    // Do the baseline transaction
    populate_baseline_input_t in = {_sf, _loader_count, DIVISOR, 
                                    _lineorder_per_thread};

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


//static unsigned long part_completed = 0;
//static unsigned long cust_completed = 0;
static unsigned long lo_completed = 0;

void ShoreSSBEnv::table_builder_t::work() 
{
    w_rc_t e = RCOK;

    // 1. Load Lineorder 
    for(int i=_lineorder_start ; i < _lineorder_end; i+=LO_POP_UNIT) {
	while(_env->get_measure() != MST_MEASURE) {
	    usleep(1000);
	}

	long oid = i;
	populate_some_lineorders_input_t in = {oid};

        oid = std::min(_lineorder_end-i,LO_POP_UNIT);

	long log_space_needed = 0;
    retrypart:
	W_COERCE(_env->db()->begin_xct());
#ifdef USE_SHORE_6
	if(log_space_needed > 0) {
	    W_COERCE(_env->db()->xct_reserve_log_space(log_space_needed));
	}
#endif
        e = _env->xct_populate_some_lineorders(oid, in);

        CHECK_XCT_RETURN(e,log_space_needed,retrypart,_env);

	long nval = atomic_add_64_nv(&lo_completed,oid);

	if (nval % LINEORDER_COUNT == 0) {
            TRACE( TRACE_ALWAYS, "Lineorder %d/%d (%.0f%%)\n", 
                   nval, (int)(_sf*LINEORDER_UNIT_PER_SF),
                   (double)(100*nval)/(double)(_sf*LINEORDER_UNIT_PER_SF));
        }
    }
    
    TRACE( TRACE_ALWAYS, "Finished Lineorder %d .. %d \n", _lineorder_start, _lineorder_end);

    /*    // 2. Load Cust-related (~825MB)
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

        CHECK_XCT_RETURN(e,log_space_needed,retrycust);

	long nval = atomic_add_64_nv(&cust_completed,tid);
	if (nval % PART_COUNT == 0) {
            TRACE( TRACE_ALWAYS, "Customers %d/%d (%.0f%%)\n", 
                   nval, (int)(_sf*CUST_UNIT_PER_SF),
                   (double)(100*nval)/(double)(_sf*CUST_UNIT_PER_SF));
        }
    }
    
    TRACE( TRACE_ALWAYS, "Finished Custs %d .. %d \n", _cust_start, _cust_end);
    */
}




/******************************************************************** 
 *
 * ShoreSSBEnv functions
 *
 ********************************************************************/ 

ShoreSSBEnv::ShoreSSBEnv()
    : ShoreEnv()
{
    _scaling_factor = SSB_SCALING_FACTOR;

#ifdef CFG_QPIPE
    // Set the default scheduling policy. We will worry later about changing
    // that, possibly through the shell
    set_sched_policy(NULL);

    // Register stage containers
    register_stage_containers();
#endif
}


ShoreSSBEnv::~ShoreSSBEnv() 
{
}



w_rc_t ShoreSSBEnv::load_schema()
{
    // create the schema
    _ppart_desc      = new part_t(get_pd());
    _psupplier_desc  = new supplier_t(get_pd());
    _pdate_desc      = new date_t(get_pd());
    _pcustomer_desc  = new customer_t(get_pd());
    _plineorder_desc = new lineorder_t(get_pd());

    // initiate the table managers
    _ppart_man      = new part_man_impl(_ppart_desc.get());
    _psupplier_man  = new supplier_man_impl(_psupplier_desc.get());
    _pdate_man      = new date_man_impl(_pdate_desc.get());
    _pcustomer_man  = new customer_man_impl(_pcustomer_desc.get());
    _plineorder_man = new lineorder_man_impl(_plineorder_desc.get());
                
    return (RCOK);
}


#ifdef CFG_QPIPE

/******************************************************************** 
 *
 *  @fn:    {set,get}_sched_policy()
 *
 ********************************************************************/

policy_t* ShoreSSBEnv::get_sched_policy()
{
    CRITICAL_SECTION(init_cs,_load_mutex);
    return (_sched_policy.get());
}

policy_t* ShoreSSBEnv::set_sched_policy(const char* spolicy)
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

int ShoreSSBEnv::info() const
{
    TRACE( TRACE_ALWAYS, "SF      = (%.1f)\n", _scaling_factor);
    return (0);
}



/******************************************************************** 
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics for SSB
 *
 ********************************************************************/

int ShoreSSBEnv::statistics() 
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

int ShoreSSBEnv::start()
{
    return (ShoreEnv::start());
}

int ShoreSSBEnv::stop()
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
 * @brief: Loads the data for all the SSB tables, given the current
 *         scaling factor value. During the loading the SF cannot be
 *         changed.
 *
 ******************************************************************/

w_rc_t ShoreSSBEnv::loaddata() 
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
    ssb_dbgen_init();


    time_t tstart = time(NULL);

    /* partly (no) thanks to Shore's next key index locking, and
       partly due to page latch and SMO issues, we have ridiculous
       deadlock rates if we try to throw lots of threads at a small
       btree. To work around this we preload a number of records for
       each table and then fire up the (parallel) workers.
     */

    // 1. Read the number of parallel loaders and calculate ranges
    int loaders_to_use = envVar::instance()->getVarInt("db-loaders",10);
    //long total_parts = _scaling_factor*PART_UNIT_PER_SF;
    //long total_custs = _scaling_factor*CUST_UNIT_PER_SF;
    //long parts_per_thread = total_parts/loaders_to_use;
    //long custs_per_thread = total_custs/loaders_to_use;
    long total_lineorders = _scaling_factor*LINEORDER_UNIT_PER_SF;
    long lineorders_per_thread = total_lineorders/loaders_to_use;

    // 2. Fire up the table creator and baseline loader
    {
	guard<table_creator_t> tc;
	tc = new table_creator_t(this, _scaling_factor, loaders_to_use,
                                 lineorders_per_thread);
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
        	long lineorder_start = (i*lineorders_per_thread) + DIVISOR + 1;
        	long lineorder_end = ((i+1)*lineorders_per_thread > total_lineorders) ? 
                    total_lineorders : (i+1)*lineorders_per_thread;
        assert (lineorder_start <= lineorder_end);

        //	long cust_start = (i*custs_per_thread) + DIVISOR;
        //	long cust_end = ((i+1)*custs_per_thread > total_custs - 1) ? 
        //            total_custs : (i+1)*custs_per_thread - 1;
        //        assert (cust_start <= cust_end);

	loaders[i] = new table_builder_t(this, i, 
                                         lineorder_start, lineorder_end, 
                                         _scaling_factor);
	loaders[i]->fork();
    }

    for(int i=0; i < loaders_to_use; i++) {
	loaders[i]->join();
    }

    time_t tstop = time(NULL);

    // 5. Print stats
    TRACE( TRACE_STATISTICS, "Loading finished. %d tables loaded in (%d) secs...\n",
           SHORE_SSB_TABLES, (tstop - tstart));

    dbgenssb::free_asc_date();

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

w_rc_t ShoreSSBEnv::check_consistency()
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

w_rc_t ShoreSSBEnv::warmup()
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

int ShoreSSBEnv::dump()
{
    return (0);
}


int ShoreSSBEnv::conf()
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

int ShoreSSBEnv::post_init() 
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
 *  @brief: No contented tables in SSB
 *
 *********************************************************************/ 

w_rc_t 
ShoreSSBEnv::_post_init_impl() 
{
    TRACE( TRACE_DEBUG, "So far, nothing to pad in SSB..\n");
    return (RCOK);
}
  


EXIT_NAMESPACE(ssb);
