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

/** @file:   shore_tm1_env.cpp
 *
 *  @brief:  Declaration of the Shore TM1 environment (database)
 *
 *  @author: Ippokratis Pandis, Feb 2009
 */

#include "workload/tm1/shore_tm1_env.h"
#include "sm/shore/shore_helper_loader.h"

using namespace shore;


ENTER_NAMESPACE(shore);
DEFINE_ROW_CACHE_TLS(tm1, sub);
DEFINE_ROW_CACHE_TLS(tm1, ai);
DEFINE_ROW_CACHE_TLS(tm1, sf);
DEFINE_ROW_CACHE_TLS(tm1, cf);
EXIT_NAMESPACE(shore);


ENTER_NAMESPACE(tm1);


/******************************************************************** 
 *
 * ShoreTM1Env functions
 *
 ********************************************************************/ 

ShoreTM1Env::ShoreTM1Env() 
    : ShoreEnv()
{ 
    _scaling_factor = TM1_DEF_SF;
    _queried_factor = TM1_DEF_QF;
}

ShoreTM1Env::~ShoreTM1Env()
{
}
    

/******************************************************************** 
 *
 *  @fn:    load_schema()
 *
 *  @brief: Creates the table_desc_t and table_man_impl objects for 
 *          each TM1 table
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::load_schema()
{
    // create the schema
    _psub_desc  = new subscriber_t(get_pd());
    _pai_desc   = new access_info_t(get_pd());
    _psf_desc   = new special_facility_t(get_pd());
    _pcf_desc   = new call_forwarding_t(get_pd());

    // initiate the table managers
    _psub_man = new sub_man_impl(_psub_desc.get());
    _pai_man  = new ai_man_impl(_pai_desc.get());
    _psf_man  = new sf_man_impl(_psf_desc.get());
    _pcf_man  = new cf_man_impl(_pcf_desc.get());   
        
    return (RCOK);
}


/******************************************************************** 
 *
 *  @fn:    update_partitioning()
 *
 *  @brief: Applies the baseline partitioning to the TM1 tables
 *
 ********************************************************************/

w_rc_t ShoreTM1Env::update_partitioning() 
{
    // *** Reminder: The numbering in TM1 starts from 1. 

    // First configure
    conf();

    // Pulling this partitioning out of the thin air
    uint mrbtparts = envVar::instance()->getVarInt("mrbt-partitions",10);
    int minKeyVal = 1;
    int maxKeyVal = (get_sf()*TM1_SUBS_PER_SF)+1;

    char* minKey = (char*)malloc(sizeof(int));
    memset(minKey,0,sizeof(int));
    memcpy(minKey,&minKeyVal,sizeof(int));

    char* maxKey = (char*)malloc(sizeof(int));
    memset(maxKey,0,sizeof(int));
    memcpy(maxKey,&maxKeyVal,sizeof(int));

    // All the TM1 tables use the SUB_ID as the first column
    _psub_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),mrbtparts);
    _pai_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),mrbtparts);
    _psf_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),mrbtparts);
    _pcf_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),mrbtparts);

    free (minKey);
    free (maxKey);

    return (RCOK);
}



/******************************************************************** 
 *
 *  @fn:    start/stop
 *
 *  @brief: Simply call the corresponding functions of shore_env 
 *
 ********************************************************************/

int ShoreTM1Env::start()
{
    return (ShoreEnv::start());
}

int ShoreTM1Env::stop()
{
    return (ShoreEnv::stop());
}


/******************************************************************** 
 *
 *  @fn:    set_skew()
 *
 *  @brief: sets load imbalance for TM-1
 *
 ********************************************************************/
void ShoreTM1Env::set_skew(int area, int load, int start_imbalance) 
{
    ShoreEnv::set_skew(area, load, start_imbalance);
    // for subscribers
    s_skewer.set(area, 1, _scaling_factor * TM1_SUBS_PER_SF, load);
}


/******************************************************************** 
 *
 *  @fn:    start_load_imbalance()
 *
 *  @brief: sets the flag that triggers load imbalance for TM1
 *          resets the intervals if necessary (depending on the skew type)
 *
 ********************************************************************/
void ShoreTM1Env::start_load_imbalance() 
{
    if(s_skewer.is_used()) {
	_change_load = false;
	// for subscribers
	s_skewer.reset(_skew_type);
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
 *  @brief: sets the flag that stops the load imbalance for TM1
 *          and cleans the intervals
 *
 ********************************************************************/
void ShoreTM1Env::reset_skew() 
{
    ShoreEnv::reset_skew();
    _change_load = false;
    s_skewer.clear();
}


/******************************************************************** 
 *
 *  @fn:    info()
 *
 *  @brief: Prints information about the current db instance status
 *
 ********************************************************************/

int ShoreTM1Env::info() const
{
    TRACE( TRACE_ALWAYS, "SF      = (%.1f)\n", _scaling_factor);
    TRACE( TRACE_ALWAYS, "Workers = (%d)\n", _worker_cnt);
    return (0);
}



/******************************************************************** 
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics for TM1 
 *
 ********************************************************************/

int ShoreTM1Env::statistics() 
{
    // read the current trx statistics
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreTM1TrxStats rval;
    rval -= rval; // dirty hack to set all zeros
    for (statmap_t::iterator it=_statmap.begin(); it != _statmap.end(); ++it) 
	rval += *it->second;

    TRACE( TRACE_STATISTICS, "GebSubData. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.get_sub_data,
           rval.failed.get_sub_data,
           rval.deadlocked.get_sub_data);

    TRACE( TRACE_STATISTICS, "GebNewDest. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.get_new_dest,
           rval.failed.get_new_dest,
           rval.deadlocked.get_new_dest);

    TRACE( TRACE_STATISTICS, "GebAccData. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.get_acc_data,
           rval.failed.get_acc_data,
           rval.deadlocked.get_acc_data);

    TRACE( TRACE_STATISTICS, "UpdSubData. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.upd_sub_data,
           rval.failed.upd_sub_data,
           rval.deadlocked.upd_sub_data);

    TRACE( TRACE_STATISTICS, "UpdLocation. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.upd_loc,
           rval.failed.upd_loc,
           rval.deadlocked.upd_loc);

    TRACE( TRACE_STATISTICS, "InsCallFwd. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.ins_call_fwd,
           rval.failed.ins_call_fwd,
           rval.deadlocked.ins_call_fwd);

    TRACE( TRACE_STATISTICS, "DelCallFwd. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.del_call_fwd,
           rval.failed.del_call_fwd,
           rval.deadlocked.del_call_fwd);

    TRACE( TRACE_STATISTICS, "GetSubNbr. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.get_sub_nbr,
           rval.failed.get_sub_nbr,
           rval.deadlocked.get_sub_nbr);

    TRACE( TRACE_STATISTICS, "InsCallFwdBench. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.ins_call_fwd_bench,
           rval.failed.ins_call_fwd_bench,
           rval.deadlocked.ins_call_fwd_bench);

    TRACE( TRACE_STATISTICS, "DelCallFwdBench. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.del_call_fwd_bench,
           rval.failed.del_call_fwd_bench,
           rval.deadlocked.del_call_fwd_bench);

    ShoreEnv::statistics();

    return (0);
}




/****************************************************************** 
 *
 * @struct: table_creator_t
 *
 * @brief:  Helper class for creating the environment tables and
 *          loading a number of records in a single-threaded fashion
 *
 ******************************************************************/

struct ShoreTM1Env::table_creator_t : public thread_t 
{
    ShoreTM1Env* _env;
    int _loaders;
    int _subs_per_worker;
    int _preloads_per_worker;

    table_creator_t(ShoreTM1Env* env, 
                    const int loaders, const int subs_per_worker, const int preloads_per_worker)
	: thread_t("CR"), _env(env),
          _loaders(loaders),_subs_per_worker(subs_per_worker),_preloads_per_worker(preloads_per_worker)
    { 
        assert (loaders);
        assert (subs_per_worker);
        assert (preloads_per_worker>=0);
        assert (subs_per_worker>=preloads_per_worker);
    }
    virtual void work();

}; // EOF: ShoreTM1Env::table_creator_t


void  ShoreTM1Env::table_creator_t::work() 
{
    // Create the tables
    W_COERCE(_env->db()->begin_xct());
    W_COERCE(_env->_psub_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pai_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_psf_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pcf_desc->create_physical_table(_env->db()));
    W_COERCE(_env->db()->commit_xct());    

    // After they obtained their fid, register managers
    _env->_psub_man->register_table_man();
    _env->_pai_man->register_table_man();
    _env->_psf_man->register_table_man();
    _env->_pcf_man->register_table_man();


    // Preload (preloads_per_worker) records for each of the loaders
    int sub_id = 0;
    for (int i=0; i<_loaders; i++) {
        W_COERCE(_env->db()->begin_xct());        
        TRACE( TRACE_ALWAYS, "Preloading (%d). Start (%d). Todo (%d)\n", 
               i, (i*_subs_per_worker), _preloads_per_worker);
            
        for (int j=0; j<_preloads_per_worker; j++) {
            sub_id = i*_subs_per_worker + j;
            W_COERCE(_env->xct_populate_one(sub_id+1));
        }
        W_COERCE(_env->db()->commit_xct());
    }
}


/****************************************************************** 
 *
 * @class: table_builder_t
 *
 * @brief:  Helper class for loading the environment tables
 *
 ******************************************************************/

class ShoreTM1Env::table_builder_t : public thread_t 
{
    ShoreTM1Env* _env;
    int _loader_id;
    int _start;
    int _count;
public:
    table_builder_t(ShoreTM1Env* env, int loaderid, int start, int count)
	: thread_t(c_str("LD-%d",loaderid)), 
          _env(env), _loader_id(loaderid), _start(start), _count(count) 
    { }
    virtual void work();

}; // EOF: ShoreTM1Env::table_builder_t


void ShoreTM1Env::table_builder_t::work() 
{
    assert (_count>=0);
    
    w_rc_t e = RCOK;
    int commitmark = 0;
    int tracemark = 0;
    int subsadded = 0;

    W_COERCE(_env->db()->begin_xct());

    // add _count number of Subscribers (with the corresponding AI, SF, and CF entries)
    int last_commit = 0;
    for (subsadded=0; subsadded<_count; ++subsadded) {
    again:
	int sub_id = _start + subsadded;

        // insert row
	e = _env->xct_populate_one(sub_id+1);

	if(e.is_error()) {
	    W_COERCE(_env->db()->abort_xct());
	    if(e.err_num() == smlevel_0::eDEADLOCK) {
		W_COERCE(_env->db()->begin_xct());
		subsadded = last_commit + 1;
		goto again;
	    }
	    stringstream os;
	    os << e << ends;
	    string str = os.str();
	    TRACE( TRACE_ALWAYS, "Unable to Insert Subscriber (%d) due to:\n%s\n",
                   sub_id, str.c_str());
	}

        // Output some information
        if (subsadded>=tracemark) {
            TRACE( TRACE_ALWAYS, "Start (%d). Todo (%d). Added (%d)\n", 
                   _start, _count, subsadded);
            tracemark += TM1_LOADING_TRACE_INTERVAL;
        }

        // Commit every now and then
        if (subsadded>=commitmark) {
            e = _env->db()->commit_xct();

            if (e.is_error()) {
                stringstream os;
                os << e << ends;
                string str = os.str();
                TRACE( TRACE_ALWAYS, "Unable to Commit (%d) due to:\n%s\n",
                       sub_id, str.c_str());
	    
                w_rc_t e2 = _env->db()->abort_xct();
                if(e2.is_error()) {
                    TRACE( TRACE_ALWAYS, 
                           "Unable to abort trx for Subscriber (%d) due to [0x%x]\n", 
                           sub_id, e2.err_num());
                }
            }

	    last_commit = subsadded;
            commitmark += TM1_LOADING_COMMIT_INTERVAL;

            W_COERCE(_env->db()->begin_xct());
        }
    }

    // final commit
    e = _env->db()->commit_xct();

    if (e.is_error()) {
        stringstream os;
        os << e << ends;
        string str = os.str();
        TRACE( TRACE_ALWAYS, "Unable to final Commit due to:\n%s\n",
               str.c_str());
	    
        w_rc_t e2 = _env->db()->abort_xct();
        if(e2.is_error()) {
            TRACE( TRACE_ALWAYS, "Unable to abort trx due to [0x%x]\n", 
                   e2.err_num());
        }
    }    
}


/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/****************************************************************** 
 *
 * @fn:    loaddata()
 *
 * @brief: Loads the data for all the TM1 tables, given the current
 *         scaling factor value. During the loading the SF cannot be
 *         changed.
 *
 ******************************************************************/

w_rc_t ShoreTM1Env::loaddata() 
{
    // 0. lock the loading status
    CRITICAL_SECTION(load_cs, _load_mutex);
    if (_loaded) {
        TRACE( TRACE_TRX_FLOW, 
               "Env already loaded. Doing nothing...\n");
        return (RCOK);
    }        


    // 1. Create tables and load a number of records to each table

    /* partly (no) thanks to Shore's next key index locking, and
       partly due to page latch and SMO issues, we have ridiculous
       deadlock rates if we try to throw lots of threads at a small
       btree. To work around this we'll partition the space of
       subscribers into TM1_LOADERS_TO_USE segments and have a single 
       thread load the first TM1_SUBS_TO_PRELOAD (2k) subscribers 
       from each partition before firing up the real workers.
     */
    
    int loaders_to_use = envVar::instance()->getVarInt("db-loaders",TM1_LOADERS_TO_USE);
    int creator_loaders_to_use = loaders_to_use;

    int total_subs = _scaling_factor*TM1_SUBS_PER_SF;
    assert ((total_subs % loaders_to_use) == 0);
   
    int subs_per_worker = total_subs/loaders_to_use; 
    int preloads_per_worker = envVar::instance()->getVarInt("db-record-preloads",TM1_SUBS_TO_PRELOAD);
    
    // Special case for very small databases where the preloads is larger than 
    // the total subs per worker. In that case the table creator does all the work
    // (no parallel loaders) 
    if (subs_per_worker<preloads_per_worker) { 
        preloads_per_worker = subs_per_worker;
        loaders_to_use = 0;
    }

    time_t tstart = time(NULL);

    {
	guard<table_creator_t> tc;
	tc = new table_creator_t(this, creator_loaders_to_use, subs_per_worker, preloads_per_worker);
	tc->fork();
	tc->join();
    }


    // 2. Fire up the loader threads
    
    /* This number is really flexible. Basically, it just needs to be
       high enough to give good parallelism, while remaining low
       enough not to cause too much contention. Ryan pulled '40' out of
       thin air.
     */

    array_guard_t< guard<table_builder_t> > loaders(new guard<table_builder_t>[loaders_to_use]);
    for (int i=0; i<loaders_to_use; i++) {
	// the preloader thread picked up a first set of subscribers...
	int start = i*subs_per_worker + preloads_per_worker;
	int count = subs_per_worker - preloads_per_worker;
	loaders[i] = new table_builder_t(this, i, start, count);
	loaders[i]->fork();
    }
    
    for(int i=0; i<loaders_to_use; i++) {
	loaders[i]->join();        
    }


    // 3. Join the loading threads
    time_t tstop = time(NULL);

    // 4. Print stats
    TRACE( TRACE_STATISTICS, "Loading finished. %d subscribers loaded in (%d) secs...\n",
           total_subs, (tstop - tstart));

    // 5. Notify that the env is loaded
    _loaded = true;

    return (RCOK);
}


/****************************************************************** 
 *
 * @fn:    conf()
 *
 ******************************************************************/

int ShoreTM1Env::conf()
{
    // reread the params
    ShoreEnv::conf();
    upd_sf();
    upd_worker_cnt();
    return (0);
}



/********************************************************************* 
 *
 *  @fn:    _post_init_impl
 *
 *********************************************************************/ 

int ShoreTM1Env::post_init() 
{
    return (0);
}


w_rc_t ShoreTM1Env::_post_init_impl() 
{
    TRACE(TRACE_DEBUG, "Doing nothing\n");
    return (RCOK);
}


/********************************************************************* 
 *
 *  @fn:   db_print
 *
 *  @brief: Prints the current tm1 tables to files
 *
 *********************************************************************/ 

w_rc_t ShoreTM1Env::db_print(int lines) 
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // print tables
    W_DO(_psub_man->print_table(_pssm, lines));
    W_DO(_pai_man->print_table(_pssm, lines));    
    W_DO(_psf_man->print_table(_pssm, lines));
    W_DO(_pcf_man->print_table(_pssm, lines));    

    return (RCOK);
}


/********************************************************************* 
 *
 *  @fn:   db_fetch
 *
 *  @brief: Fetches the current tm1 tables to buffer pool
 *
 *********************************************************************/ 

w_rc_t ShoreTM1Env::db_fetch() 
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // fetch tables
    W_DO(_psub_man->fetch_table(_pssm));
    W_DO(_pai_man->fetch_table(_pssm));    
    W_DO(_psf_man->fetch_table(_pssm));
    W_DO(_pcf_man->fetch_table(_pssm));    

    return (RCOK);
}


EXIT_NAMESPACE(tm1);
