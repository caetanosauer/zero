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

/** @file:   shore_tpcb_env.cpp
 *
 *  @brief:  Declaration of the Shore TPC-C environment (database)
 *
 *  @author: Ryan Johnson      (ryanjohn)
 *  @author: Ippokratis Pandis (ipandis)
 *  @date:   Feb 2009
 */

#include "workload/tpcb/shore_tpcb_env.h"
#include "sm/shore/shore_helper_loader.h"

#include "k_defines.h"

using namespace shore;

ENTER_NAMESPACE(shore);
DEFINE_ROW_CACHE_TLS(tpcb, branch);
DEFINE_ROW_CACHE_TLS(tpcb, teller);
DEFINE_ROW_CACHE_TLS(tpcb, account);
DEFINE_ROW_CACHE_TLS(tpcb, history);
EXIT_NAMESPACE(shore);

ENTER_NAMESPACE(tpcb);


/******************************************************************** 
 *
 * ShoreTPCBEnv functions
 *
 ********************************************************************/ 

ShoreTPCBEnv::ShoreTPCBEnv()
    : ShoreEnv()
{
}

ShoreTPCBEnv::~ShoreTPCBEnv()
{
}



/******************************************************************** 
 *
 *  @fn:    load_schema()
 *
 *  @brief: Creates the table_desc_t and table_man_impl objects for 
 *          each TPC-B table
 *
 ********************************************************************/

w_rc_t ShoreTPCBEnv::load_schema()
{
    // create the schema
    _pbranch_desc   = new branch_t(get_pd());
    _pteller_desc   = new teller_t(get_pd());
    _paccount_desc  = new account_t(get_pd());
    _phistory_desc  = new history_t(get_pd());


    // initiate the table managers
    _pbranch_man   = new branch_man_impl(_pbranch_desc.get());
    _pteller_man   = new teller_man_impl(_pteller_desc.get());
    _paccount_man  = new account_man_impl(_paccount_desc.get());
    _phistory_man  = new history_man_impl(_phistory_desc.get());
        
    return (RCOK);
}



/******************************************************************** 
 *
 *  @fn:    update_partitioning()
 *
 *  @brief: Applies the baseline partitioning to the TPC-B tables
 *
 ********************************************************************/

w_rc_t ShoreTPCBEnv::update_partitioning() 
{
    // *** Reminder: The TPC-B records start their numbering from 0 ***

    // First configure
    conf();

    // Pulling this partitioning out of the thin air
    uint mrbtparts = envVar::instance()->getVarInt("mrbt-partitions",10);
    int minKeyVal = 0;
    int maxKeyVal = get_sf();

    char* minKey = (char*)malloc(sizeof(int));
    memset(minKey,0,sizeof(int));
    memcpy(minKey,&minKeyVal,sizeof(int));

    char* maxKey = (char*)malloc(sizeof(int));
    memset(maxKey,0,sizeof(int));
    memcpy(maxKey,&maxKeyVal,sizeof(int));

    // Branches: [ 0 .. #Branches )
    _pbranch_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),mrbtparts);

    // History: does not have account we use the same with Branches
    _phistory_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),mrbtparts);    

    // Tellers:  [ 0 .. (#Branches*TPCB_TELLERS_PER_BRANCH) )
    maxKeyVal = (get_sf()*TPCB_TELLERS_PER_BRANCH);
    memset(maxKey,0,sizeof(int));
    memcpy(maxKey,&maxKeyVal,sizeof(int));
    _pteller_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),mrbtparts);

    // Accounts: [ 0 .. (#Branches*TPCB_ACCOUNTS_PER_BRANCH) )
    maxKeyVal = (get_sf()*TPCB_ACCOUNTS_PER_BRANCH);
    memset(maxKey,0,sizeof(int));
    memcpy(maxKey,&maxKeyVal,sizeof(int));
    _paccount_desc->set_partitioning(minKey,sizeof(int),maxKey,sizeof(int),mrbtparts);

    free (minKey);
    free (maxKey);

    return (RCOK);
}



/******************************************************************** 
 *
 *  @fn:    set_skew()
 *
 *  @brief: sets load imbalance for TPC-B
 *
 ********************************************************************/
void ShoreTPCBEnv::set_skew(int area, int load, int start_imbalance) 
{
    ShoreEnv::set_skew(area, load, start_imbalance);
    // for branches
    b_skewer.set(area, 0, _scaling_factor-1, load);
    // for tellers
    t_skewer.set(area, 0, TPCB_TELLERS_PER_BRANCH-1, load);
    // for accounts
    a_skewer.set(area, 0, TPCB_ACCOUNTS_PER_BRANCH-1, load);
}


/******************************************************************** 
 *
 *  @fn:    start_load_imbalance()
 *
 *  @brief: sets the flag that triggers load imbalance for TPC-B
 *          resets the intervals if necessary (depending on the skew type)
 *
 ********************************************************************/
void ShoreTPCBEnv::start_load_imbalance() 
{
    if(b_skewer.is_used()) {
	_change_load = false;
	// for branches
	b_skewer.reset(_skew_type);
	// for tellers
	t_skewer.reset(_skew_type);
	// for accounts
	a_skewer.reset(_skew_type);
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
 *  @brief: sets the flag that stops the load imbalance for TPC-B
 *          and cleans the intervals
 *
 ********************************************************************/
void ShoreTPCBEnv::reset_skew() 
{
    ShoreEnv::reset_skew();
    _change_load = false;
    b_skewer.clear();
    t_skewer.clear();
    a_skewer.clear();
}


/******************************************************************** 
 *
 *  @fn:    info()
 *
 *  @brief: Prints information about the current db instance status
 *
 ********************************************************************/

int ShoreTPCBEnv::info() const
{
    TRACE( TRACE_ALWAYS, "SF      = (%.1f)\n", _scaling_factor);
    TRACE( TRACE_ALWAYS, "Workers = (%d)\n", _worker_cnt);
    return (0);
}



/******************************************************************** 
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics 
 *
 ********************************************************************/

int ShoreTPCBEnv::statistics() 
{
    // read the current trx statistics
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreTPCBTrxStats rval;
    rval -= rval; // dirty hack to set all zeros
    for (statmap_t::iterator it=_statmap.begin(); it != _statmap.end(); ++it) 
	rval += *it->second;

    TRACE( TRACE_STATISTICS, "AcctUpd. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.acct_update,
           rval.failed.acct_update,
           rval.deadlocked.acct_update);

    TRACE( TRACE_STATISTICS, "MbenchInsertOnly. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.mbench_insert_only,
           rval.failed.mbench_insert_only,
           rval.deadlocked.mbench_insert_only);

    TRACE( TRACE_STATISTICS, "MbenchDeleteOnly. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.mbench_delete_only,
           rval.failed.mbench_delete_only,
           rval.deadlocked.mbench_delete_only);

    TRACE( TRACE_STATISTICS, "MbenchProbeOnly. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.mbench_probe_only,
           rval.failed.mbench_probe_only,
           rval.deadlocked.mbench_probe_only);

    TRACE( TRACE_STATISTICS, "MbenchInsertDelte. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.mbench_insert_delete,
           rval.failed.mbench_insert_delete,
           rval.deadlocked.mbench_insert_delete);

    TRACE( TRACE_STATISTICS, "MbenchInsertProbe. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.mbench_insert_probe,
           rval.failed.mbench_insert_probe,
           rval.deadlocked.mbench_insert_probe);

    TRACE( TRACE_STATISTICS, "MbenchDeleteProbe. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.mbench_delete_probe,
           rval.failed.mbench_delete_probe,
           rval.deadlocked.mbench_delete_probe);

    TRACE( TRACE_STATISTICS, "MbenchMix. Att (%d). Abt (%d). Dld (%d)\n",
           rval.attempted.mbench_mix,
           rval.failed.mbench_mix,
           rval.deadlocked.mbench_mix);

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

int ShoreTPCBEnv::start()
{
    return (ShoreEnv::start());
}

int ShoreTPCBEnv::stop()
{
    return (ShoreEnv::stop());
}



/****************************************************************** 
 *
 * @class: checkpointer_t
 *
 * @brief: Checkpoints during the TPC-B db loading every 1 min.
 *
 ******************************************************************/

struct ShoreTPCBEnv::checkpointer_t : public thread_t 
{
    ShoreTPCBEnv* _env;
    checkpointer_t(ShoreTPCBEnv* env) 
        : thread_t("LDChkpt"), _env(env) 
    { }
    virtual void work();
};


void ShoreTPCBEnv::checkpointer_t::work() 
{
    bool volatile* loaded = &_env->_loaded;
    while(!*loaded) {
	_env->set_measure(MST_MEASURE);
	for(int i=0; i < 60 && !*loaded; i++) {
	    ::sleep(1);
        }
	
        TRACE( TRACE_ALWAYS, "db checkpoint - start\n");
        _env->checkpoint();
        TRACE( TRACE_ALWAYS, "db checkpoint - end\n");
    }
    _env->set_measure(MST_PAUSE);
}


/****************************************************************** 
 *
 * @class: table_builder_t
 *
 * @brief: Parallel workers for loading the TPC-B tables
 *
 ******************************************************************/

class ShoreTPCBEnv::table_builder_t : public thread_t 
{
    ShoreTPCBEnv* _env;
    int _sf;
    long _start;
    long _count;

public:
    table_builder_t(ShoreTPCBEnv* env, int id, int sf, long start, long count)
	: thread_t(c_str("LD-%d",id)), 
          _env(env), _sf(sf), _start(start), _count(count) 
    { }

    virtual void work();

}; // EOF: table_builder_t


const uint branchesPerRound = 5; // Update branch count every 5 rounds  
static uint volatile iBranchesLoaded = 0;
void ShoreTPCBEnv::table_builder_t::work() 
{
    w_rc_t e;

    for(int i=0; i < _count; i += TPCB_ACCOUNTS_CREATED_PER_POP_XCT) {
	long a_id = _start + i;
	populate_db_input_t in(_sf, a_id);
	long log_space_needed = 0;
    retry:
	W_COERCE(_env->db()->begin_xct());
#ifdef USE_SHORE_6
	if(log_space_needed > 0) {
	    W_COERCE(_env->db()->xct_reserve_log_space(log_space_needed));
	}
#endif
	e = _env->xct_populate_db(a_id, in);
        CHECK_XCT_RETURN(e,log_space_needed,retry,_env);

        if ((i % (branchesPerRound*TPCB_ACCOUNTS_PER_BRANCH)) == 0) {
            atomic_add_int(&iBranchesLoaded, branchesPerRound);
            TRACE(TRACE_ALWAYS, "%d branches loaded so far...\n",
                  iBranchesLoaded);
        }
    }
    TRACE( TRACE_STATISTICS, 
           "Finished loading account groups %ld .. %ld \n", 
           _start, _start+_count);
}



/****************************************************************** 
 *
 * @struct: table_creator_t
 *
 * @brief:  Helper class for creating the TPC-B tables and
 *          loading an initial number of records in a 
 *          single-threaded fashion
 *
 ******************************************************************/

struct ShoreTPCBEnv::table_creator_t : public thread_t 
{
    ShoreTPCBEnv* _env;
    int _sf;
    long _psize;
    long _pcount;
    table_creator_t(ShoreTPCBEnv* env, int sf, long psize, long pcount)
	: thread_t("CR"), _env(env), _sf(sf), _psize(psize), _pcount(pcount) { }
    virtual void work();

}; // EOF: table_creator_t


void ShoreTPCBEnv::table_creator_t::work() 
{
    // Create the tables, if any partitioning is to be applied, that has already
    // been set at update_partitioning()
    W_COERCE(_env->db()->begin_xct());
    W_COERCE(_env->_pbranch_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_pteller_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_paccount_desc->create_physical_table(_env->db()));
    W_COERCE(_env->_phistory_desc->create_physical_table(_env->db()));
    W_COERCE(_env->db()->commit_xct());

    // After they obtained their fid, register managers
    _env->_pbranch_man->register_table_man();
    _env->_pteller_man->register_table_man();
    _env->_paccount_man->register_table_man();
    _env->_phistory_man->register_table_man();
    
    // Create 10k accounts in each partition to buffer 
    // workers from each other
    for(long i=-1; i < _pcount; i++) {
	long a_id = i*_psize;
	populate_db_input_t in(_sf, a_id);
	TRACE( TRACE_STATISTICS, "Populating %ld a_ids starting with %ld\n", 
               TPCB_ACCOUNTS_CREATED_PER_POP_XCT, a_id);
	W_COERCE(_env->db()->begin_xct());
	W_COERCE(_env->xct_populate_db(a_id, in));
    }

    // Before returning, run the post initialization phase 
    W_COERCE(_env->db()->begin_xct());
    W_COERCE(_env->_post_init_impl());
    W_COERCE(_env->db()->commit_xct());
}


/********
 ******** Caution: The functions below should be invoked inside
 ********          the context of a smthread
 ********/


/****************************************************************** 
 *
 * @fn:    loaddata()
 *
 * @brief: Loads the data for all the TPCB tables, given the current
 *         scaling factor value. During the loading the SF cannot be
 *         changed.
 *
 ******************************************************************/

w_rc_t ShoreTPCBEnv::loaddata() 
{
    // 0. Lock the loading status and the scaling factor
    CRITICAL_SECTION(load_cs, _load_mutex);
    if (_loaded) {
        TRACE( TRACE_TRX_FLOW, 
               "Env already loaded. Doing nothing...\n");
        return (RCOK);
    }        
    CRITICAL_SECTION(scale_cs, _scaling_mutex);

    // 1. Create and fire up the checkpointing threads
    guard<checkpointer_t> chk(new checkpointer_t(this));
    chk->fork();
    
    /* partly (no) thanks to Shore's next key index locking, and
       partly due to page latch and SMO issues, we have ridiculous
       deadlock rates if we try to throw lots of threads at a small
       btree. To work around this we'll partition the space of
       accounts into LOADERS_TO_USE segments and have a single thread
       load the first 10k accounts from each partition before firing
       up the real workers.
     */
    int loaders_to_use = envVar::instance()->getVarInt("db-loaders",10);
    long total_accounts = _scaling_factor*TPCB_ACCOUNTS_PER_BRANCH;
    w_assert1((total_accounts % loaders_to_use) == 0);
    long accts_per_worker = total_accounts/loaders_to_use;

    // Adjust the number of loaders to use, if the scaling factor is very small
    // and the total_accounts < #loaders* accounts_per_branch
    if (_scaling_factor<loaders_to_use) loaders_to_use = _scaling_factor;
    
    time_t tstart = time(NULL);

    // 2. Create and fire up the table creator which will also start the loading
    {
	guard<table_creator_t> tc;
	tc = new table_creator_t(this, _scaling_factor, 
                                 accts_per_worker, loaders_to_use);
	tc->fork();
	tc->join();
    }


    // 3. Create and file up the loading workers 

    /* This number is really flexible. Basically, it just needs to be
       high enough to give good parallelism, while remaining low
       enough not to cause too much contention. I pulled '40' out of
       thin air.
     */
    array_guard_t< guard<table_builder_t> > loaders(new guard<table_builder_t>[loaders_to_use]);
    for(int i=0; i < loaders_to_use; i++) {
	// the preloader thread picked up that first set of accounts...
	long start = accts_per_worker*i+TPCB_ACCOUNTS_CREATED_PER_POP_XCT;
	long count = accts_per_worker-TPCB_ACCOUNTS_CREATED_PER_POP_XCT;
	loaders[i] = new table_builder_t(this, i, _scaling_factor, start, count);
	loaders[i]->fork();
    }

    // 4. Join the loading threads    
    for(int i=0; i<loaders_to_use; i++) {
	loaders[i]->join();        
    }

    time_t tstop = time(NULL);

    // 5. Print stats
    TRACE( TRACE_STATISTICS, "Loading finished. %.1f branches loaded in (%d) secs...\n",
           _scaling_factor, (tstop - tstart));

    // 6. notify that the env is loaded
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

w_rc_t ShoreTPCBEnv::check_consistency()
{
    // not loaded from files, so no inconsistency possible
    return RCOK;
}


/****************************************************************** 
 *
 * @fn:    warmup()
 *
 * @brief: Touches the entire database - For memory-fitting databases
 *         this is enough to bring it to load it to memory
 *
 ******************************************************************/

w_rc_t ShoreTPCBEnv::warmup()
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

int ShoreTPCBEnv::dump()
{
    assert (0); // IP: not implemented yet

//     table_man_t* ptable_man = NULL;
//     for(table_man_list_iter table_man_iter = _table_man_list.begin(); 
//         table_man_iter != _table_man_list.end(); table_man_iter++)
//         {
//             ptable_man = *table_man_iter;
//             ptable_man->print_table(this->_pssm);
//         }

    return (0);
}


int ShoreTPCBEnv::conf()
{
    // reread the params
    ShoreEnv::conf();
    upd_sf();
    upd_worker_cnt();
    return (0);
}


/********************************************************************
 *
 * Make sure the very contented tables are padded to one record per page
 *
 *********************************************************************/

int ShoreTPCBEnv::post_init() 
{
    conf();

    // If the database is set to be padded
    if (get_pd() & PD_PADDED) {
        TRACE( TRACE_ALWAYS, "Checking for BRANCH/TELLER record padding...\n");

        W_COERCE(db()->begin_xct());
        w_rc_t rc = _post_init_impl();
        if(rc.is_error()) {
            cerr << "-> BRANCH/TELLER padding failed with: " << rc << endl;
            rc = db()->abort_xct();
            return (rc.err_num());
        }
        else {
            TRACE( TRACE_ALWAYS, "-> Done\n");
            rc = db()->commit_xct();
        }
    }
    return (0);
}


/********************************************************************* 
 *
 *  @fn:    _post_init_impl
 *
 *  @brief: Makes sure the BRANCHES and TELLERS tables for TPC-B 
 *          are padded to one record per page. Otherwise, we observe
 *          contention for page latches.
 *
 *********************************************************************/ 

w_rc_t ShoreTPCBEnv::_post_init_impl() 
{
#ifdef CFG_HACK
    TRACE (TRACE_ALWAYS, "Padding BRANCHES and TELLERS\n");
    //#warning IP - Adding padding also for the TPC-B TELLERS table
    W_DO(_pad_BRANCHES());
    W_DO(_pad_TELLERS());
#endif    
    return (RCOK);
}


/********************************************************************* 
 *
 *  @fn:    _pad_BRANCHES
 *
 *  @brief: Pads the BRANCHES records.
 *
 *********************************************************************/ 

w_rc_t ShoreTPCBEnv::_pad_BRANCHES()
{
    ss_m* db = this->db();
    
    // lock the BRANCHES table    
    branch_t* br = branch_desc();
    index_desc_t* br_idx = br->indexes();
    int br_idx_count = br->index_count();
    W_DO(br->find_fid(db));
    stid_t br_fid = br->fid();

    // lock the table and index(es) for exclusive access
    W_DO(db->lock(br_fid, EX));
    for(int i=0; i < br_idx_count; i++) {
	W_DO(br_idx[i].check_fid(db));
	for(int j=0; j < br_idx[i].get_partition_count(); j++) {
	    W_DO(db->lock(br_idx[i].fid(j), EX));
        }
    }

    guard<ats_char_t> pts = new ats_char_t(br->maxsize());
    
    // copy and pad all tuples smaller than 4k

    // WARNING: this code assumes that existing tuples are packed
    // densly so that all padded tuples are added after the last
    // unpadded one
    
    bool eof;
    
    // we know you can't fit two 4k records on a single page
    static int const PADDED_SIZE = 4096;
    
    array_guard_t<char> padding = new char[PADDED_SIZE];
    std::vector<rid_t> hit_list;
    {
	guard<branch_man_impl::table_iter> iter;
	{
	    branch_man_impl::table_iter* tmp;
	    W_DO(branch_man()->get_iter_for_file_scan(db, tmp));
	    iter = tmp;
	}

	int count = 0;
	branch_man_impl::table_tuple row(br);
	rep_row_t arep(pts);
	int psize = br->maxsize()+1;

	W_DO(iter->next(db, eof, row));	
	while (1) {
	    pin_i* handle = iter->cursor();
	    if (!handle) {
		TRACE(TRACE_ALWAYS, 
                      "-> Reached EOF. Search complete (%d)\n", 
                      count);
		break;
	    }

	    // figure out how big the old record is
	    int hsize = handle->hdr_size();
	    int bsize = handle->body_size();
	    if (bsize == psize) {
		TRACE(TRACE_ALWAYS, 
                      "-> Found padded BRANCH record. Stopping search (%d)\n", 
                      count);
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
		W_DO(db->create_rec(br_fid, hvec, PADDED_SIZE, dvec, new_rid));
		W_DO(db->append_rec(new_rid, pvec
#ifndef CFG_SHORE_6
                                    , false
#endif
                                    ));

                // mark the old record for deletion
		hit_list.push_back(handle->rid());

		// update the index(es)
		vec_t rvec(&row._rid, sizeof(rid_t));
		vec_t nrvec(&new_rid, sizeof(new_rid));
		for(int i=0; i < br_idx_count; i++) {
		    int key_sz = branch_man()->format_key(br_idx+i, &row, arep);
		    vec_t kvec(arep._dest, key_sz);

		    // destroy the old mapping and replace it with the new
                    // one.  If it turns out this is super-slow, we can
                    // look into probing the index with a cursor and
                    // updating it directly.
		    int pnum = _pbranch_man->get_pnum(&br_idx[i], &row);
		    stid_t fid = br_idx[i].fid(pnum);

		    if(br_idx[i].is_mr()) {
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
        TRACE(TRACE_ALWAYS, "padded records added\n");

	// put the iter out of scope
    }

    // delete the old records     
    int hlsize = hit_list.size();
    TRACE(TRACE_ALWAYS, 
          "-> Deleting (%d) old BRANCH unpadded records\n", 
          hlsize);
    for(int i=0; i < hlsize; i++) {
	W_DO(db->destroy_rec(hit_list[i]));
    }

    return (RCOK);
}
  


/********************************************************************* 
 *
 *  @fn:    _pad_TELLERS
 *
 *  @brief: Pads the TELLERS records.
 *
 *********************************************************************/ 

w_rc_t ShoreTPCBEnv::_pad_TELLERS()
{
    ss_m* db = this->db();

    // lock the TELLERS table    
    teller_t* te = teller_desc();
    index_desc_t* te_idx = te->indexes();
    int te_idx_count = te->index_count();
    W_DO(te->find_fid(db));
    stid_t te_fid = te->fid();

    // lock the table and index(es) for exclusive access
    W_DO(db->lock(te_fid, EX));
    for(int i=0; i < te_idx_count; i++) {
	W_DO(te_idx[i].check_fid(db));
	for(int j=0; j < te_idx[i].get_partition_count(); j++)
	    W_DO(db->lock(te_idx[i].fid(j), EX));
    }

    guard<ats_char_t> pts = new ats_char_t(te->maxsize());
    
    // copy and pad all tuples smaller than 4k

    // WARNING: this code assumes that existing tuples are packed
    // densly so that all padded tuples are added after the last
    // unpadded one
    
    bool eof;
    
    // we know you can't fit two 4k records on a single page
    static int const PADDED_SIZE = 4096;
    
    array_guard_t<char> padding = new char[PADDED_SIZE];
    std::vector<rid_t> hit_list;
    {
	guard<teller_man_impl::table_iter> iter;
	{
	    teller_man_impl::table_iter* tmp;
	    W_DO(teller_man()->get_iter_for_file_scan(db, tmp));
	    iter = tmp;
	}

	int count = 0;
	teller_man_impl::table_tuple row(te);
	rep_row_t arep(pts);
	int psize = te->maxsize()+1;

	W_DO(iter->next(db, eof, row));	
	while (1) {
	    pin_i* handle = iter->cursor();
	    if (!handle) {
		TRACE(TRACE_ALWAYS, 
                      "-> Reached EOF. Search complete (%d)\n", 
                      count);
		break;
	    }

	    // figure out how big the old record is
	    int hsize = handle->hdr_size();
	    int bsize = handle->body_size();
	    if (bsize == psize) {
		TRACE(TRACE_ALWAYS, 
                      "-> Found padded TELLER record. Stopping search (%d)\n", 
                      count);
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
		W_DO(db->create_rec(te_fid, hvec, PADDED_SIZE, dvec, new_rid));
		W_DO(db->append_rec(new_rid, pvec
#ifndef CFG_SHORE_6
                                    , false
#endif
                                    ));

                // mark the old record for deletion
		hit_list.push_back(handle->rid());

		// update the index(es)
		vec_t rvec(&row._rid, sizeof(rid_t));
		vec_t nrvec(&new_rid, sizeof(new_rid));
		for(int i=0; i < te_idx_count; i++) {
		    int key_sz = teller_man()->format_key(te_idx+i, &row, arep);
		    vec_t kvec(arep._dest, key_sz);

		    // destroy the old mapping and replace it with the new
                    // one.  If it turns out this is super-slow, we can
                    // look into probing the index with a cursor and
                    // updating it directly.
		    int pnum = _pteller_man->get_pnum(&te_idx[i], &row);
		    stid_t fid = te_idx[i].fid(pnum);

		    if(te_idx[i].is_mr()) {
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
        TRACE(TRACE_ALWAYS, "padded records added\n");

	// put the iter out of scope
    }

    // delete the old records     
    int hlsize = hit_list.size();
    TRACE(TRACE_ALWAYS, 
          "-> Deleting (%d) old TELLER unpadded records\n", 
          hlsize);
    for(int i=0; i < hlsize; i++) {
	W_DO(db->destroy_rec(hit_list[i]));
    }

    return (RCOK);
}
  

/********************************************************************* 
 *
 *  @fn:   db_print
 *
 *  @brief: Prints the current tpcb tables to files
 *
 *********************************************************************/ 

w_rc_t ShoreTPCBEnv::db_print(int lines) 
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // print tables
    W_DO(_pbranch_man->print_table(_pssm, lines));
    W_DO(_pteller_man->print_table(_pssm, lines));    
    W_DO(_paccount_man->print_table(_pssm, lines));
    W_DO(_phistory_man->print_table(_pssm, lines));    

    return (RCOK);
}


/********************************************************************* 
 *
 *  @fn:   db_fetch
 *
 *  @brief: Fetches the current tpcb tables to buffer pool
 *
 *********************************************************************/ 

w_rc_t ShoreTPCBEnv::db_fetch() 
{
    // ensure a valid environment
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    // fetch tables
    W_DO(_pbranch_man->fetch_table(_pssm));
    W_DO(_pteller_man->fetch_table(_pssm));    
    W_DO(_paccount_man->fetch_table(_pssm));
    W_DO(_phistory_man->fetch_table(_pssm));    

    return (RCOK);
}


EXIT_NAMESPACE(tpcb);
