#include "ycsb.h"

#include "trx_worker.h"

DEFINE_ROW_CACHE_TLS(ycsb, ycsbtable);

namespace ycsb {

// related to dynamic skew for load imbalance
skewer_t y_skewer;
bool _change_load = false;

// SF=1 should be around 100MB, i.e., 100 thousand records of 1KB
constexpr unsigned RecordsPerSF = 100'000;
// Records inserted on each populate transaction
constexpr unsigned RecordsPerPopXct = 1000;
static_assert(RecordsPerSF % RecordsPerPopXct == 0);

// Thread-local Env stats
static thread_local ShoreYCSBTrxStats my_stats;

// Only one type of transaction so far
const int XCT_YCSB_SIMPLE = 99;

// This is similar to get_wh() used in tpcc_input.cpp
int get_key(int sf, int specificPrefix, int tspread)
{
    // 10-byte key has 2 parts: prefix and user
    // - Prefix (2 bytes) is determined by the sf, like the warehouse of TPC-C
    // - User (8 bytes) is picked randomly with a uniform distribution
    // Skew only applies to the prefix. A "90-10" type of skew should use at least SF=10.
    // Within a prefix, static chunks are partitioned to worker threads just like the
    // warehouses in TPC-C, according to the tspread argument.
    int prefix = _change_load ? y_skewer.get_input() : URand(1, sf);
    if (specificPrefix > 0) {
        // If prefix is given, it is essentially the worker thread number
        w_assert1(tspread > 0);
        w_assert1(specificPrefix <= tspread);
        prefix = (prefix / tspread) * tspread + specificPrefix;
        if (prefix > sf) { prefix -= tspread; }
        w_assert1(prefix <= sf);
        w_assert1(prefix > 0);
    }
    w_assert1(prefix < std::numeric_limits<uint16_t>::max());
    int user = URand(0, RecordsPerSF-1);
    uint64_t key = (static_cast<uint64_t>(prefix) << 48) | static_cast<uint64_t>(user);
    // cout << "probing prefix " << prefix << " user " << user << " key " << key <<endl;
    return key;
}

read_input_t create_read_input(int sf, int specificPrefix, int tspread)
{
    read_input_t input;
    input.key = get_key(sf, specificPrefix, tspread);
    return input;
}

update_input_t create_update_input(int sf, int specificPrefix, int tspread)
{
    update_input_t input;
    input.key = get_key(sf, specificPrefix, tspread);
    input.field_number = URand(1, 10);
    fill_value(input.value);
    return input;
}

ycsbtable_t::ycsbtable_t(const uint32_t& pd)
    : table_desc_t("YCSBTABLE", 11, pd)
{
     _desc[0].setup(SQL_LONG, "KEY");
     _desc[1].setup(SQL_FIXCHAR, "FIELD1", FieldSize);
     _desc[2].setup(SQL_FIXCHAR, "FIELD2", FieldSize);
     _desc[3].setup(SQL_FIXCHAR, "FIELD3", FieldSize);
     _desc[4].setup(SQL_FIXCHAR, "FIELD4", FieldSize);
     _desc[5].setup(SQL_FIXCHAR, "FIELD5", FieldSize);
     _desc[6].setup(SQL_FIXCHAR, "FIELD6", FieldSize);
     _desc[7].setup(SQL_FIXCHAR, "FIELD7", FieldSize);
     _desc[8].setup(SQL_FIXCHAR, "FIELD8", FieldSize);
     _desc[9].setup(SQL_FIXCHAR, "FIELD9", FieldSize);
     _desc[10].setup(SQL_FIXCHAR, "FIELD10", FieldSize);

     uint keys[1] = {0};
     create_primary_idx_desc(keys, 1, pd);
}

rc_t ycsbtable_man_impl::index_probe(Database* db, table_row_t* ptuple, const uint64_t id)
{
    assert (ptuple);
    ptuple->set_value(0, id);
    return (index_probe_by_name(db, "YCSBTABLE", ptuple));
}

rc_t ycsbtable_man_impl::index_probe_forupdate(Database* db, table_row_t* ptuple, const uint64_t id)
{
    assert (ptuple);
    ptuple->set_value(0, id);
    return (index_probe_forupdate_by_name(db, "YCSBTABLE", ptuple));
}

ShoreYCSBEnv::ShoreYCSBEnv(boost::program_options::variables_map vm)
    : ShoreEnv(vm)
{
}

ShoreYCSBEnv::~ShoreYCSBEnv()
{
}

rc_t ShoreYCSBEnv::load_schema()
{
    ycsbtable_man   = new ycsbtable_man_impl(new ycsbtable_t(get_pd()));
    return (RCOK);
}

rc_t ShoreYCSBEnv::load_and_register_fids()
{
    W_DO(ycsbtable_man->load_and_register_fid(db()));
    return (RCOK);
}

void ShoreYCSBEnv::set_skew(int area, int load, int start_imbalance, int skew_type, bool shifting)
{
    ShoreEnv::set_skew(area, load, start_imbalance, skew_type);
    y_skewer.set(area, 0, _scaling_factor-1, load, shifting);
}

void ShoreYCSBEnv::start_load_imbalance()
{
    if(y_skewer.is_used()) {
	_change_load = false;
	y_skewer.reset(_skew_type);
    }
    if(_skew_type != SKEW_CHAOTIC || URand(1,100) > 30) {
	_change_load = true;
    }
    ShoreEnv::start_load_imbalance();
}

void ShoreYCSBEnv::reset_skew()
{
    ShoreEnv::reset_skew();
    _change_load = false;
    y_skewer.clear();
}

int ShoreYCSBEnv::info() const
{
    TRACE( TRACE_ALWAYS, "SF      = (%.1f)\n", _scaling_factor);
    TRACE( TRACE_ALWAYS, "Workers = (%d)\n", _worker_cnt);
    return (0);
}

int ShoreYCSBEnv::statistics()
{
    // read the current trx statistics
    // CRITICAL_SECTION(cs, _statmap_mutex);
    // ShoreYCSBTrxStats rval;
    // rval -= rval; // dirty hack to set all zeros
    // for (statmap_t::iterator it=_statmap.begin(); it != _statmap.end(); ++it)
	// rval += *it->second;

    // TRACE( TRACE_STATISTICS, "AcctUpd. Att (%d). Abt (%d). Dld (%d)\n",
    //        rval.attempted.acct_update,
    //        rval.failed.acct_update,
    //        rval.deadlocked.acct_update);

    // TRACE( TRACE_STATISTICS, "MbenchInsertOnly. Att (%d). Abt (%d). Dld (%d)\n",
    //        rval.attempted.mbench_insert_only,
    //        rval.failed.mbench_insert_only,
    //        rval.deadlocked.mbench_insert_only);

    // TRACE( TRACE_STATISTICS, "MbenchDeleteOnly. Att (%d). Abt (%d). Dld (%d)\n",
    //        rval.attempted.mbench_delete_only,
    //        rval.failed.mbench_delete_only,
    //        rval.deadlocked.mbench_delete_only);

    // TRACE( TRACE_STATISTICS, "MbenchProbeOnly. Att (%d). Abt (%d). Dld (%d)\n",
    //        rval.attempted.mbench_probe_only,
    //        rval.failed.mbench_probe_only,
    //        rval.deadlocked.mbench_probe_only);

    // TRACE( TRACE_STATISTICS, "MbenchInsertDelte. Att (%d). Abt (%d). Dld (%d)\n",
    //        rval.attempted.mbench_insert_delete,
    //        rval.failed.mbench_insert_delete,
    //        rval.deadlocked.mbench_insert_delete);

    // TRACE( TRACE_STATISTICS, "MbenchInsertProbe. Att (%d). Abt (%d). Dld (%d)\n",
    //        rval.attempted.mbench_insert_probe,
    //        rval.failed.mbench_insert_probe,
    //        rval.deadlocked.mbench_insert_probe);

    // TRACE( TRACE_STATISTICS, "MbenchDeleteProbe. Att (%d). Abt (%d). Dld (%d)\n",
    //        rval.attempted.mbench_delete_probe,
    //        rval.failed.mbench_delete_probe,
    //        rval.deadlocked.mbench_delete_probe);

    // TRACE( TRACE_STATISTICS, "MbenchMix. Att (%d). Abt (%d). Dld (%d)\n",
    //        rval.attempted.mbench_mix,
    //        rval.failed.mbench_mix,
    //        rval.deadlocked.mbench_mix);

    ShoreEnv::statistics();

    return (0);
}

int ShoreYCSBEnv::start()
{
    return (ShoreEnv::start());
}

int ShoreYCSBEnv::stop()
{
    return (ShoreEnv::stop());
}

class table_builder_t : public thread_t
{
    ShoreYCSBEnv* _env;
    uint64_t _start;
    unsigned _count;
    int _id;

public:
    table_builder_t(ShoreYCSBEnv* env, int id, uint64_t start, unsigned count)
	: thread_t(std::string("LD-%d",id)),
          _env(env), _start(start), _count(count), _id(id)
    { }

    virtual void work()
    {
        cout << "Thread " << _id << " loading prefixes " << _start << " to " << _start+_count-1 << endl;
        for(uint64_t i=0; i < _count; i++) {
            uint64_t prefix = _start + i;
            for(uint64_t j=0; j < RecordsPerSF; j += RecordsPerPopXct) {
               uint64_t firstKey = (prefix << 48) | j;
               populate_db_input_t in{firstKey, RecordsPerPopXct};
               W_COERCE(_env->xct_populate_db(prefix, in));
            }
        }
        TRACE(TRACE_STATISTICS, "Finished loading prefixes %ld .. %ld \n", _start, _start+_count);
    }
};

struct table_creator_t : public thread_t
{
    ShoreYCSBEnv* _env;
    table_creator_t(ShoreYCSBEnv* env) : thread_t("CR"), _env(env) { }
    virtual void work()
    {
        // Create the tables, if any partitioning is to be applied, that has already
        // been set at update_partitioning()
        W_COERCE(_env->db()->begin_xct());
        W_COERCE(_env->ycsbtable_man->table()->create_physical_table(_env->db()));
        W_COERCE(_env->db()->commit_xct());
    }
};

rc_t ShoreYCSBEnv::create_tables()
{
    guard<table_creator_t> tc;
    tc = new table_creator_t(this);
    tc->fork();
    tc->join();
    return RCOK;
}

rc_t ShoreYCSBEnv::load_data()
{
    // Adjust the number of loaders to use, if the scaling factor is very small
    // and the total_accounts < #loaders* accounts_per_branch
    if (_scaling_factor<_loaders_to_use) {
        _loaders_to_use = _scaling_factor;
    }
    else {
        // number of prefixes must be multiple of number of loaders, otherwise load will fail
        while (static_cast<int>(_scaling_factor) % _loaders_to_use != 0) {
            _loaders_to_use--;
        }
    }

    long prefixes_per_worker = _scaling_factor/_loaders_to_use;

    array_guard_t< guard<table_builder_t> > loaders(new guard<table_builder_t>[_loaders_to_use]);
    for(int i=0; i < _loaders_to_use; i++) {
	// the preloader thread picked up that first set of accounts...
	uint64_t start = prefixes_per_worker*i;
	loaders[i] = new table_builder_t(this, i, start, prefixes_per_worker);
	loaders[i]->fork();
    }

    // 4. Join the loading threads
    for(int i=0; i<_loaders_to_use; i++) {
	loaders[i]->join();
    }

    return RCOK;
}

int ShoreYCSBEnv::conf()
{
    // reread the params
    ShoreEnv::conf();
    upd_worker_cnt();
    return (0);
}

void ShoreYCSBEnv::env_thread_init()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap[pthread_self()] = &my_stats;
}

void ShoreYCSBEnv::env_thread_fini()
{
    CRITICAL_SECTION(stat_mutex_cs, _statmap_mutex);
    _statmap.erase(pthread_self());
}


/********************************************************************
 *
 *  @fn:    _get_stats
 *
 *  @brief: Returns a structure with the currently stats
 *
 ********************************************************************/

ShoreYCSBTrxStats ShoreYCSBEnv::_get_stats()
{
    CRITICAL_SECTION(cs, _statmap_mutex);
    ShoreYCSBTrxStats rval;
    rval -= rval; // dirty hack to set all zeros
    for (statmap_t::iterator it=_statmap.begin(); it != _statmap.end(); ++it)
	rval += *it->second;
    return (rval);
}


/********************************************************************
 *
 *  @fn:    reset_stats
 *
 *  @brief: Updates the last gathered statistics
 *
 ********************************************************************/

void ShoreYCSBEnv::reset_stats()
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);
    _last_stats = _get_stats();
}


/********************************************************************
 *
 *  @fn:    print_throughput
 *
 *  @brief: Prints the throughput given a measurement delay
 *
 ********************************************************************/

void ShoreYCSBEnv::print_throughput(const double iQueriedSF,
                                    const int iSpread,
                                    const int iNumOfThreads,
                                    const double delay,
                                    const unsigned long mioch,
                                    const double avgcpuusage)
{
    CRITICAL_SECTION(last_stats_cs, _last_stats_mutex);

    // get the current statistics
    ShoreYCSBTrxStats current_stats = _get_stats();

    // now calculate the diff
    current_stats -= _last_stats;

    uint trxs_att  = current_stats.attempted.total();
    uint trxs_abt  = current_stats.failed.total();
    uint trxs_dld  = current_stats.deadlocked.total();

    TRACE( TRACE_ALWAYS, "*******\n"             \
           "QueriedSF: (%.1f)\n"                 \
           "Spread:    (%s)\n"                   \
           "Threads:   (%d)\n"                   \
           "Trxs Att:  (%d)\n"                   \
           "Trxs Abt:  (%d)\n"                   \
           "Trxs Dld:  (%d)\n"                   \
           "Secs:      (%.2f)\n"                 \
           "IOChars:   (%.2fM/s)\n"              \
           "AvgCPUs:   (%.1f) (%.1f%%)\n"        \
           "TPS:       (%.2f)\n",
           iQueriedSF,
           (iSpread ? "Yes" : "No"),
           iNumOfThreads, trxs_att, trxs_abt, trxs_dld,
           delay, mioch/delay, avgcpuusage,
           100*avgcpuusage/get_max_cpu_count(),
           (trxs_att-trxs_abt-trxs_dld)/delay);
}






/********************************************************************
 *
 * TPC-B TRXS
 *
 * (1) The run_XXX functions are wrappers to the real transactions
 * (2) The xct_XXX functions are the implementation of the transactions
 *
 ********************************************************************/


/*********************************************************************
 *
 *  @fn:    run_one_xct
 *
 *  @brief: Initiates the execution of one TPC-B xct
 *
 *  @note:  The execution of this trx will not be stopped even if the
 *          measure internal has expired.
 *
 *********************************************************************/

rc_t ShoreYCSBEnv::run_one_xct(Request* prequest)
{
    assert (prequest);

    if(_start_imbalance > 0 && !_bAlarmSet) {
	CRITICAL_SECTION(alarm_cs, _alarm_lock);
	if(!_bAlarmSet) {
	    alarm(_start_imbalance);
	    _bAlarmSet = true;
	}
    }

    // Only one xct type for now
    prequest->set_type(XCT_YCSB_SIMPLE);

    switch (prequest->type()) {
    case XCT_YCSB_SIMPLE:
        if (URand(1,100) <= _update_freq) { return (run_update(prequest)); }
        else { return (run_read(prequest)); }
     default:
	 assert (0); // UNKNOWN TRX-ID
     }
    return (RCOK);
}

DEFINE_TRX(ShoreYCSBEnv,read);
DEFINE_TRX(ShoreYCSBEnv,update);
DEFINE_TRX(ShoreYCSBEnv,populate_db);

rc_t ShoreYCSBEnv::xct_read(const int /* xct_id */, read_input_t& pin)
{
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<ycsbtable_man_impl> tuple(ycsbtable_man);
    rep_row_t areprow(ycsbtable_man->ts());
    rep_row_t areprowkey(ycsbtable_man->ts());
    areprow.set(ycsbtable_man->table()->maxsize());
    areprowkey.set(ycsbtable_man->table()->maxsize());
    tuple->_rep = &areprow;
    tuple->_rep_key = &areprowkey;

    // Probe index for given key
    W_DO(ycsbtable_man->index_probe(_pssm, tuple, pin.key));
    // Copy fields into local variables, just to "do something" with the tuple
    uint64_t key;
    char values[10][FieldSize];
    tuple->get_value(0, key);
    for (int i = 0; i < 10; i++) {
        tuple->get_value(i+1, values[i], FieldSize);
    }

    return RCOK;
}


rc_t ShoreYCSBEnv::xct_update(const int /* xct_id */, update_input_t& pin)
{
    assert (_pssm);
    assert (_initialized);
    assert (_loaded);

    tuple_guard<ycsbtable_man_impl> tuple(ycsbtable_man);
    rep_row_t areprow(ycsbtable_man->ts());
    rep_row_t areprowkey(ycsbtable_man->ts());
    areprow.set(ycsbtable_man->table()->maxsize());
    areprowkey.set(ycsbtable_man->table()->maxsize());
    tuple->_rep = &areprow;
    tuple->_rep_key = &areprowkey;

    // Probe index for given key
    W_DO(ycsbtable_man->index_probe_forupdate(_pssm, tuple, pin.key));
    // Update value of given field
    w_assert1(pin.field_number <= FieldCount);
    w_assert1(pin.field_number > 0);
    tuple->set_value(pin.field_number, pin.value);
    W_DO(ycsbtable_man->update_tuple(_pssm, tuple));

    return RCOK;
}

rc_t ShoreYCSBEnv::xct_populate_db(const int /* xct_id */, populate_db_input_t& pin)
{
    assert (_pssm);
    assert (_initialized);

    tuple_guard<ycsbtable_man_impl> tuple(ycsbtable_man);
    rep_row_t areprow(ycsbtable_man->ts());
    rep_row_t areprowkey(ycsbtable_man->ts());
    areprow.set(ycsbtable_man->table()->maxsize());
    areprowkey.set(ycsbtable_man->table()->maxsize());
    tuple->_rep = &areprow;
    tuple->_rep_key = &areprowkey;

    W_DO(db()->begin_xct());

    uint64_t key = pin.firstKey;
    char field[FieldSize];
    for (unsigned i = 0; i < pin.count; i++) {
        tuple->set_value(0, key);
        for (int j = 1; j <= FieldCount; j++) {
            fill_value(field);
            tuple->set_value(j, field);
        }
        W_DO(ycsbtable_man->add_tuple(_pssm, tuple));
        key++;
    }

    W_DO(db()->commit_xct());
    return RCOK;
}

baseline_ycsb_client_t::baseline_ycsb_client_t(std::string tname, const int id,
                                               ShoreYCSBEnv* env,
                                               const MeasurementType aType,
                                               const int trxid,
                                               const int numOfTrxs,
                                               const int selID, const double qf,
                                               int tspread)
    : base_client_t(tname,id,env,aType,trxid,numOfTrxs),
      _selid(selID), _qf(qf), _tspread(tspread)
{
    assert (env);
    assert (_id>=0 && _qf>0);

    // pick worker thread
    _worker = _env->worker(_id);
    assert (_worker);
}


int baseline_ycsb_client_t::load_sup_xct(mapSupTrxs& stmap)
{
    stmap.clear();
    stmap[XCT_YCSB_SIMPLE] = "YCSB-Simple";
    return (stmap.size());
}

rc_t baseline_ycsb_client_t::submit_one(int xct_type, int xctid)
{
    // Set input
    trx_result_tuple_t atrt;
    bool bWake = false;
    if (condex* c = _cp->take_one()) {
        atrt.set_notify(c);
        bWake = true;
    }

    // Get one action from the trash stack
    trx_request_t* arequest = new (_env->_request_pool) trx_request_t;
    tid_t atid;
    arequest->set(NULL,atid,xctid,atrt,xct_type,_selid,_tspread);

    // Enqueue to worker thread
    assert (_worker);
    _worker->enqueue(arequest,bWake);
    return (RCOK);
}


}; // namespace
