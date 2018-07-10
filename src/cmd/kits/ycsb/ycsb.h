#pragma once

#include "sm_vas.h"
#include "table_man.h"
#include "table_desc.h"
#include "skewer.h"
#include "shore_env.h"
#include "shore_client.h"
#include "util/random_input.h"

typedef ss_m Database;

namespace ycsb {

// YCSB table has 10 fields of 100 bytes each and an 8-byte primary key
static constexpr int FieldSize = 100;
static constexpr int FieldCount = 10;

DECLARE_TABLE_SCHEMA_PD(ycsbtable_t);

const int XCT_YCSB_MBENCH_INSERT_ONLY = 41;
const int XCT_YCSB_MBENCH_DELETE_ONLY = 42;
const int XCT_YCSB_MBENCH_PROBE_ONLY = 43;
const int XCT_YCSB_MBENCH_INSERT_DELETE = 44;
const int XCT_YCSB_MBENCH_INSERT_PROBE = 45;
const int XCT_YCSB_MBENCH_DELETE_PROBE = 46;
const int XCT_YCSB_MBENCH_MIX = 47;

//-----------------------------------------------------------------------------
// INPUT
//-----------------------------------------------------------------------------

extern skewer_t y_skewer;
extern bool _change_load;

// struct insert_input_t
// {
//     char key[10];
//     char field1[10];
//     char field2[10];
//     char field3[10];
//     char field4[10];
//     char field5[10];
//     char field6[10];
//     char field7[10];
//     char field8[10];
//     char field9[10];
// }

struct update_input_t
{
    uint64_t key;
    char value[FieldSize];
    uint8_t field_number;
};

struct read_input_t
{
    uint64_t key;
};

struct populate_db_input_t
{
    uint64_t firstKey;
    unsigned count;
};

static void fill_value(char* dest)
{
    static const char charset[] =
     "0123456789"
     "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     "abcdefghijklmnopqrstuvwxyz";
    auto randIndex = URand(0, sizeof(charset)-1);
    for (int i = 0; i < FieldSize; i++) { dest[i] = charset[randIndex]; }
}

static update_input_t create_update_input(int SF, int specificBr = 0, int tspread = 0);
static read_input_t create_read_input(int SF, int specificBr = 0, int tspread = 0);
// Required for macros but not used
static populate_db_input_t create_populate_db_input(int, int, int) { w_assert0(false); return {0,0}; }

//-----------------------------------------------------------------------------
// TABLE MANAGER
//-----------------------------------------------------------------------------

class ycsbtable_man_impl : public table_man_t<ycsbtable_t>
{
public:
    ycsbtable_man_impl(ycsbtable_t* tdesc) : table_man_t(tdesc) { }
    ~ycsbtable_man_impl() { }

    rc_t index_probe(Database* db, table_row_t* ptuple, const uint64_t id);
    rc_t index_probe_forupdate(Database* db, table_row_t* ptuple, const uint64_t id);

};

//-----------------------------------------------------------------------------
// ENV
//-----------------------------------------------------------------------------

struct ShoreYCSBTrxCount
{
    uint read;
    uint update;
    uint populate_db;

    ShoreYCSBTrxCount& operator+=(ShoreYCSBTrxCount const& rhs) {
        read += rhs.read;
        update += rhs.update;
	return (*this);
    }

    ShoreYCSBTrxCount& operator-=(ShoreYCSBTrxCount const& rhs) {
        read -= rhs.read;
        update -= rhs.update;
	return (*this);
    }

    uint total() const {
        return (read+update);
    }

}; // EOF: ShoreYCSBTrxCount

struct ShoreYCSBTrxStats
{
    ShoreYCSBTrxCount attempted;
    ShoreYCSBTrxCount failed;
    ShoreYCSBTrxCount deadlocked;

    ShoreYCSBTrxStats& operator+=(ShoreYCSBTrxStats const& other) {
        attempted  += other.attempted;
        failed     += other.failed;
        deadlocked += other.deadlocked;
        return (*this);
    }

    ShoreYCSBTrxStats& operator-=(ShoreYCSBTrxStats const& other) {
        attempted  -= other.attempted;
        failed     -= other.failed;
        deadlocked -= other.deadlocked;
        return (*this);
    }
};

class ShoreYCSBEnv : public ShoreEnv
{
public:
    typedef std::map<pthread_t, ShoreYCSBTrxStats*> statmap_t;

    ShoreYCSBEnv(boost::program_options::variables_map vm);
    virtual ~ShoreYCSBEnv();

    // DB INTERFACE

    virtual int set(envVarMap* /* vars */) { return(0); /* do nothing */ };
    virtual int open() { return(0); /* do nothing */ };
    virtual int pause() { return(0); /* do nothing */ };
    virtual int resume() { return(0); /* do nothing */ };
    virtual rc_t newrun() { return(RCOK); /* do nothing */ };

    virtual int post_init() { conf(); return 0; };
    virtual rc_t load_schema();

    virtual rc_t load_and_register_fids();

    virtual int conf();
    virtual int start();
    virtual int stop();
    virtual int info() const;
    virtual int statistics();

    int dump() { return 0; };

    virtual void print_throughput(const double iQueriedSF,
                                  const int iSpread,
                                  const int iNumOfThreads,
                                  const double delay,
                                  const unsigned long mioch,
                                  const double avgcpuusage);


    // --- operations over tables --- //
    rc_t create_tables();
    rc_t load_data();

    // YCSB Table
    ycsbtable_man_impl* ycsbtable_man;

    rc_t run_one_xct(Request* prequest);

    // Transactions
    DECLARE_TRX(read);
    DECLARE_TRX(update);
    // Database population
    DECLARE_TRX(populate_db);

    // for thread-local stats
    virtual void env_thread_init();
    virtual void env_thread_fini();

    // stat map
    statmap_t _statmap;

    // snapshot taken at the beginning of each experiment
    ShoreYCSBTrxStats _last_stats;
    virtual void reset_stats();
    ShoreYCSBTrxStats _get_stats();

    // set load imbalance and time to apply it
    void set_skew(int area, int load, int start_imbalance, int skew_type, bool shifting);
    void start_load_imbalance();
    void reset_skew();

    // not implemented
    rc_t warmup() { return RCOK; };
    rc_t check_consistency() { return RCOK; };
    rc_t db_print(int /*lines*/) { return RCOK; };
    rc_t db_fetch() { return RCOK; };
}; // EOF ShoreYCSBEnv

class baseline_ycsb_client_t : public base_client_t
{
private:
    int _selid;
    trx_worker_t* _worker;
    double _qf;
    int _tspread;

public:

    baseline_ycsb_client_t() { }

    baseline_ycsb_client_t(std::string tname, const int id, ShoreYCSBEnv* env,
                           const MeasurementType aType, const int trxid,
                           const int numOfTrxs,
                           const int selID, const double qf,
                           int tspread = 0);

    ~baseline_ycsb_client_t() { }

    // every client class should implement this function
    static int load_sup_xct(mapSupTrxs& map);

    rc_t submit_one(int xct_type, int xctid);

}; // EOF: baseline_ycsb_client_t

} // namespace ycsb
