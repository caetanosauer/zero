#include <sstream>
#include "sm.h"
#include "stopwatch.h"
#include "base/command.h"
#include <atomic>
#include <random>
#include <chrono>
#include <thread>
#include <functional>

#include <boost/program_options.hpp>
namespace po = boost::program_options;

using namespace std;

po::options_description options_desc;
po::variables_map options;
sm_options sm_opt;

ss_m* sm;
StoreID stid;

size_t duration, threads, write_ratio, insert_ratio, random_ratio;

// Current max key value
std::atomic<long> max_key(1);

void setup_options()
{
    Command::setupSMOptions(options_desc);
    options_desc.add_options()
    ("duration,d", po::value<size_t>(&duration)->default_value(10),
        "Duration for which to run experiment (in seconds)")
    ("threads,t", po::value<size_t>(&threads)->default_value(1),
        "Number of threads to use")
    ("write-ratio", po::value<size_t>(&write_ratio)->default_value(50),
        "Percentage of write operations (integer from 0 to 100)")
    ("insert-ratio", po::value<size_t>(&write_ratio)->default_value(50),
        "Percentage of inserts among write operations (the rest are updates) \
         (integer from 0 to 100)")
    ("random-ratio", po::value<size_t>(&random_ratio)->default_value(50),
        "Percentage of random read/write operations (the rest are sequential) \
        (integer from 0 to 100)")
    ;
}

class btree_thread_t : public smthread_t
{
public:
    btree_thread_t() :
        smthread_t(t_regular, "btree_stresser"),
        element_length(80)
    {}

    virtual ~btree_thread_t()
    {}

    w_keystr_t key;

    // length of elements to insert
    smsize_t element_length;

    std::default_random_engine generator;

    virtual void run ()
    {
        std::uniform_int_distribution<size_t> distr(1, 100);

        long total_time = duration * 1000000; // microseconds
        stopwatch_t watch;
        long long begin_ts = watch.now();
        long counter = 0;

        cout << "Worker thread " << me()->id << " beginning" << endl;

        W_COERCE(sm->begin_xct());
        while (true) {
            size_t random = distr(generator);
            if (random >= write_ratio) {
                random = distr(generator);
                if (random >= insert_ratio) {
                    make_an_insertion();
                }
                else {
                    make_an_update();
                }
            }
            else {
                read_something();
            }
            // Only check for expiration every 10k iterations
            if (counter % 10000 == 0 && watch.now() - begin_ts > total_time) {
                break;
            }

            counter++;
        }
        // CS TODO: allow batching of TA's
        W_COERCE(sm->commit_xct());

        cout << "Worker thread " << me()->id << " finished" << endl;
    }

    void build_key(long id)
    {
        string keystr = string("key") + to_string(id);
        key.construct_regularkey(keystr.c_str(), keystr.length());
    }

    long get_random_key()
    {
        // TODO: support other distributions
        if (max_key <= 2) { return 1; }
        std::uniform_int_distribution<long> distr(1, max_key - 1);
        return distr(generator);
    }

    void make_an_insertion()
    {
        build_key(max_key++);
        char buffer[element_length];
        W_COERCE(sm->create_assoc(stid, key, vec_t(buffer, element_length)));
    }

    void make_an_update()
    {
        build_key(get_random_key());
        char buffer[element_length];
        W_COERCE(sm->overwrite_assoc(stid, key, buffer, 0 /* offset */, element_length));
    }

    void read_something()
    {
        long id = get_random_key();
        build_key(id);
        char buffer[element_length];
        bool found;
        W_COERCE(sm->find_assoc(stid, key, &buffer, element_length, found));
        w_assert0(found || id == 1);
    }
};

class main_thread_t : public smthread_t
{
public:
    main_thread_t()
    {}

    virtual ~main_thread_t() {}

    virtual void run ()
    {
        sm_opt.set_bool_option("sm_format", true);
        sm_opt.set_int_option("sm_cleaner_interval", -1);
        Command::setSMOptions(sm_opt, options);
        sm = new ss_m(sm_opt);

        W_COERCE(sm->begin_xct());
        W_COERCE(sm->create_index(stid));
        W_COERCE(sm->commit_xct());

        vector<btree_thread_t*> t(threads);
        for (size_t i = 0; i < threads; i++) {
            t[i] = new btree_thread_t();
            t[i]->fork();
        }

        for (size_t i = 0; i < threads; i++) {
            t[i]->join();
        }

        sm_stats_info_t stats;
        ss_m::gather_stats(stats);
        // cout << stats << endl;

        delete sm;
    }
};

int main(int argc, char** argv)
{
    setup_options();
    po::store(po::parse_command_line(argc, argv, options_desc), options);
    po::notify(options);

    main_thread_t t;
    t.fork();
    t.join();
}

