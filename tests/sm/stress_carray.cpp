#include <sstream>
#include <random>
#include "stopwatch.h"
#include "sm_options.h"
#include "log_core.h"
#include "vol.h"

#include <boost/program_options.hpp>
namespace po = boost::program_options;

po::options_description options_desc;
po::variables_map options;

sm_options sm_opt;
log_core* logcore;

size_t num_threads;
size_t duration;
size_t min_logrec_size;
size_t max_logrec_size;
size_t distr_mean;
size_t distr_stddev;
size_t commit_freq;
string logdir;

void setup_options()
{
    options_desc.add_options()
    ("threads,t", po::value<size_t>(&num_threads)->default_value(4),
        "Number of threads to run")
    ("duration,d", po::value<size_t>(&duration)->default_value(10),
        "Duration for which to run experiment (in seconds)")
    ("min", po::value<size_t>(&min_logrec_size)->default_value(48),
        "Minimum log record size to be generated")
    ("max", po::value<size_t>(&max_logrec_size)->default_value(8192),
        "Maximum log record size to be generated")
    ("mean", po::value<size_t>(&distr_mean)->default_value(192),
        "Mean log record size to be generated")
    ("stddev", po::value<size_t>(&distr_stddev)->default_value(64),
        "Standard deviation of generated log record size")
    ("commit", po::value<size_t>(&commit_freq)->default_value(0),
        "Simulate commit by flushing log every N log records")
    ("logdir,l", po::value<string>(&logdir)->default_value("/dev/shm/log"),
        "Log directory")
    ;
}

class log_inserter_thread : public smthread_t
{
public:
    log_inserter_thread() :
        smthread_t(t_regular, "log_inserter"),
        counter(0), volume(0)
    {}

    virtual ~log_inserter_thread() {}

    unsigned long counter;
    unsigned long volume;

    virtual void run ()
    {
        logrec_t logrec;
        lsn_t lsn;
        std::default_random_engine generator;
        std::normal_distribution<float> distr(distr_mean, distr_stddev);

        long total_time = duration * 1000000; // microseconds
        stopwatch_t watch;
        long long begin_ts = watch.now();

        while (true) {
            unsigned int size = distr(generator);
            if (size < min_logrec_size) {
                size = min_logrec_size;
            }
            else if (size > max_logrec_size) {
                size = max_logrec_size;
            }

            logrec.fill(0, size);
            W_COERCE(logcore->insert(logrec, &lsn));
            counter++;
            volume += size;

            if (commit_freq > 0 && counter % commit_freq == 0) {
                W_COERCE(logcore->flush(lsn));
            }

            // Only check for expiration every 10k iterations
            if (counter % 10000 == 0 && watch.now() - begin_ts > total_time) {
                break;
            }
        }
    }
};

class main_thread_t : public smthread_t
{
public:
    main_thread_t() :
        smthread_t(t_regular, "log_inserter")
    {}

    virtual ~main_thread_t() {}

    virtual void run ()
    {
        sm_opt.set_string_option("sm_logdir", logdir);
        sm_opt.set_bool_option("sm_format", true);
        logcore = new log_core(sm_opt);
        smlevel_0::log = logcore;
        W_COERCE(logcore->init());

        log_inserter_thread* threads[num_threads];
        for (size_t i = 0; i < num_threads; i++) {
            threads[i] = new log_inserter_thread();
            threads[i]->fork();
        }

        long total_count = 0, total_volume = 0;
        for (size_t i = 0; i < num_threads; i++) {
            threads[i]->join();
            total_volume += threads[i]->volume;
            total_count += threads[i]->counter;
            delete threads[i];
        }

        size_t bwidth = (total_volume / duration) / 1048576;
        cout << "Thread_count: " << num_threads << endl;
        cout << "Total_log_volume: " << (float) total_volume / (1024*1024*1024) << " GB" << endl;
        cout << "Log_record_count: " << total_count << endl;
        cout << "Avg_logrec_size: " << total_volume / total_count << endl;
        cout << "Total_bandwidth: " << bwidth << " MB/s" << endl;
        cout << "Bandwidth_per_thread: " << bwidth / num_threads << " MB/s" << endl;

        logcore->shutdown();
        delete logcore;
    }
};


int main(int argc, char** argv)
{
    setup_options();
    po::store(po::parse_command_line(argc, argv, options_desc), options);
    po::notify(options);

    sthread_t::initialize_sthreads_package();
    smthread_t::init_fingerprint_map();

    cout << "Forking " << num_threads << " threads for "
        << duration << " seconds " << endl;

    main_thread_t t;
    t.fork();
    t.join();
}
