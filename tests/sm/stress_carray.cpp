#include "btree_test_env.h"

#include <sstream>

#include "stopwatch.h"
#include "sm_options.h"
#include "log_core.h"
#include "vol.h"

/*
 * CONFIGURE TEST BEGIN
 */
int NUM_THREADS = 24;
const int RUN_SECONDS = 10;
const int MIN_LOGREC_SIZE = 48;
const int MAX_LOGREC_SIZE = 8192;
const int DISTR_MEAN = 192;
const int DISTR_STDDEV = 64;
/*
 * CONFIGURE TEST END
 */

sm_options options;
log_core* logcore;

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
        std::default_random_engine generator;
        std::normal_distribution<float> distr(DISTR_MEAN, DISTR_STDDEV);

        long total_time = RUN_SECONDS * 1000000; // microseconds
        stopwatch_t watch;
        long long begin_ts = watch.now();

        while (true) {
            unsigned int size = distr(generator);
            if (size < MIN_LOGREC_SIZE) {
                size = MIN_LOGREC_SIZE;
            }
            else if (size > MAX_LOGREC_SIZE) {
                size = MAX_LOGREC_SIZE;
            }

            logrec.fill(0, size);
            W_COERCE(logcore->insert(logrec));
            counter++;
            volume += size;

            if (watch.now() - begin_ts > total_time) {
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
        options.set_string_option("sm_logdir", "/dev/shm/log");
        options.set_bool_option("sm_format", true);
        logcore = new log_core(options);
        smlevel_0::log = logcore;
        W_COERCE(logcore->init());

        log_inserter_thread* threads[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new log_inserter_thread();
            threads[i]->fork();
        }

        long total_count = 0, total_volume = 0;
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i]->join();
            total_volume += threads[i]->volume;
            total_count += threads[i]->counter;
            delete threads[i];
        }

        cout << "Thread_count: " << NUM_THREADS << endl;
        cout << "Total_log_volume: " << (float) total_volume / (1024*1024*1024) << " GB" << endl;
        cout << "Log_record_count: " << total_count << endl;
        cout << "Avg_logrec_size: " << total_volume / total_count << endl;
        cout << "Total_bandwidth: " << (total_volume / 1048576) << " MB/s" << endl;
        cout << "Bandwidth_per_thread: " << (total_volume / 1048576) / NUM_THREADS
            << " MB/s" << endl;

        logcore->shutdown();
        delete logcore;
    }
};


int main(int argc, char** argv)
{
    sthread_t::initialize_sthreads_package();
    smthread_t::init_fingerprint_map();

    if (argc > 1) {
        NUM_THREADS = atoi(argv[1]);
    }

    cout << "Forking " << NUM_THREADS << " threads for "
        << RUN_SECONDS << " seconds " << endl;

    main_thread_t t;
    t.fork();
    t.join();
}
