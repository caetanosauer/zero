#ifndef KITS_CMD_H
#define KITS_CMD_H

#include "command.h"
#include "shore_client.h"

class ShoreEnv;
class sm_options;
class FailureThread;
class SkewShiftingThread;
template <class T> class CrashThread;

class KitsCommand : public Command
{
	using Command::setupOptions;
public:
    KitsCommand();
    virtual ~KitsCommand() {}

    virtual void setupOptions();
    virtual void run();
    void mediaFailure(unsigned);
    void randomRootPageFailure();
    void crashFilthy();
	bool running();
    void set_stop_benchmark(bool);
    ShoreEnv* getShoreEnv();

protected:
    ShoreEnv* shoreEnv;

    bool opt_sharpBackup;
    bool opt_load;
    string opt_benchmark;
    string opt_conffile;
    int opt_num_trxs;
    unsigned opt_duration;
    unsigned opt_log_volume;
    bool opt_no_stop;
    int opt_num_threads;
    int opt_select_trx;
    int opt_queried_sf;
    int opt_update_freq;
    bool opt_eager;
    bool opt_skew;
    bool opt_spread;
    bool opt_asyncCommit;
    bool opt_warmup;
    int opt_crashDelay;
    bool opt_crashDelayAfterInit;
    int opt_failDelay;
    int opt_skewShiftDelay;

    bool hasFailed;
    MeasurementType mtype;

    std::shared_ptr<CrashThread<ShoreEnv>> pre_init_crash_thread;
    std::shared_ptr<SkewShiftingThread> skew_shifter;

    // overridden in sub-commands to set their own options
    virtual void loadOptions(sm_options& opt);

    template<class Client, class Environment> void runBenchmarkSpec();
    void runBenchmark();

    virtual void doWork();
    template <class Client, class Environment> void createClients();
    void forkClients();
    void joinClients();

    template <class Environment> void initShoreEnv();
    void init();
    void finish();

    // Filesystem functions
    void mkdirs(string);
    void ensureEmptyPath(string);
    void ensureParentPathExists(string);

    bool runBenchAfterLoad()
    {
        return opt_duration > 0 || opt_num_trxs > 0 || opt_log_volume > 0 || opt_no_stop == true;
    }

private:
    std::vector<base_client_t*> clients;
    bool clientsForked;
    FailureThread* failure_thread;
};

#endif
