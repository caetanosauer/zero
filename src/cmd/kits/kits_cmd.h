#ifndef KITS_CMD_H
#define KITS_CMD_H

#include "command.h"
#include "shore_client.h"

class ShoreEnv;
class sm_options;

class KitsCommand : public Command
{
	using Command::setupOptions;
public:
    KitsCommand();
    virtual ~KitsCommand() {}

    virtual void setupOptions();
    virtual void run();
protected:
    ShoreEnv* shoreEnv;

    string logdir;
    string archdir;
    string opt_dbfile;
    string opt_backup;

    bool opt_sharpBackup;
    bool opt_load;
    string opt_benchmark;
    string opt_conffile;
    int opt_num_trxs;
    unsigned opt_duration;
    int opt_num_threads;
    int opt_select_trx;
    int opt_queried_sf;
    bool opt_eager;
    bool opt_truncateLog;
    bool opt_skew;
    bool opt_spread;
    unsigned opt_warmup;
    int opt_crashDelay;

    MeasurementType mtype;

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

    void archiveLog();

private:
    std::vector<base_client_t*> clients;
    bool clientsForked;
};

#endif
