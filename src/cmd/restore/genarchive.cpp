#include "genarchive.h"
#include "log_core.h"

#include <fstream>

// CS TODO: LA metadata -- should be serialized on run files
const size_t BLOCK_SIZE = 1048576;

void GenArchive::setupOptions()
{
    options.add_options()
        ("logdir,l", po::value<string>(&logdir)->required(),
            "Directory containing the log to be archived")
        ("archdir,a", po::value<string>(&archdir)->required(),
            "Directory where the archive runs will be stored (must exist)")
        ("bucket", po::value<size_t>(&bucketSize)->default_value(1),
            "Size of log archive index bucked in output runs")
        // ("maxLogSize,m", po::value<long>(&maxLogSize)->default_value(m),
        //     "max_logsize parameter of Shore-MT (default should be fine)")
    ;
}

void GenArchive::run()
{
    sm_options opt;
    opt.set_string_option("sm_logdir", logdir);
    opt.set_string_option("sm_archdir", archdir);
    opt.set_int_option("sm_archiver_block_size", BLOCK_SIZE);
    opt.set_int_option("sm_archiver_bucket_size", bucketSize);
    opt.set_int_option("sm_page_img_compression", 16384);

    log_core* log = new log_core(opt);
    W_COERCE(log->init());
    smlevel_0::log = log;
    LogArchiver* la = new LogArchiver(opt);;

    lsn_t durableLSN = log->durable_lsn();
    cerr << "Activating log archiver until LSN " << durableLSN << endl;

    la->fork();

    la->requestFlushSync(durableLSN);

    la->shutdown();
    la->join();

    delete la;
    delete log;
}
