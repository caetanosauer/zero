#include "genarchive.h"

#include <fstream>

void GenArchive::setupOptions()
{
    // default value
    long m = 274877906944L; // 256GB

    options.add_options()
        ("logdir,l", po::value<string>(&logdir)->required(),
            "Directory containing the log to be archived")
        ("archdir,a", po::value<string>(&archdir)->required(),
            "Directory where the archive runs will be stored (must exist)")
        // ("maxLogSize,m", po::value<long>(&maxLogSize)->default_value(m),
        //     "max_logsize parameter of Shore-MT (default should be fine)")
    ;
}

void GenArchive::run()
{
    // check if directory exists
    ifstream f(archdir);
    bool exists = f.good();
    f.close();

    if (!exists) {
        // TODO make command and handler exceptions
        throw runtime_error("Directory does not exist: " + archdir);
    }

    /*
     * TODO if we add boost filesystem, we can create the directory
     * and if it already exists, check if there are any files in it
     */

    const size_t blockSize = 1048576;
    const size_t workspaceSize = 300 * blockSize;

    start_base();
    start_log(logdir);
    start_archiver(archdir, workspaceSize, blockSize);

    lsn_t durableLSN = smlevel_0::log->durable_lsn();
    cerr << "Activating log archiver until LSN " << durableLSN << endl;

    LogArchiver* la = smlevel_0::logArchiver;
    la->fork();

    // wait for log record to be consumed
    while (la->getNextConsumedLSN() < durableLSN) {
        la->activate(durableLSN, true);
        ::usleep(10000); // 10ms
    }

    // Time to wait until requesting a log archive flush (msec). If we're
    // lucky, log is archiving very fast and a flush request is not needed.
    int waitBeforeFlush = 100;
    ::usleep(waitBeforeFlush * 1000);

    if (la->getDirectory()->getLastLSN() < durableLSN) {
        la->requestFlushSync(durableLSN);
    }

    la->shutdown();
    la->join();
}
