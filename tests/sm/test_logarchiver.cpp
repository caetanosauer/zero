#include "logarchiver.h"

#include <cstdlib>
#include <sm_vas.h>
#include <sm.h>
#include <sm_s.h>
#include <pmap.h>
#include <sm_io.h>
#include <pmap.h>
#include <logrec.h>
#include <log.h>
#include <log_core.h>
#include <log_carray.h>
#include <xct.h>
#include <restart.h>
#include <stdexcept>

char * archdir;
char * logdir;
size_t bsize = 8192;
size_t bcount = 16;

static const int logbufsize = 81920 * 1024; // 80 MB
static const long max_logsz = 11811160064L; // 11GB -- enough for 128 open partitions
static rc_t rc;

void initLog()
{

    // initialization stuff
    sthread_t::initialize_sthreads_package();
    smthread_t::init_fingerprint_map();
    //smlevel_0::init_errorcodes();
    smlevel_0::errlog = new ErrLog("logarchiver_test", log_to_stderr, "-");
    smlevel_0::max_logsz = max_logsz;
    smlevel_0::shutting_down = true;

    // instantiate log manager
    log_m * log = new log_core(
                    logdir,
                    logbufsize,      // logbuf_segsize
                    true,
                    ConsolidationArray::DEFAULT_ACTIVE_SLOT_COUNT
                    );
    if (rc.is_error()) {
        throw runtime_error("Failure initializing log_m");
    }
    // force log scan to start at the given LSN,
    // even if it is earlier than master_lsn
    //log_m::set_inspecting(true);
    (void) log;
}

int main(int argc, char ** argv)
{
    if (argc < 3) {
        cout << "Usage: " << argv[0] << " <logdir> <archdir>" << endl;
        return 1;
    }
    logdir = argv[1];
    archdir = argv[2];

    initLog();

    LogArchiver* la;
    LogArchiver::constructOnce(la, archdir, true, 100 * 1024 * 1024);
    la->fork();
    la->start_shutdown();
    la->join();
}
