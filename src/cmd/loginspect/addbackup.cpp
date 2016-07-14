#include "addbackup.h"

#include "log_core.h"
#include "logarchiver.h"

void AddBackup::setupOptions()
{
    boost::program_options::options_description opt("AddBackup Options");
    opt.add_options()
        ("logdir,l", po::value<string>(&logdir)->required(),
            "Log directory")
        ("file,f", po::value<string>(&backupPath)->required(),
            "Path to backup file")
        ("lsn", po::value<string>(&lsnString)->required(),
            "Backup LSN (up to which all updates are guaranteed to be propagated)")
    ;
    options.add(opt);
}

void AddBackup::run()
{
    sm_options opt;
    opt.set_string_option("sm_logdir", logdir);

    auto log = new log_core(opt);
    smlevel_0::log = log;
    W_COERCE(log->init());

    lsn_t backupLSN;
    stringstream ss(lsnString);
    ss >> backupLSN;

    sys_xct_section_t ssx(true);
    W_COERCE(log_add_backup(backupPath, backupLSN));
    W_COERCE(ssx.end_sys_xct(RCOK));

    W_COERCE(log->flush_all());
    log->shutdown();
    delete log;
}
