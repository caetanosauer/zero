#include "command.h"

#include "kits_cmd.h"
#include "genarchive.h"
#include "mergeruns.h"
#include "agglog.h"
#include "logcat.h"
#include "verifylog.h"
#include "truncatelog.h"
#include "propstats.h"
#include "logpagestats.h"
#include "loganalysis.h"
#include "dbscan.h"
#include "addbackup.h"
#include "xctlatency.h"
#include "tracerestore.h"
#include "archstats.h"
#include "logrecinfo.h"
#include "nodbgen.h"

#include <boost/foreach.hpp>

/*
 * Adapted from
 * http://stackoverflow.com/questions/582331/is-there-a-way-to-instantiate-objects-from-a-string-holding-their-class-name
 */
Command::ConstructorMap Command::constructorMap;

template<typename T> Command* createCommand()
{
    return new T;
}

#define REGISTER_COMMAND(str, cmd) \
{ \
    Command::constructorMap[str] = &createCommand<cmd>; \
}

void Command::init()
{
    /*
     * COMMANDS MUST BE REGISTERED HERE AND ONLY HERE
     */
    REGISTER_COMMAND("logcat", LogCat);
    //REGISTER_COMMAND("logreplay", LogReplay);
    REGISTER_COMMAND("genarchive", GenArchive);
    REGISTER_COMMAND("mergeruns", MergeRuns);
    REGISTER_COMMAND("verifylog", VerifyLog);
    REGISTER_COMMAND("truncatelog", TruncateLog);
    REGISTER_COMMAND("dbscan", DBScan);
    REGISTER_COMMAND("nodbgen", NoDBGen);
    REGISTER_COMMAND("addbackup", AddBackup);
    REGISTER_COMMAND("xctlatency", XctLatency);
    REGISTER_COMMAND("agglog", AggLog);
    REGISTER_COMMAND("logpagestats", LogPageStats);
    REGISTER_COMMAND("loganalysis", LogAnalysis);
    REGISTER_COMMAND("kits", KitsCommand);
    REGISTER_COMMAND("propstats", PropStats);
    REGISTER_COMMAND("tracerestore", RestoreTrace);
    REGISTER_COMMAND("logrecinfo", LogrecInfo);
    REGISTER_COMMAND("archstats", ArchStats);
}

void Command::setupCommonOptions()
{
    options.add_options()
        ("help,h", "Displays help information regarding a specific command")
        ("config,c", po::value<string>()->implicit_value("zapps.conf"),
         "Specify path to a config file");
}

void Command::showCommands()
{
    cerr << "Usage: zapps <command> [options] "
        << endl << "Commands:" << endl;
    ConstructorMap::iterator it;
    for (it = constructorMap.begin(); it != constructorMap.end(); it++) {
        // Options common to all commands
        Command* cmd = (it->second)();
        cmd->setupCommonOptions();
        cmd->setupOptions();
        cerr << it->first << endl << cmd->options << endl << endl;
    }
}

Command* Command::parse(int argc, char ** argv)
{
    if (argc >= 2) {
        string cmdStr = argv[1];
        std::transform(cmdStr.begin(), cmdStr.end(), cmdStr.begin(), ::tolower);
        if (constructorMap.find(cmdStr) != constructorMap.end()) {
            Command* cmd = constructorMap[cmdStr]();
            cmd->setupCommonOptions();
            cmd->setCommandString(cmdStr);
            cmd->setupOptions();

            po::variables_map vm;
            po::store(po::parse_command_line(argc,argv,cmd->getOptions()), vm);
            if (vm.count("config") > 0) {
                string pathToFile = vm["config"].as<string>();
                std::ifstream file;
                file.open(pathToFile.c_str());
                po::store(po::parse_config_file(file,cmd->getOptions(), true), vm);
            }
            if (vm.count("help") > 0) {
                cmd->helpOption();
                return NULL;
            }

            po::notify(vm);
            cmd->setOptionValues(vm);
            return cmd;
        }
    }

    showCommands();
    return NULL;
}

void Command::setupSMOptions(po::options_description& options)
{
    boost::program_options::options_description smoptions("Storage Manager Options");
    smoptions.add_options()
    ("db-config-design", po::value<string>()->default_value("normal"),
       "")
    ("physical-hacks-enable", po::value<int>()->default_value(0),
        "Enables physical hacks, such as padding of records")
    ("db-worker-sli", po::value<bool>()->default_value(0),
        "Speculative Lock inheritance")
    ("db-loaders", po::value<int>()->default_value(10),
        "Specifies the number of threads that are used to load the db")
    ("db-worker-queueloops", po::value<int>()->default_value(10),
                "?")
    ("db-cl-batchsz", po::value<int>()->default_value(10),
                "Specify the batchsize of a client executing transactions")
    ("db-cl-thinktime", po::value<int>()->default_value(0),
            "Specify a 'thinktime' for a client")
    ("records-to-access", po::value<uint>()->default_value(0),
        "Used in the benchmarks for the secondary indexes")
    ("activation_delay", po::value<uint>()->default_value(0),
            "")
    ("db-workers", po::value<uint>()->default_value(1),
        "Specify the number of workers executing transactions")
    ("dir-trace", po::value<string>()->default_value("RAT"),
        "")
    /** System related options **/
    ("sys-maxcpucount", po::value<uint>()->default_value(0),
        "Maximum CPU Count of a system")
    ("sys-activecpucount", po::value<uint>()->default_value(0),
        "Active CPU Count of a system")
    /**SM Options**/
    ("sm_logdir", po::value<string>()->default_value("log"),
        "Path to log directory")
    ("sm_dbfile", po::value<string>()->default_value("db"),
        "Path to the file on which to store database pages")
    ("sm_format", po::value<bool>()->default_value(false),
        "Format SM by emptying logdir and truncating DB file")
    ("sm_truncate_log", po::value<bool>()->default_value(false)
        ->implicit_value(true),
        "Whether to truncate log partitions at SM shutdown")
    ("sm_truncate_archive", po::value<bool>()->default_value(false)
        ->implicit_value(true),
        "Whether to truncate log archive runs at SM shutdown")
    ("sm_log_partition_size", po::value<int>()->default_value(1024),
        "Size of a log partition in MB")
    ("sm_log_max_partitions", po::value<int>()->default_value(0),
        "Maximum number of partitions maintained in log directory")
    ("sm_log_delete_old_partitions", po::value<bool>()->default_value(true),
        "Whether to delete old log partitions as cleaner and chkpt make progress")
    ("sm_group_commit_size", po::value<int>(),
        "Size in bytes of group commit window (higher -> larger log writes)")
    ("sm_group_commit_timeout", po::value<int>(),
        "Max time to wait (in ms) to fill up group commit window")
    ("sm_log_benchmark_start", po::value<bool>()->default_value(false),
        "Whether to generate benchmark_start log record on SM constructor")
    ("sm_page_img_compression", po::value<int>()->default_value(0),
        "Enables page-image compression for every N log bytes (N=0 turns off)")
    ("sm_bufpoolsize", po::value<int>()->default_value(1024),
        "Size of buffer pool in MB")
    ("sm_fakeiodelay-enable", po::value<int>()->default_value(0),
        "Enables a artificial delay whenever there is a I/O operation")
    ("sm_fakeiodelay", po::value<uint>()->default_value(0),
            "Specify the imposed delay in usec")
    ("sm_errlog", po::value<string>()->default_value("shoremt.err.log"),
            "Path to the error log of the storage manager")
    ("sm_chkpt_interval", po::value<int>(),
            "Interval for checkpoint flushes")
    ("sm_chkpt_log_based", po::value<bool>(),
        "Take checkpoints decoupled from buffer and transaction manager, using log scans")
    ("sm_chkpt_use_log_archive", po::value<bool>(),
        "Checkpoints use archived LSN to compute min_rec_lsn")
    ("sm_chkpt_print_propstats", po::value<bool>(),
        "Print min recl lsn and dirty page coutn for every chkpt taken")
    ("sm_chkpt_only_root_pages", po::value<bool>(),
        "Checkpoints only record dirty root pages and SPR takes care of rest")
    ("sm_log_fetch_buf_partitions", po::value<uint>()->default_value(0),
        "Number of partitions to buffer in memory for recovery")
    ("sm_log_page_flushers", po::value<uint>()->default_value(1),
        "Number of log page flushers")
    ("sm_preventive_chkpt", po::value<uint>()->default_value(1),
        "Disable/enable preventive checkpoints (0 to disable, 1 to enable)")
    ("sm_logbuf_seg_count", po::value<int>(),
        "Log Buffer Segment Count")
    ("sm_logbuf_flush_trigger", po::value<int>(),
        "?")
    ("sm_logbuf_block_size", po::value<int>(),
        "Log Buffer Block isze")
    ("sm_logbuf_part_size", po::value<int>(),
        "Log Buffer part size")
    ("sm_carray_slots", po::value<int>(),
        "")
    ("sm_vol_cluster_stores", po::value<bool>(),
        "Cluster pages of the same store into extents")
    ("sm_vol_log_reads", po::value<bool>(),
        "Generate log records for every page read")
    ("sm_vol_log_writes", po::value<bool>(),
        "Generate log records for every page write")
    ("sm_vol_readonly", po::value<bool>(),
        "Volume will be opened in read-only mode and all writes from buffer pool \
         will be ignored (uses write elision and single-page recovery)")
    ("sm_log_o_direct", po::value<bool>(),
        "Whether to open log file with O_DIRECT")
    ("sm_arch_o_direct", po::value<bool>(),
        "Whether to open log archive files with O_DIRECT")
    ("sm_vol_o_direct", po::value<bool>(),
        "Whether to open volume (i.e., db file) with O_DIRECT")
    ("sm_no_db", po::value<bool>()->implicit_value(true)->default_value(false),
        "No-database mode, a.k.a. log-structured mode, a.k.a. extreme write elision: \
         DB file is written and all fetched pages are rebuilt \
         using single-page recovery from scratch")
    ("sm_batch_segment_size", po::value<int>(),
        "Size of segments to use during batch restore warmup")
    ("sm_restart_instant", po::value<bool>(),
        "Enable instant restart")
    ("sm_restart_log_based_redo", po::value<bool>(),
        "Perform non-instant restart with log-based redo instead of page-based")
    ("sm_restart_prioritize_archive", po::value<bool>(),
        "When performing single-page recovery, fetch as much as possible from \
        log archive and minimize random reads in the recovery log")
    ("sm_rawlock_gc_interval_ms", po::value<int>(),
        "Garbage Collection Interval in ms")
    ("sm_rawlock_lockpool_segsize", po::value<int>(),
        "Segment size Lockpool")
    ("sm_rawlock_xctpool_segsize", po::value<int>(),
        "Segment size Transaction Pool")
    ("sm_rawlock_gc_generation_count", po::value<int>(),
        "Garbage collection generation count")
    ("sm_rawlock_gc_init_generation_count", po::value<int>(),
        "Garbage collection initial generation count")
    ("sm_rawlock_lockpool_initseg", po::value<int>(),
        "Lock pool init segment")
    ("sm_rawlock_xctpool_segsize", po::value<int>(),
        "Transaction Pool Segment Size")
    ("sm_rawlock_gc_free_segment_count", po::value<int>(),
        "Garbage Collection Free Segment Count")
    ("sm_rawlock_gc_max_segment_count", po::value<int>(),
        "Garbage Collection Maximum Segment Count")
    ("sm_locktablesize", po::value<int>(),
        "Lock table size")
    ("sm_rawlock_xctpool_initseg", po::value<int>(),
        "Transaction Pool Initialization Segment")
    ("sm_bf_warmup_hit_ratio", po::value<int>(),
        "Hit ratio to be achieved until system is considered warmed up (int from 0 to 100)")
    ("sm_bf_warmup_min_fixes", po::value<int>(),
        "Only consider warmup hit ratio once this minimum number of fixes has been performed")
    ("sm_cleaner_decoupled", po::value<bool>(),
        "Enable/Disable decoupled cleaner")
    ("sm_cleaner_interval", po::value<int>(),
        "Cleaner sleep interval in ms")
    ("sm_cleaner_workspace_size", po::value<int>(),
        "Size of cleaner write buffer")
    ("sm_cleaner_num_candidates", po::value<int>(),
        "Number of candidate frames considered by each cleaner round")
    ("sm_cleaner_policy", po::value<string>(),
        "Policy used by cleaner to select candidates")
    ("sm_cleaner_min_write_size", po::value<int>(),
        "Page cleaner only writes clusters of pages with this minimum size")
    ("sm_cleaner_min_write_ignore_freq", po::value<int>(),
        "Ignore min_write_size every N rounds of cleaning")
    ("sm_cleaner_async_candidate_collection", po::value<bool>(),
        "Collect candidate frames to be cleaned in an asynchronous thread")
    ("sm_evict_policy", po::value<string>(),
        "Policy to use in eviction (a.k.a. page replacement)")
    ("sm_evict_dirty_pages", po::value<bool>(),
        "Do not skip dirty pages when performing eviction and write them out if necessary")
    ("sm_evict_random", po::value<bool>(),
        "Pick eviction victim at random, instead of going round-robin over frames")
    ("sm_evict_use_clock", po::value<bool>(),
        "Maintain clock bits on buffer frames and only evict if clock bit is zero")
    ("sm_async_eviction", po::value<bool>(),
        "Perform eviction in a dedicated thread, while fixing threads wait")
    ("sm_eviction_interval", po::value<int>(),
            "Interval for async eviction thread (in msec)")
    ("sm_wakeup_cleaner_attempts", po::value<int>(),
            "How many failed eviction attempts until cleaner is woken up (0 = never)")
    ("sm_clean_only_attempts", po::value<int>(),
            "How many failed eviction attempts until dity frames are picked as victims (0 = never)")
    ("sm_log_page_evictions", po::value<bool>(),
        "Generate evict_page log records for every page evicted from the buffer pool")
    ("sm_log_page_fetches", po::value<bool>(),
        "Generate fetch_page log records for every page fetched (and recovered) into the buffer pool")
    ("sm_archiver_workspace_size", po::value<int>(),
        "Workspace size archiver")
    // CS TODO: archiver currently only works with 1MB blocks
    // ("sm_archiver_block_size", po::value<int>()->default_value(1024*1024),
    //     "Archiver Block size")
    ("sm_archiver_bucket_size", po::value<int>(),
        "Archiver bucket size")
    ("sm_archiver_merging", po::value<bool>(),
        "Whether to turn on asynchronous merging with log archiver")
    ("sm_archiver_fanin", po::value<int>(),
        "Log archiver merge fan-in")
    ("sm_archiver_replication_factor", po::value<int>(),
        "Replication factor maintained by the log archive \
         run recycler (0 = never delete a run)")
    ("sm_archiving_blocksize", po::value<int>(),
        "Archiving block size")
    ("sm_reformat_log", po::value<bool>(),
        "Enable/Disable reformat log")
    ("sm_logging", po::value<bool>()->default_value(true),
        "Enable/Disable logging")
    ("sm_decoupled_cleaner", po::value<bool>(),
        "Use log-based propagation to clean pages")
    ("sm_shutdown_clean", po::value<bool>(),
        "Force buffer before shutting down SM")
    ("sm_archiving", po::value<bool>(),
        "Enable/Disable archiving")
    ("sm_async_merging", po::value<bool>(),
        "Enable/Disable Asynchronous merging")
    ("sm_statistics", po::value<bool>(),
        "Enable/Disable display of statistics")
    ("sm_ticker_enable", po::value<bool>(),
        "Enable/Disable ticker (currently always enabled)")
    ("sm_ticker_msec", po::value<int>(),
        "Ticker interval in millisec")
    ("sm_ticker_print_tput", po::value<bool>(),
        "Print transaction throughput on every tick to a file tput.txt")
    ("sm_prefetch", po::value<bool>(),
        "Enable/Disable prefetching")
    ("sm_backup_prefetcher_segments", po::value<int>(),
        "Segment size restore")
    ("sm_restore_segsize", po::value<int>(),
        "Segment size restore")
    ("sm_restore_prefetcher_window", po::value<int>(),
        "Segment size restore")
    ("sm_restore_instant", po::value<bool>(),
        "Enable/Disable instant restore")
    ("sm_restore_reuse_buffer", po::value<bool>(),
        "Enable/Disable reusage of buffer")
    ("sm_restore_multiple_segments", po::value<int>(),
        "Number of segments to attempt restore at once")
    ("sm_restore_min_read_size", po::value<int>(),
        "Attempt to read at least this many bytes when scanning log archive")
    ("sm_restore_max_read_size", po::value<int>(),
        "Attempt to read at most this many bytes when scanning log archive")
    ("sm_restore_preemptive", po::value<bool>(),
        "Use preemptive scheduling during restore")
    ("sm_restore_sched_singlepass", po::value<bool>(),
        "Use single-pass scheduling in restore")
    ("sm_restore_threads", po::value<int>(),
        "Number of restore threads to use")
    ("sm_restore_sched_ondemand", po::value<bool>(),
        "Support on-demand restore")
    ("sm_restore_sched_random", po::value<bool>(),
        "Use random page order in restore scheduler")
    ("sm_bufferpool_swizzle", po::value<bool>(),
        "Enable/Disable bufferpool swizzle")
    ("sm_write_elision", po::value<bool>(),
        "Enable/Disable write elision in buffer pool")
    ("sm_archiver_eager", po::value<bool>(),
        "Enable/Disable eager archiving")
    ("sm_archiver_read_whole_blocks", po::value<bool>(),
        "Enable/Disable reading whole blocks in the archiver")
    ("sm_archiver_slow_log_grace_period", po::value<int>(),
        "Enable/Disable slow log grace period")
    ("sm_errlog_level", po::value<string>(),
        "Specify a errorlog level. Options:")
        //TODO Stefan Find levels and insert them
    ("sm_log_impl", po::value<string>(),
        "Choose log implementation. Options")
        //TODO Stefan Find Implementations
    ("sm_backup_dir", po::value<string>(),
        "Path to a backup directory")
    ("sm_bufferpool_replacement_policy", po::value<string>(),
        "Replacement Policy")
    ("sm_archdir", po::value<string>()->default_value("archive"),
        "Path to archive directory");
    options.add(smoptions);
}

void Command::helpOption()
{
    cerr << "Usage: zapps Command:" << commandString << " [options] "
                << endl << options << endl;
}

size_t LogScannerCommand::BLOCK_SIZE = 1024 * 1024;

BaseScanner* LogScannerCommand::getScanner(
        bitset<t_max_logrec>* filter)
{
    BaseScanner* s;
    if (isArchive) {
        if (merge) s = new MergeScanner(optionValues);
        else s = new LogArchiveScanner(optionValues);
    }
    else {
        s = new BlockScanner(optionValues, filter);
    }

    if (!filename.empty()) {
        if (!isArchive) { s->setRestrictFile(logdir + "/" + filename); }
        else { s->setRestrictFile(filename); }
    }

    return s;
}

void LogScannerCommand::setupOptions()
{
    setupSMOptions(options);
    po::options_description logscanner("Log Scanner Options");
    logscanner.add_options()
        ("logdir,l", po::value<string>(&logdir)->required(),
         "Directory containing log to be scanned")
        ("file,f", po::value<string>(&filename)->default_value(""),
            "Scan only a specific file inside the given directory")
        ("archive,a", po::value<bool>(&isArchive)->default_value(false)
         ->implicit_value(true),
            "Scan log archive files isntead of normal recovery log")
        ("merge,m", po::value<bool>(&merge)->default_value(false)
         ->implicit_value(true),
            "Merge archiver input so that global sort order is produced")
        ("limit,n", po::value<size_t>(&limit)->default_value(0),
             "Number of log records to scan")
        ("level", po::value<int>(&level)->default_value(-1),
             "Level of log archive to scan (-1 for all)")
        ("pid", po::value<PageID>(&scan_pid)->default_value(0),
             "PageID on which to begin scan (archive only)")
        ;
    options.add(logscanner);
}

void Command::setSMOptions(sm_options& sm_opt, const po::variables_map& values)
{
    BOOST_FOREACH(const po::variables_map::value_type& pair, values)
    {
        const std::string& key = pair.first;
        try {
            sm_opt.set_int_option(key, values[key].as<int>());
        }
        catch(boost::bad_any_cast const& e) {
            try {
                cerr << "Set option " << key << " to " << values[key].as<bool>() << endl;
                sm_opt.set_bool_option(key, values[key].as<bool>());
            }
            catch(boost::bad_any_cast const& e) {
                try {
                    sm_opt.set_string_option(key, values[key].as<string>());
                }
                catch(boost::bad_any_cast const& e) {
                    try {
                        sm_opt.set_int_option(key, values[key].as<uint>());
                    }
                    catch(boost::bad_any_cast const& e) {
                        cerr << "Could not process option " << key
                            << " .. skippking." << endl;
                        continue;
                    }
                }
            }
        }
    };
}
