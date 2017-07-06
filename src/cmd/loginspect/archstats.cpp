#include "archstats.h"

sm_options smopt;
shared_ptr<ArchiveIndex> archIndex;

void ArchStats::setupOptions()
{
    LogScannerCommand::setupOptions();
    boost::program_options::options_description opt("ArchStats Options");
    opt.add_options()
        ("printStats", po::value<bool>(&printStats)
         ->default_value(true)->implicit_value(true),
            "Print run stats: number of data and index blocks")
        ("dumpIndex", po::value<bool>(&dumpIndex)
         ->default_value(false)->implicit_value(true),
            "Print all entries on the index")
        ("scan", po::value<bool>(&scan)
         ->default_value(false)->implicit_value(true),
            "Derive and print index entries (pid-offset pairs) by scanning runs")
    ;
    options.add(opt);
}

void ArchStats::printRunInfo(const RunId& runid)
{
    auto runFile = archIndex->openForScan(runid);
    size_t indexBlockCount = 0, dataBlockCount = 0;
    archIndex->getBlockCounts(runFile, &indexBlockCount, &dataBlockCount);
    std::cout << "level " << runid.level
        << " begin " << runid.beginLSN
        << " end " << runid.endLSN
        << " data " << dataBlockCount
        << " index " << indexBlockCount
        << " index_percent " << (double) indexBlockCount / dataBlockCount
        << std::endl;
}

void ArchStats::run()
{
    smopt.set_string_option("sm_archdir", logdir);
    archIndex = make_shared<ArchiveIndex>(smopt);

    std::vector<RunId> runs;
    if (!filename.empty()) {
        RunId runid;
        ArchiveIndex::parseRunFileName(filename, runid);
        runs.push_back(runid);
    }
    else {
        archIndex->listRunsNonOverlapping(std::back_inserter(runs));
    }

    if (printStats) {
        for (auto rid : runs) {
            printRunInfo(rid);
        }
    }

    if (dumpIndex) {
        for (auto rid : runs) {
            archIndex->dumpIndex(std::cout, rid);
        }
    }

    if (scan) {
        BaseScanner* s = getScanner();
        ArchStatsScanner h;
        s->add_handler(&h);
        s->fork();
        s->join();
        delete s;
    }
}

void ArchStatsScanner::invoke(logrec_t& r)
{
    auto pid = r.pid();
    if ((pid != currentPID || !started) && r.type() != t_skip)
    {
        started = true;
        std::cout << "pid " << pid
            << " offset " << pos
            << " delta " << pos - prevPos
            << std::endl;
        prevPos = pos;
        currentPID = pid;
    }
    pos += r.length();
}
