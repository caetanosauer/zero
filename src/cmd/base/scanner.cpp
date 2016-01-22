#include "scanner.h"

#include <chkpt.h>
#include <sm.h>
#include <restart.h>
#include <vol.h>

#define PARSE_LSN(a,b) \
    LogArchiver::ArchiveDirectory::parseLSN(a, b);


void BaseScanner::handle(logrec_t* lr)
{
    logrec_t& r = *lr;
    size_t i;
    for (i=0; i < any_handlers.size(); i++)
        any_handlers.at(i)->invoke(r);
    for (i=0; i < pid_handlers.size(); i++)
        pid_handlers.at(i)->invoke(r);
    if (r.is_single_sys_xct() || r.tid() != tid_t::null){
        for (i=0; i < transaction_handlers.size(); i++)
            transaction_handlers.at(i)->invoke(r);
    }
    for (i=0; type_handlers.size() > 0 && i < type_handlers.at(r.type()).size(); i++)
        type_handlers.at(r.type()).at(i)->invoke(r);
}

void BaseScanner::finalize()
{
    size_t i;
    for (i=0; i < any_handlers.size(); i++)
        any_handlers.at(i)->finalize();
    for (i=0; i < pid_handlers.size(); i++)
        pid_handlers.at(i)->finalize();
    for (i=0; i < transaction_handlers.size(); i++)
        transaction_handlers.at(i)->finalize();
    for (i=0; type_handlers.size() > 0 && i < logrec_t::t_max_logrec; i++)
    {
        for (size_t j=0; type_handlers.at(i).size() > 0 &&
                j < type_handlers.at(i).size(); j++)
        {
            type_handlers.at(i).at(j)->finalize();
        }
    }
}

BlockScanner::BlockScanner(const po::variables_map& options,
        bitset<logrec_t::t_max_logrec>* filter)
    : BaseScanner(options), pnum(-1)
{
    logdir = options["logdir"].as<string>().c_str();
    blockSize = options["sm_archiver_block_size"].as<int>();
    logScanner = new LogScanner(blockSize);
    currentBlock = new char[blockSize];

    if (filter) {
        logScanner->ignoreAll();
        for (int i = 0; i < logrec_t::t_max_logrec; i++) {
            if (filter->test(i)) {
                logScanner->unsetIgnore((logrec_t::kind_t) i);
            }
        }
        // skip cannot be ignored because it tells us when file ends
        logScanner->unsetIgnore(logrec_t::t_skip);
    }
}

void BlockScanner::findFirstFile()
{
    pnum = numeric_limits<int>::max();
    os_dir_t dir = os_opendir(logdir);
    if (!dir) {
        smlevel_0::errlog->clog << fatal_prio <<
            "Error: could not open recovery log dir: " <<
            logdir << flushl;
        W_COERCE(RC(fcOS));
    }
    os_dirent_t* entry = os_readdir(dir);
    const char * PREFIX = "log.";

    while (entry != NULL) {
        const char* fname = entry->d_name;
        if (strncmp(PREFIX, fname, strlen(PREFIX)) == 0) {
            int p = atoi(fname + strlen(PREFIX));
            if (p < pnum) {
                pnum = p;
            }
        }
        entry = os_readdir(dir);
    }
    os_closedir(dir);
}

string BlockScanner::getNextFile()
{
    stringstream fname;
    fname << logdir << "/";
    if (pnum < 0) {
        findFirstFile();
    }
    else {
        pnum++;
    }
    fname << "log." << pnum;

    if (openFileCallback) {
        openFileCallback(fname.str().c_str());
    }

    return fname.str();
}

void BlockScanner::run()
{
    size_t bpos = 0;
    streampos fpos = 0, fend = 0;
    //long count = 0;
    int firstPartition = pnum;
    logrec_t* lr = NULL;

    while (true) {
        // open partition number pnum
        string fname = restrictFile.empty() ? getNextFile() : restrictFile;
        ifstream in(fname, ios::binary | ios::ate);

        // does the file exist?
        if (!in.good()) {
            in.close();
            break;
        }

        // file is opened at the end
        fend = in.tellg();
        fpos = 0;

        cerr << "Scanning log file " << fname << endl;

        while (fpos < fend) {
            //cerr << "Reading block at " << fpos << " from " << fname.str();

            // read next block from partition file
            in.seekg(fpos);
            if (in.fail()) {
                throw runtime_error("IO error seeking into file");
            }
            in.read(currentBlock, blockSize);
            if (in.eof()) {
                // partial read on end of file
                fpos = fend;
            }
            else if (in.gcount() == 0) {
                // file ended exactly on block boundary
                break;
            }
            else if (in.fail()) {
                // EOF implies fail, so we check it first
                throw runtime_error("IO error reading block from file");
            }
            else {
                fpos += blockSize;
            }

            //cerr << " - " << in.gcount() << " bytes OK" << endl;

            bpos = 0;
            while (logScanner->nextLogrec(currentBlock, bpos, lr)) {
                handle(lr);
                if (lr->type() == logrec_t::t_skip) {
                    fpos = fend;
                    break;
                }
            }
        }

        in.close();

        if (!restrictFile.empty()) {
            break;
        }
    }

    if (pnum == firstPartition && bpos == 0) {
        throw runtime_error("Could not find/open log files in "
                + string(logdir));
    }

    BaseScanner::finalize();
}

BlockScanner::~BlockScanner()
{
    delete currentBlock;
    delete logScanner;
}


LogArchiveScanner::LogArchiveScanner(const po::variables_map& options)
    : BaseScanner(options), runBegin(lsn_t::null), runEnd(lsn_t::null)
{
    archdir = options["logdir"].as<string>();
}

bool runCompare (string a, string b)
{
    lsn_t lsn_a = PARSE_LSN(a.c_str(), false);
    lsn_t lsn_b = PARSE_LSN(b.c_str(), false);
    return lsn_a < lsn_b;
}

void LogArchiveScanner::run()
{
    size_t blockSize = options["sm_archiver_block_size"].as<int>();
    LogArchiver::ArchiveDirectory* directory = new
        LogArchiver::ArchiveDirectory(archdir, blockSize);

    std::vector<std::string> runFiles;

    if (restrictFile.empty()) {
        directory->listFiles(runFiles);
        std::sort(runFiles.begin(), runFiles.end(), runCompare);
    }
    else {
        runFiles.push_back(restrictFile);
    }

    runBegin = PARSE_LSN(runFiles[0].c_str(), false);
    runEnd = PARSE_LSN(runFiles[0].c_str(), true);
    std::vector<std::string>::const_iterator it;
    for(size_t i = 0; i < runFiles.size(); i++) {
        if (i > 0) {
            // begin of run i must be equal to end of run i-1
            runBegin = PARSE_LSN(runFiles[i].c_str(), false);
            if (runBegin != runEnd) {
                throw runtime_error("Hole found in run boundaries!");
            }
            runEnd = PARSE_LSN(runFiles[i].c_str(), true);
        }

        if (openFileCallback) {
            openFileCallback(runFiles[i].c_str());
        }

        LogArchiver::ArchiveScanner::RunScanner* rs =
            new LogArchiver::ArchiveScanner::RunScanner(
                    runBegin,
                    runEnd,
                    0, // first PID
                    0, // last PID
                    0,            // file offset
                    directory
            );

        lsn_t prevLSN = lsn_t::null;
        PageID prevPid = 0;

        logrec_t* lr;
        while (rs->next(lr)) {
            w_assert1(lr->pid() >= prevPid);
            w_assert1(lr->pid() != prevPid ||
                    lr->page_prev_lsn() == lsn_t::null ||
                    lr->page_prev_lsn() == prevLSN);
            w_assert1(lr->lsn_ck() >= runBegin);
            w_assert1(lr->lsn_ck() < runEnd);

            handle(lr);

            prevLSN = lr->lsn_ck();
            prevPid = lr->pid();
        };

        delete rs;
    }

    BaseScanner::finalize();
}

MergeScanner::MergeScanner(const po::variables_map& options)
    : BaseScanner(options)
{
    archdir = options["logdir"].as<string>();
}

void MergeScanner::run()
{
    // CS TODO blockSize not used anymore
    size_t blockSize = options["sm_archiver_block_size"].as<int>();
    size_t bucketSize = options["sm_archiver_bucket_size"].as<int>();

    LogArchiver::ArchiveDirectory* directory = new
        LogArchiver::ArchiveDirectory(archdir, blockSize, bucketSize);
    LogArchiver::ArchiveScanner logScan(directory);

    LogArchiver::ArchiveScanner::RunMerger* merger =
        logScan.open(0, 0, lsn_t::null, blockSize);

    logrec_t* lr;

    lsn_t prevLSN = lsn_t::null;
    PageID prevPid = 0;

    while (merger && merger->next(lr)) {
        w_assert1(lr->pid() >= prevPid);
        w_assert1(lr->pid() != prevPid ||
                lr->page_prev_lsn() == lsn_t::null ||
                lr->page_prev_lsn() == prevLSN);

        handle(lr);

        prevLSN = lr->lsn_ck();
        prevPid = lr->pid();
    }

    BaseScanner::finalize();
}

