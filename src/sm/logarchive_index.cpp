#include "logarchive_index.h"

#include <boost/regex.hpp>
#include <sys/stat.h>
#include <fcntl.h>
#include <sstream>

#include "w_debug.h"
#include "lsn.h"
#include "latches.h"
#include "sm_base.h"
#include "log_core.h"
#include "stopwatch.h"

// definition of static members
const string ArchiveDirectory::RUN_PREFIX = "archive_";
const string ArchiveDirectory::CURR_RUN_FILE = "current_run";
const string ArchiveDirectory::CURR_MERGE_FILE = "current_merge";
const string ArchiveDirectory::run_regex =
    "^archive_([1-9][0-9]*)_([1-9][0-9]*\\.[0-9]+)-([1-9][0-9]*\\.[0-9]+)$";
const string ArchiveDirectory::current_regex = "current_run|current_merge";

// CS TODO: Aligning with the Linux standard FS block size
// We could try using 512 (typical hard drive sector) at some point,
// but none of this is actually standardized or portable
const size_t IO_ALIGN = 512;

// CS TODO
const static int DFT_BLOCK_SIZE = 1024 * 1024; // 1MB = 128 pages

baseLogHeader SKIP_LOGREC;

// TODO proper exception mechanism
#define CHECK_ERRNO(n) \
    if (n == -1) { \
        W_FATAL_MSG(fcOS, << "Kernel errno code: " << errno); \
    }

bool ArchiveDirectory::parseRunFileName(string fname, RunFileStats& fstats)
{
    boost::regex run_rx(run_regex, boost::regex::perl);
    boost::smatch res;
    if (!boost::regex_match(fname, res, run_rx)) { return false; }

    fstats.level = std::stoi(res[1]);

    std::stringstream is;
    is.str(res[2]);
    is >> fstats.beginLSN;
    is.clear();
    is.str(res[3]);
    is >> fstats.endLSN;

    return true;
}

lsn_t ArchiveDirectory::getLastLSN()
{
    // CS TODO index mandatory
    w_assert0(archIndex);
    return archIndex->getLastLSN(1 /* level */);
}

size_t ArchiveDirectory::getFileSize(int fd)
{
    struct stat stat;
    auto ret = ::fstat(fd, &stat);
    CHECK_ERRNO(ret);
    return stat.st_size;
}

ArchiveDirectory::ArchiveDirectory(const sm_options& options)
{
    archdir = options.get_string_option("sm_archdir", "archive");
    // CS TODO: archiver currently only works with 1MB blocks
    blockSize = DFT_BLOCK_SIZE;
        // options.get_int_option("sm_archiver_block_size", DFT_BLOCK_SIZE);
    size_t bucketSize =
        options.get_int_option("sm_archiver_bucket_size", 128);
    w_assert0(bucketSize > 0);

    bool reformat = options.get_bool_option("sm_format", false);

    if (archdir.empty()) {
        W_FATAL_MSG(fcINTERNAL,
                << "Option for archive directory must be specified");
    }

    if (!fs::exists(archdir)) {
        if (reformat) {
            fs::create_directories(archdir);
        } else {
            cerr << "Error: could not open the log directory " << archdir <<endl;
            W_COERCE(RC(eOS));
        }
    }

    maxLevel = 0;
    archpath = archdir;
    fs::directory_iterator it(archpath), eod;
    boost::regex current_rx(current_regex, boost::regex::perl);
    lsn_t highestLSN = lsn_t::null;

    for (; it != eod; it++) {
        fs::path fpath = it->path();
        string fname = fpath.filename().string();
        RunFileStats fstats;

        if (parseRunFileName(fname, fstats)) {
            if (reformat) {
                fs::remove(fpath);
                continue;
            }
            // parse lsn from file name
            lsn_t currLSN = fstats.endLSN;
            if (currLSN > highestLSN) {
                DBGTHRD(<< "Highest LSN found so far in archdir: " << currLSN);
                highestLSN = currLSN;
            }
            if (fstats.level > maxLevel) { maxLevel = fstats.level; }
        }
        else if (boost::regex_match(fname, current_rx)) {
            DBGTHRD(<< "Found unfinished log archive run. Deleting");
            fs::remove(fpath);
        }
        else {
            cerr << "ArchiveDirectory cannot parse filename " << fname << endl;
            W_FATAL(fcINTERNAL);
        }
    }
    startLSN = highestLSN;

    // no runs found in archive log -- start from first available log file
    if (startLSN.hi() == 0 && smlevel_0::log) {
        int nextPartition = startLSN.hi();

        int max = smlevel_0::log->durable_lsn().hi();

        while (nextPartition <= max) {
            string fname = smlevel_0::log->make_log_name(nextPartition);
            if (fs::exists(fname)) { break; }
            nextPartition++;
        }

        if (nextPartition > max) {
            W_FATAL_MSG(fcINTERNAL,
                << "Could not find partition files in log manager");
        }

        startLSN = lsn_t(nextPartition, 0);
    }

    // nothing worked -- start from 1.0 and hope for the best
    if (startLSN.hi() == 0) {
        startLSN = lsn_t(1,0);
    }

    // create/load index
    archIndex = new ArchiveIndex(blockSize, bucketSize);

    {
        int fd;
        std::list<RunFileStats> runFiles;
        listFileStats(runFiles);
        for(auto f : runFiles) {
            W_COERCE(openForScan(fd, f.beginLSN, f.endLSN, f.level));
            W_COERCE(archIndex->loadRunInfo(fd, f));
            W_COERCE(closeScan(fd));
        }

        // sort runinfo vector by lsn
        if (runFiles.size() > 0) {
            archIndex->init();
        }
    }

    // CS TODO this should be initialized statically, but whatever...
    memset(&SKIP_LOGREC, 0, sizeof(baseLogHeader));
    SKIP_LOGREC._len = sizeof(baseLogHeader);
    SKIP_LOGREC._type = logrec_t::t_skip;
    SKIP_LOGREC._cat = 1; // t_status is protected...

    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));

    // ArchiveDirectory invariant is that current_run file always exists
    openNewRun(1);
}

ArchiveDirectory::~ArchiveDirectory()
{
    if(archIndex) {
        delete archIndex;
    }
    DO_PTHREAD(pthread_mutex_destroy(&mutex));
}

void ArchiveDirectory::listFiles(std::vector<std::string>& list,
        int level)
{
    list.clear();

    // CS TODO unify with listFileStats
    fs::directory_iterator it(archpath), eod;
    for (; it != eod; it++) {
        string fname = it->path().filename().string();
        RunFileStats fstats;
        if (parseRunFileName(fname, fstats)) {
            if (level < 0 || level == static_cast<int>(fstats.level)) {
                list.push_back(fname);
            }
        }
    }
}

void ArchiveDirectory::listFileStats(list<RunFileStats>& list,
        int level)
{
    list.clear();
    if (level > static_cast<int>(maxLevel)) { return; }

    vector<string> fnames;
    listFiles(fnames, level);

    RunFileStats stats;
    for (size_t i = 0; i < fnames.size(); i++) {
        parseRunFileName(fnames[i], stats);
        list.push_back(stats);
    }
}

/**
 * Opens a new run file of the log archive, closing the current run
 * if it exists. Upon closing, the file is renamed to contain the LSN
 * range of the log records contained in that run. The upper boundary
 * (lastLSN) is exclusive, meaning that it will be found on the beginning
 * of the following run. This also allows checking the filenames for any
 * any range of the LSNs which was "lost" when archiving.
 *
 * We assume the rename operation is atomic, even in case of OS crashes.
 *
 */
rc_t ArchiveDirectory::openNewRun(unsigned level)
{
    if (appendFd.size() > level && appendFd[level] >= 0) {
        return RC(fcINTERNAL);
    }

    int flags = O_WRONLY | O_SYNC | O_CREAT;
    std::string fname = archdir + "/" + CURR_RUN_FILE;
    auto fd = ::open(fname.c_str(), flags, 0744 /*mode*/);
    CHECK_ERRNO(fd);
    DBGTHRD(<< "Opened new output run in level " << level);

    appendFd.resize(level+1, -1);
    appendFd[level] = fd;
    appendPos.resize(level+1, 0);
    appendPos[level] = 0;
    return RCOK;
}

fs::path ArchiveDirectory::make_run_path(lsn_t begin, lsn_t end, unsigned level)
    const
{
    return archpath / fs::path(RUN_PREFIX + std::to_string(level) + "_" + begin.str()
            + "-" + end.str());
}

fs::path ArchiveDirectory::make_current_run_path() const
{
    return archpath / fs::path(CURR_RUN_FILE);
}

rc_t ArchiveDirectory::closeCurrentRun(lsn_t runEndLSN, unsigned level)
{
    CRITICAL_SECTION(cs, mutex);

    if (appendFd[level] >= 0) {
        if (appendPos[level] == 0 && runEndLSN == lsn_t::null) {
            // nothing was appended -- just close file and return
            auto ret = ::close(appendFd[level]);
            CHECK_ERRNO(ret);
            appendFd[level] = -1;
            return RCOK;
        }

        // CS TODO from now on, archiveIndex is mandatory
        // CS TODO unify ArchiveDirectory and ArchiveIndex
        w_assert0(archIndex);
        lsn_t lastLSN = archIndex->getLastLSN(level);
        if (lastLSN != runEndLSN) {
            // register index information and write it on end of file
            if (archIndex && appendPos[level] > 0) {
                // take into account space for skip log record
                appendPos[level] += sizeof(baseLogHeader);
                // and make sure data is written aligned to block boundary
                appendPos[level] -= appendPos[level] % blockSize;
                appendPos[level] += blockSize;
                archIndex->finishRun(lastLSN, runEndLSN, appendFd[level], appendPos[level], level);
            }

            fs::path new_path = make_run_path(lastLSN, runEndLSN, level);
            fs::rename(make_current_run_path(), new_path);

            DBGTHRD(<< "Closing current output run: " << new_path.string());
        }

        auto ret = ::close(appendFd[level]);
        CHECK_ERRNO(ret);
        appendFd[level] = -1;
    }

    openNewRun(level);

    return RCOK;
}

rc_t ArchiveDirectory::append(char* data, size_t length, unsigned level)
{
    // make sure there is always a skip log record at the end
    w_assert1(length + sizeof(baseLogHeader) <= blockSize);
    memcpy(data + length, &SKIP_LOGREC, sizeof(baseLogHeader));

    // beginning of block must be a valid log record
    w_assert1(reinterpret_cast<logrec_t*>(data)->valid_header());

    INC_TSTAT(la_block_writes);
    auto ret = ::pwrite(appendFd[level], data, length + sizeof(baseLogHeader),
                appendPos[level]);
    CHECK_ERRNO(ret);
    appendPos[level] += length;
    return RCOK;
}

rc_t ArchiveDirectory::openForScan(int& fd, lsn_t runBegin,
        lsn_t runEnd, unsigned level)
{
    fs::path fpath = make_run_path(runBegin, runEnd, level);

    // Using direct I/O
    int flags = O_RDONLY | O_DIRECT;
    fd = ::open(fpath.string().c_str(), flags, 0744 /*mode*/);
    CHECK_ERRNO(fd);

    return RCOK;
}

/** Note: buffer must be allocated for at least readSize + IO_ALIGN bytes,
 * otherwise direct I/O with alignment will corrupt memory.
 */
rc_t ArchiveDirectory::readBlock(int fd, char* buf,
        size_t& offset, size_t readSize)
{
    stopwatch_t timer;

    if (readSize == 0) { readSize = blockSize; }
    size_t actualOffset = IO_ALIGN * (offset / IO_ALIGN);
    size_t diff = offset - actualOffset;
    // make sure we don't read more than a block worth of data
    w_assert1(actualOffset <= offset);
    w_assert1(offset % blockSize != 0 || readSize == blockSize);
    w_assert1(diff < IO_ALIGN);

    size_t actualReadSize = readSize + diff;
    if (actualReadSize % IO_ALIGN != 0) {
        actualReadSize = (1 + actualReadSize / IO_ALIGN) * IO_ALIGN;
    }

    int howMuchRead = ::pread(fd, buf, actualReadSize, actualOffset);
    CHECK_ERRNO(howMuchRead);
    if (howMuchRead == 0) {
        // EOF is signalized by setting offset to zero
        offset = 0;
        return RCOK;
    }

    if (diff > 0) {
        memmove(buf, buf + diff, readSize);
    }

    ADD_TSTAT(la_read_time, timer.time_us());
    ADD_TSTAT(la_read_volume, howMuchRead);
    INC_TSTAT(la_read_count);

    offset += readSize;
    return RCOK;
}

rc_t ArchiveDirectory::closeScan(int& fd)
{
    auto ret = ::close(fd);
    CHECK_ERRNO(ret);
    fd = -1;
    return RCOK;
}

void ArchiveDirectory::deleteAllRuns()
{
    fs::directory_iterator it(archpath), eod;
    boost::regex run_rx(run_regex, boost::regex::perl);
    for (; it != eod; it++) {
        string fname = it->path().filename().string();
        if (boost::regex_match(fname, run_rx)) {
            fs::remove(it->path());
        }
    }
}

ArchiveIndex::ArchiveIndex(size_t blockSize, size_t bucketSize)
    : blockSize(blockSize), bucketSize(bucketSize), maxLevel(0)
{
    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
}

ArchiveIndex::~ArchiveIndex()
{
}

void ArchiveIndex::newBlock(const vector<pair<PageID, size_t> >&
        buckets, unsigned level)
{
    CRITICAL_SECTION(cs, mutex);

    w_assert1(bucketSize > 0);

    size_t prevOffset = 0;
    for (size_t i = 0; i < buckets.size(); i++) {
        BlockEntry e;
        e.pid = buckets[i].first;
        e.offset = buckets[i].second;
        w_assert1(e.offset == 0 || e.offset > prevOffset);
        prevOffset = e.offset;
        runs[level].back().entries.push_back(e);
    }
}

rc_t ArchiveIndex::finishRun(lsn_t first, lsn_t last, int fd,
        off_t offset, unsigned level)
{
    CRITICAL_SECTION(cs, mutex);
    w_assert1(offset % blockSize == 0);

    // check if it isn't an empty run (from truncation)
    int& lf = lastFinished[level];
    if (offset > 0 && lf < (int) runs[level].size()) {
        lf++;
        w_assert1(lf == 0 || first == runs[level][lf-1].lastLSN);
        w_assert1(lf < (int) runs[level].size());

        runs[level][lf].firstLSN = first;
        runs[level][lf].lastLSN = last;
        W_DO(serializeRunInfo(runs[level][lf], fd, offset));
    }

    return RCOK;
}

rc_t ArchiveIndex::serializeRunInfo(RunInfo& run, int fd,
        off_t offset)
{
    // Assumption: mutex is held by caller

    // lastPID is stored on first block, but we reserve space for it in every
    // block to simplify things
    int entriesPerBlock =
        (blockSize - sizeof(BlockHeader) - sizeof(PageID)) / sizeof(BlockEntry);
    int remaining = run.entries.size();
    int i = 0;
    size_t currEntry = 0;

    // CS TODO RAII
    char * writeBuffer = new char[blockSize];

    while (remaining > 0) {
        int j = 0;
        size_t bpos = sizeof(BlockHeader);
        while (j < entriesPerBlock && remaining > 0)
        {
            memcpy(writeBuffer + bpos, &run.entries[currEntry],
                        sizeof(BlockEntry));
            j++;
            currEntry++;
            remaining--;
            bpos += sizeof(BlockEntry);
        }
        BlockHeader* h = (BlockHeader*) writeBuffer;
        h->entries = j;
        h->blockNumber = i;

        // copy lastPID into last block (space was reserved above)
        // if (remaining == 0) {
        //     memcpy(writeBuffer + bpos, &run.lastPID, sizeof(PageID));
        // }

        auto ret = ::pwrite(fd, writeBuffer, blockSize, offset);
        CHECK_ERRNO(ret);
        offset += blockSize;
        i++;
    }

    delete[] writeBuffer;

    return RCOK;
}

void ArchiveIndex::init()
{
    for (unsigned l = 0; l < runs.size(); l++) {
        std::sort(runs[l].begin(), runs[l].end());
    }
}

void ArchiveIndex::appendNewEntry(unsigned level)
{
    CRITICAL_SECTION(cs, mutex);

    // if (runs.size() > 0) {
    //     runs.back().lastPID = lastPID;
    // }

    RunInfo newRun;
    if (level > maxLevel) {
        maxLevel = level;
        runs.resize(maxLevel+1);
        lastFinished.resize(maxLevel+1, -1);
    }
    runs[level].push_back(newRun);
}

lsn_t ArchiveIndex::getLastLSN(unsigned level)
{
    CRITICAL_SECTION(cs, mutex);

    if (level > maxLevel) { return lsn_t::null; }

    if (lastFinished[level] < 0) {
        // No runs exist in the given level. If a previous level exists, it
        // must be the first LSN in that level; otherwise, it's simply 1.0
        if (level == 0) { return lsn_t(1,0); }
        return getFirstLSN(level - 1);
    }

    return runs[level][lastFinished[level]].lastLSN;
}

lsn_t ArchiveIndex::getFirstLSN(unsigned level)
{
    if (level <= 1) { return lsn_t(1,0); }
    // If no runs exist at this level, recurse down to previous level;
    if (lastFinished[level] < 0) { return getFirstLSN(level-1); }

    return runs[level][0].firstLSN;
}

rc_t ArchiveIndex::loadRunInfo(int fd, const ArchiveDirectory::RunFileStats& fstats)
{
    RunInfo run;
    {
        memalign_allocator<char, IO_ALIGN> alloc;
        char* readBuffer = alloc.allocate(blockSize);

        size_t indexBlockCount = 0;
        size_t dataBlockCount = 0;
        W_DO(getBlockCounts(fd, &indexBlockCount, &dataBlockCount));

        off_t offset = dataBlockCount * blockSize;
        w_assert1(dataBlockCount == 0 || offset > 0);
        size_t lastOffset = 0;

        while (indexBlockCount > 0) {
            auto bytesRead = ::pread(fd, readBuffer, blockSize, offset);
            CHECK_ERRNO(bytesRead);
            if (bytesRead != blockSize) { return RC(stSHORTIO); }

            BlockHeader* h = (BlockHeader*) readBuffer;

            unsigned j = 0;
            size_t bpos = sizeof(BlockHeader);
            while(j < h->entries)
            {
                BlockEntry* e = (BlockEntry*)(readBuffer + bpos);
                w_assert1(lastOffset == 0 || e->offset > lastOffset);
                run.entries.push_back(*e);

                lastOffset = e->offset;
                bpos += sizeof(BlockEntry);
                j++;
            }
            indexBlockCount--;
            offset += blockSize;

            // if (indexBlockCount == 0) {
            //     // read lasPID from last block
            //     run.lastPID = *((PageID*) (readBuffer + bpos));
            // }
        }

        alloc.deallocate(readBuffer);
    }

    run.firstLSN = fstats.beginLSN;
    run.lastLSN = fstats.endLSN;

    if (fstats.level > maxLevel) {
        maxLevel = fstats.level;
        // level 0 reserved, so add 1
        runs.resize(maxLevel+1);
        lastFinished.resize(maxLevel+1);
    }
    runs[fstats.level].push_back(run);
    lastFinished[fstats.level] = runs[fstats.level].size() - 1;

    return RCOK;
}

rc_t ArchiveIndex::getBlockCounts(int fd, size_t* indexBlocks,
        size_t* dataBlocks)
{
    size_t fsize = ArchiveDirectory::getFileSize(fd);
    w_assert1(fsize % blockSize == 0);

    // skip emtpy runs
    if (fsize == 0) {
        if(indexBlocks) { *indexBlocks = 0; };
        if(dataBlocks) { *dataBlocks = 0; };
        return RCOK;
    }

    // read header of last block in file -- its number is the block count
    // Using direct I/O -- must read whole align block
    char* buffer;
    int res = posix_memalign((void**) &buffer, IO_ALIGN, IO_ALIGN);
    w_assert0(res == 0);

    auto bytesRead = ::pread(fd, buffer, IO_ALIGN, fsize - blockSize);
    CHECK_ERRNO(bytesRead);
    if (bytesRead != IO_ALIGN) { return RC(stSHORTIO); }

    BlockHeader* header = (BlockHeader*) buffer;
    if (indexBlocks) {
        *indexBlocks = header->blockNumber + 1;
    }
    if (dataBlocks) {
        *dataBlocks = (fsize / blockSize) - (header->blockNumber + 1);
        w_assert1(*dataBlocks > 0);
    }
    free(buffer);

    return RCOK;
}

size_t ArchiveIndex::findRun(lsn_t lsn, unsigned level)
{
    // Assumption: mutex is held by caller
    if (lsn == lsn_t::null) {
        // full log replay (backup-less)
        return 0;
    }

    /*
     * CS: requests are more likely to access the last runs, so
     * we do a linear search instead of binary search.
     */
    auto& lf = lastFinished[level];
    w_assert1(lf >= 0);

    if(lsn >= runs[level][lf].lastLSN) {
        return lf + 1;
    }

    int result = lf;
    while (result > 0 && runs[level][result].firstLSN > lsn) {
        result--;
    }

    // skip empty runs
    while (runs[level][result].entries.size() == 0 && result <= lf) {
        result++;
    }

    // caller must check if returned index is valid
    return result >= 0 ? result : runs[level].size();
}

size_t ArchiveIndex::findEntry(RunInfo* run,
        PageID pid, int from, int to)
{
    // Assumption: mutex is held by caller

    if (from > to) {
        if (from == 0) {
            // Queried pid lower than first in run
            return 0;
        }
        // Queried pid is greater than last in run.  This should not happen
        // because probes must not consider this run if that's the case
        W_FATAL_MSG(fcINTERNAL, << "Invalid probe on archiver index! "
                << " PID = " << pid << " run = " << run->firstLSN);
    }

    // negative value indicates first invocation
    if (to < 0) { to = run->entries.size() - 1; }
    if (from < 0) { from = 0; }

    w_assert1(run);
    w_assert1(run->entries.size() > 0);

    // binary search for page ID within run
    size_t i;
    if (from == to) {
        i = from;
    }
    else {
        i = from/2 + to/2;
    }

    w_assert0(i < run->entries.size());

    if (run->entries[i].pid <= pid &&
            (i == run->entries.size() - 1 || run->entries[i+1].pid >= pid))
    {
        // found it! must first check if previous does not contain same pid
        while (i > 0 && run->entries[i].pid == pid)
                //&& run->entries[i].pid == run->entries[i-1].pid)
        {
            i--;
        }
        return i;
    }

    // not found: recurse down
    if (run->entries[i].pid > pid) {
        return findEntry(run, pid, from, i-1);
    }
    else {
        return findEntry(run, pid, i+1, to);
    }
}

void ArchiveIndex::probeInRun(ProbeResult& res)
{
    // Assmuptions: mutex is held; run index and pid are set in given result
    size_t index = res.runIndex;
    auto level = res.level;
    w_assert1((int) index <= lastFinished[level]);
    RunInfo* run = &runs[level][index];

    res.runBegin = runs[level][index].firstLSN;
    res.runEnd = runs[level][index].lastLSN;

    size_t entryBegin = 0;
    if (res.pidBegin == 0) {
        res.offset = 0;
    }
    else {
        entryBegin = findEntry(run, res.pidBegin);
        // decide if we mean offset zero or entry zero
        if (entryBegin == 0 && run->entries[0].pid >= res.pidBegin)
        {
            res.offset = 0;
        }
        else {
            res.offset = run->entries[entryBegin].offset;
        }
    }
}

void ArchiveIndex::probe(std::vector<ProbeResult>& probes,
        PageID startPID, PageID endPID, lsn_t startLSN)
{
    CRITICAL_SECTION(cs, mutex);

    probes.clear();
    unsigned level = maxLevel;

    // Start collecting runs on the max level, which has the largest runs
    // and therefore requires the least random reads
    while (level > 0) {
        size_t index = findRun(startLSN, level);

        ProbeResult res;
        res.level = level;
        while ((int) index <= lastFinished[level]) {
            if (runs[level][index].entries.size() > 0) {
                res.pidBegin = startPID;
                res.pidEnd = endPID;
                res.runIndex = index;
                probeInRun(res);
                probes.push_back(res);
            }
            index++;
        }

        // Now go to the next level, starting on the last LSN covered
        // by the current level
        startLSN = res.runEnd;
        level--;
    }
}

void ArchiveIndex::dumpIndex(ostream& out)
{
    for (auto r : runs) {
        for (size_t i = 0; i < r.size(); i++) {
            size_t offset = 0, prevOffset = 0;
            for (size_t j = 0; j < r[i].entries.size(); j++) {
                offset = r[i].entries[j].offset;
                out << "run " << i << " entry " << j <<
                    " pid " << r[i].entries[j].pid <<
                    " offset " << offset <<
                    " delta " << offset - prevOffset <<
                    endl;
                prevOffset = offset;
            }
        }
    }
}
