#include "logarchive_index.h"

#include <regex>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sstream>

#include "w_debug.h"
#include "lsn.h"
#include "latches.h"
#include "sm_base.h"
#include "log_core.h"
#include "stopwatch.h"
#include "worker_thread.h"
#include "restart.h"
#include "bf_tree.h"

class RunRecycler : public worker_thread_t
{
public:
    RunRecycler(unsigned replFactor, ArchiveIndex* archIndex)
        : archIndex(archIndex), replFactor(replFactor)
    {}

    virtual void do_work()
    {
        archIndex->deleteRuns(replFactor);
    }

    ArchiveIndex* archIndex;
    const unsigned replFactor;
};

// definition of static members
const string ArchiveIndex::RUN_PREFIX = "archive_";
const string ArchiveIndex::CURR_RUN_PREFIX = "current_run_";
const string ArchiveIndex::run_regex =
    "^archive_([1-9][0-9]*)_([1-9][0-9]*\\.[0-9]+)-([1-9][0-9]*\\.[0-9]+)$";
const string ArchiveIndex::current_regex = "^current_run_[1-9][0-9]*$";

// CS TODO: Aligning with the Linux standard FS block size
// We could try using 512 (typical hard drive sector) at some point,
// but none of this is actually standardized or portable
const size_t IO_ALIGN = 512;

// CS TODO
const static int DFT_BLOCK_SIZE = 1024 * 1024; // 1MB = 128 pages

skip_log SKIP_LOGREC;

// TODO proper exception mechanism
#define CHECK_ERRNO(n) \
    if (n == -1) { \
        W_FATAL_MSG(fcOS, << "Kernel errno code: " << errno); \
    }

bool ArchiveIndex::parseRunFileName(string fname, RunId& fstats)
{
    std::regex run_rx(run_regex);
    std::smatch res;
    if (!std::regex_match(fname, res, run_rx)) { return false; }

    fstats.level = std::stoi(res[1]);

    std::stringstream is;
    is.str(res[2]);
    is >> fstats.beginLSN;
    is.clear();
    is.str(res[3]);
    is >> fstats.endLSN;

    return true;
}

size_t ArchiveIndex::getFileSize(int fd)
{
    struct stat stat;
    auto ret = ::fstat(fd, &stat);
    CHECK_ERRNO(ret);
    return stat.st_size;
}

ArchiveIndex::ArchiveIndex(const sm_options& options)
{
    archdir = options.get_string_option("sm_archdir", "archive");
    // CS TODO: archiver currently only works with 1MB blocks
    blockSize = DFT_BLOCK_SIZE;
        // options.get_int_option("sm_archiver_block_size", DFT_BLOCK_SIZE);
    bucketSize = options.get_int_option("sm_archiver_bucket_size", 1);
    w_assert0(bucketSize > 0);

    bool reformat = options.get_bool_option("sm_format", false);

    directIO = options.get_bool_option("sm_arch_o_direct", false);

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
    std::regex current_rx(current_regex);

    // create/load index
    unsigned runsFound = 0;
    for (; it != eod; it++) {
        fs::path fpath = it->path();
        string fname = fpath.filename().string();
        RunId fstats;

        if (parseRunFileName(fname, fstats)) {
            if (reformat) {
                fs::remove(fpath);
                continue;
            }

            auto runFile = openForScan(fstats);
            loadRunInfo(runFile, fstats);
            closeScan(fstats);

            if (fstats.level > maxLevel) { maxLevel = fstats.level; }

            runsFound++;
        }
        else if (std::regex_match(fname, current_rx)) {
            DBGTHRD(<< "Found unfinished log archive run. Deleting");
            fs::remove(fpath);
        }
        else {
            cerr << "ArchiveIndex cannot parse filename " << fname << endl;
            W_FATAL(fcINTERNAL);
        }
    }

    for (unsigned l = 0; l < runs.size(); l++) {
        std::sort(runs[l].begin(), runs[l].end());
    }

    // no runs found in archive log -- start from first available log file
    if (runsFound == 0) {
        std::vector<partition_number_t> partitions;
        if (smlevel_0::log) {
            smlevel_0::log->get_storage()->list_partitions(partitions);
        }

        if (partitions.size() > 0) {
            auto nextPartition = partitions[0];
            if (nextPartition > 1) {
                // create empty run to fill in the missing gap
                auto startLSN = lsn_t(nextPartition, 0);
                openNewRun(1);
                closeCurrentRun(startLSN, 1);
                RunId fstats = {lsn_t(1,0), startLSN, 1};
                auto runFile = openForScan(fstats);
                loadRunInfo(runFile, fstats);
                closeScan(fstats);
            }
        }
    }

    SKIP_LOGREC.init_header(logrec_t::t_skip);
    SKIP_LOGREC.construct();

    unsigned replFactor = options.get_int_option("sm_archiver_replication_factor", 0);
    if (replFactor > 0) {
        // CS TODO -- not implemented, see comments on deleteRuns
        // runRecycler.reset(new RunRecycler {replFactor, this});
        // runRecycler->fork();
    }
}

ArchiveIndex::~ArchiveIndex()
{
    if (runRecycler) { runRecycler->stop(); }
}

void ArchiveIndex::listFiles(std::vector<std::string>& list, int level)
{
    list.clear();

    // CS TODO unify with listFileStats
    fs::directory_iterator it(archpath), eod;
    for (; it != eod; it++) {
        string fname = it->path().filename().string();
        RunId fstats;
        if (parseRunFileName(fname, fstats)) {
            if (level < 0 || level == static_cast<int>(fstats.level)) {
                list.push_back(fname);
            }
        }
    }
}

void ArchiveIndex::listFileStats(list<RunId>& list, int level)
{
    list.clear();
    if (level > static_cast<int>(getMaxLevel())) { return; }

    vector<string> fnames;
    listFiles(fnames, level);

    RunId stats;
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
rc_t ArchiveIndex::openNewRun(unsigned level)
{
    int flags = O_WRONLY | O_CREAT;
    std::string fname = make_current_run_path(level).string();
    auto fd = ::open(fname.c_str(), flags, 0744 /*mode*/);
    CHECK_ERRNO(fd);
    DBGTHRD(<< "Opened new output run in level " << level);


    {
        spinlock_write_critical_section cs(&_mutex);

        appendFd.resize(level+1, -1);
        appendFd[level] = fd;
        appendPos.resize(level+1, 0);
        appendPos[level] = 0;
    }

    return RCOK;
}

fs::path ArchiveIndex::make_run_path(lsn_t begin, lsn_t end, unsigned level)
    const
{
    return archpath / fs::path(RUN_PREFIX + std::to_string(level) + "_" + begin.str()
            + "-" + end.str());
}

fs::path ArchiveIndex::make_current_run_path(unsigned level) const
{
    return archpath / fs::path(CURR_RUN_PREFIX + std::to_string(level));
}

rc_t ArchiveIndex::closeCurrentRun(lsn_t runEndLSN, unsigned level, PageID maxPID)
{
    lsn_t lastLSN = getLastLSN(level);
    if (lastLSN.is_null()) {
        if (level == 1) {
            // run being created by archiver -- must be the last LSNof all levels
            lastLSN = getLastLSN();
        }
        else {
            // run being created by merge -- previous-level runs must exist
            W_FATAL_MSG(fcINTERNAL, << "Invalid archiver state, closing run "
                    << runEndLSN << " on level " << level);
        }
    }
    w_assert1(lastLSN < runEndLSN || runEndLSN.is_null());

    if (appendFd[level] >= 0) {
        if (lastLSN != runEndLSN && !runEndLSN.is_null()) {
            {
                spinlock_read_critical_section cs(&_mutex);
                // if level>1, runEndLSN must match the boundaries in the lower level
                if (level > 1) {
                    // CS TODO: when merging from one archive to another (e.g., if
                    // different --indir and --outdir options are used in the zapps
                    // MergeRuns command), roundToEndLSN will fail because there might
                    // be no lower level.
                    runEndLSN = roundToEndLSN(runEndLSN, level-1);
                    w_assert1(!runEndLSN.is_null());
                }
                // register index information and write it on end of file
                if (appendPos[level] > 0) {
                    // take into account space for skip log record
                    appendPos[level] += SKIP_LOGREC.length();
                    // and make sure data is written aligned to block boundary
                    appendPos[level] -= appendPos[level] % blockSize;
                    appendPos[level] += blockSize;
                }
            }

            finishRun(lastLSN, runEndLSN, maxPID, appendFd[level], appendPos[level], level);
            fs::path new_path = make_run_path(lastLSN, runEndLSN, level);
            fs::rename(make_current_run_path(level), new_path);

            DBGTHRD(<< "Closing current output run: " << new_path.string());
        }

        auto ret = ::fsync(appendFd[level]);
        CHECK_ERRNO(ret);

        ret = ::close(appendFd[level]);
        CHECK_ERRNO(ret);
        appendFd[level] = -1;

        // This step atomically "commits" the creation of the new run
        {
            spinlock_write_critical_section cs(&_open_file_mutex);

            lastFinished[level]++;
        }

        // Notify other services that depend on archived LSN
        if (level == 1) {
            if (smlevel_0::recovery) {
                smlevel_0::recovery->notify_archived_lsn(runEndLSN);
            }
            if (smlevel_0::bf) {
                smlevel_0::bf->notify_archived_lsn(runEndLSN);
            }
        }
    }

    openNewRun(level);

    return RCOK;
}

rc_t ArchiveIndex::append(char* data, size_t length, unsigned level)
{
    // make sure there is always a skip log record at the end
    w_assert1(length + SKIP_LOGREC.length() <= blockSize);
    memcpy(data + length, &SKIP_LOGREC, SKIP_LOGREC.length());

    // beginning of block must be a valid log record
    w_assert1(reinterpret_cast<logrec_t*>(data)->valid_header());

    INC_TSTAT(la_block_writes);
    auto ret = ::pwrite(appendFd[level], data, length + SKIP_LOGREC.length(),
                appendPos[level]);
    CHECK_ERRNO(ret);
    appendPos[level] += length;
    return RCOK;
}

RunFile* ArchiveIndex::openForScan(const RunId& runid)
{
    spinlock_write_critical_section cs(&_open_file_mutex);

    auto& file = _open_files[runid];

    if (file.refcount == 0) {
        fs::path fpath = make_run_path(runid.beginLSN, runid.endLSN, runid.level);
        int flags = O_RDONLY;
        if (directIO) { flags |= O_DIRECT; }
        file.fd = ::open(fpath.string().c_str(), flags, 0744 /*mode*/);
        CHECK_ERRNO(file.fd);
        file.length = ArchiveIndex::getFileSize(file.fd);
#ifdef USE_MMAP
        if (file.length > 0) {
            file.data = (char*) mmap(nullptr, file.length, PROT_READ, MAP_SHARED, file.fd, 0);
            CHECK_ERRNO((long) file.data);
        }
#endif
        file.refcount = 0;
        file.runid = runid;
    }

    file.refcount++;

    INC_TSTAT(la_open_count);

    return &file;
}

/** Note: buffer must be allocated for at least readSize + IO_ALIGN bytes,
 * otherwise direct I/O with alignment will corrupt memory.
 */
rc_t ArchiveIndex::readBlock(int fd, char* buf,
        size_t& offset, size_t readSize)
{
    stopwatch_t timer;

    if (readSize == 0) { readSize = blockSize; }
    size_t actualOffset = IO_ALIGN * (offset / IO_ALIGN);
    size_t diff = offset - actualOffset;
    w_assert1(actualOffset <= offset);
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

void ArchiveIndex::closeScan(const RunId& runid)
{
    spinlock_write_critical_section cs(&_open_file_mutex);

    auto it = _open_files.find(runid);
    w_assert1(it != _open_files.end());

    auto& count = it->second.refcount;
    // count--;

    if (count == 0) {
#ifdef USE_MMAP
        // if (it->second.data) {
        //     auto ret = munmap(it->second.data, it->second.length);
        //     CHECK_ERRNO(ret);
        // }
#endif
        // auto ret = ::close(it->second.fd);
        // CHECK_ERRNO(ret);
        // _open_files.erase(it);
    }
}

void ArchiveIndex::deleteRuns(unsigned replicationFactor)
{
    /*
     * CS TODO: deleting runs is not as trivial as I initially thought.
     * Here are some issues to be aware of:
     * - RunInfo entries in the index should be deleted first while in a
     *   critical section. The actual deletion should happen after leaving
     *   the critical section.
     * - This deletion should not destroy objects eagerly, because previous
     *   index probes might still be using the runs to be deleted. Thus, some
     *   type of reference-counting mechanism is required (e.g., shared_ptr).
     * - Deleting multiple runs cannot be done atomically, so it's an
     *   operation that must be logged and repeated during recovery in
     *   case of a crash.
     * - Not only is logging required, but we must make sure that recovery
     *   correctly covers all possible states -- any of the N runs being
     *   deleted might independently be in one of 3 states: removed from index
     *   but still in use (waiting for garbage collected), not in use and ready
     *   for (or currently undergoing) deletion, and fully deleted but run file
     *   still exists.
     * - Besides all that, it is atually easier to implement *moving* a run to
     *   a different (archival-kind-of) device than to delete it, which makes
     *   more sense in practice, so it might be worth looking into that before
     *   implementing deletion.
     *
     * Currently, runs are only deleted in ss_m::_truncate_los, which runs
     * after shutdown and thus is free of the issues above. That's also why
     * this method currently only removes files
     */
    spinlock_write_critical_section cs(&_mutex);

    if (replicationFactor == 0) { // delete all runs
        fs::directory_iterator it(archpath), eod;
        std::regex run_rx(run_regex);
        for (; it != eod; it++) {
            string fname = it->path().filename().string();
            if (std::regex_match(fname, run_rx)) {
                fs::remove(it->path());
            }
        }

        return;
    }

    for (unsigned level = maxLevel; level > 0; level--) {
        if (level <= replicationFactor) {
            // there's no run with the given replication factor -- just return
            return;
        }
        for (int h = lastFinished[level]; h >= 0; h--) {
            auto& high = runs[level][h];
            unsigned levelToClean = level - replicationFactor;
            while (levelToClean > 0) {
                // delete all runs within the LSN range of the higher-level run
                for (int l = lastFinished[levelToClean]; l >= 0; l--) {
                    auto& low = runs[levelToClean][l];
                    if (low.firstLSN >= high.firstLSN && low.lastLSN <= high.lastLSN)
                    {
                        auto path = make_run_path(low.firstLSN,
                                low.lastLSN, levelToClean);
                        fs::remove(path);
                    }
                }
                levelToClean--;
            }
        }
    }
}

size_t ArchiveIndex::getSkipLogrecSize() const
{
    return SKIP_LOGREC.length();
}

void ArchiveIndex::newBlock(const vector<pair<PageID, size_t> >&
        buckets, unsigned level)
{
    spinlock_write_critical_section cs(&_mutex);

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

rc_t ArchiveIndex::finishRun(lsn_t first, lsn_t last, PageID maxPID, int fd,
        off_t offset, unsigned level)
{
    int lf;
    {
        spinlock_write_critical_section cs(&_mutex);

        if (offset == 0) {
            // at least one entry is required for empty runs
            appendNewRun(level);
        }
        w_assert1(offset % blockSize == 0);

        lf = lastFinished[level] + 1;
        w_assert1(lf == 0 || first == runs[level][lf-1].lastLSN);
        w_assert1(lf < (int) runs[level].size());

        runs[level][lf].firstLSN = first;
        runs[level][lf].lastLSN = last;
        runs[level][lf].maxPID = maxPID;
    }

    if (offset > 0 && lf < (int) runs[level].size()) {
        W_DO(serializeRunInfo(runs[level][lf], fd, offset));
    }

    if (level > 1 && runRecycler) { runRecycler->wakeup(); }

    return RCOK;
}

rc_t ArchiveIndex::serializeRunInfo(RunInfo& run, int fd,
        off_t offset)
{
    spinlock_read_critical_section cs(&_mutex);

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

        // CS TODO: max pid is replicated in every index block because that
        // was quicker to implement. In the future, we should have "meta blocks"
        // in addition to data and index blocks to store such run information.
        memcpy(writeBuffer + bpos, &run.maxPID, sizeof(PageID));
        bpos += sizeof(PageID);

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


        auto ret = ::pwrite(fd, writeBuffer, blockSize, offset);
        CHECK_ERRNO(ret);
        offset += blockSize;
        i++;
    }

    delete[] writeBuffer;

    return RCOK;
}

void ArchiveIndex::appendNewRun(unsigned level)
{
    RunInfo newRun;
    if (level > maxLevel) {
        maxLevel = level;
        runs.resize(maxLevel+1);
        lastFinished.resize(maxLevel+1, -1);
    }
    runs[level].push_back(newRun);
}

void ArchiveIndex::startNewRun(unsigned level)
{
    spinlock_write_critical_section cs(&_mutex);
    appendNewRun(level);
}

lsn_t ArchiveIndex::getLastLSN()
{
    spinlock_read_critical_section cs(&_mutex);

    lsn_t last = lsn_t(1,0);

    for (unsigned l = 1; l <= maxLevel; l++) {
        if (lastFinished[l] >= 0) {
            auto& run = runs[l][lastFinished[l]];
            if (run.lastLSN > last) {
                last = run.lastLSN;
            }
        }
    }

    return last;
}

lsn_t ArchiveIndex::getLastLSN(unsigned level)
{
    spinlock_read_critical_section cs(&_mutex);

    if (level > maxLevel) {
        return lsn_t(1,0);
    }

    if (lastFinished[level] < 0) {
        // No runs exist in the given level. If a previous level exists, it
        // must be the first LSN in that level; otherwise, it's simply 1.0
        if (level == 0) { return lsn_t::null; }
        return getFirstLSN(level - 1);
    }

    return runs[level][lastFinished[level]].lastLSN;
}

lsn_t ArchiveIndex::getFirstLSN(unsigned level)
{
    if (level == 0) { return lsn_t::null; }
    // If no runs exist at this level, recurse down to previous level;
    if (lastFinished[level] < 0) { return getFirstLSN(level-1); }

    return runs[level][0].firstLSN;
}

void ArchiveIndex::loadRunInfo(RunFile* runFile, const RunId& fstats)
{
    RunInfo run;
    {
#ifndef USE_MMAP
        memalign_allocator<char, IO_ALIGN> alloc;
        char* readBuffer = alloc.allocate(blockSize);
#endif

        size_t indexBlockCount = 0;
        size_t dataBlockCount = 0;
        getBlockCounts(runFile, &indexBlockCount, &dataBlockCount);

        off_t offset = dataBlockCount * blockSize;
        w_assert1(dataBlockCount == 0 || offset > 0);
        size_t lastOffset = 0;

        while (indexBlockCount > 0) {
#ifndef USE_MMAP
            auto bytesRead = ::pread(runFile->fd, readBuffer, blockSize, offset);
            CHECK_ERRNO(bytesRead);
            if (bytesRead != (int) blockSize) { W_FATAL(stSHORTIO); }
#else
            char* readBuffer = runFile->getOffset(offset);
#endif

            BlockHeader* h = (BlockHeader*) readBuffer;

            unsigned j = 0;
            size_t bpos = sizeof(BlockHeader);

            run.maxPID = *((PageID*) (readBuffer + bpos));
            bpos += sizeof(PageID);

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
        }

#ifndef USE_MMAP
        alloc.deallocate(readBuffer);
#endif
    }

    run.firstLSN = fstats.beginLSN;
    run.lastLSN = fstats.endLSN;

    if (fstats.level > maxLevel) {
        maxLevel = fstats.level;
        // level 0 reserved, so add 1
        runs.resize(maxLevel+1);
        lastFinished.resize(maxLevel+1, -1);
    }
    runs[fstats.level].push_back(run);
    lastFinished[fstats.level] = runs[fstats.level].size() - 1;
}

void ArchiveIndex::getBlockCounts(RunFile* runFile, size_t* indexBlocks,
        size_t* dataBlocks)
{
    // skip emtpy runs
    if (runFile->length == 0) {
        if(indexBlocks) { *indexBlocks = 0; };
        if(dataBlocks) { *dataBlocks = 0; };
        return;
    }

#ifndef USE_MMAP
    // read header of last block in file -- its number is the block count
    // Using direct I/O -- must read whole align block
    char* buffer;
    int res = posix_memalign((void**) &buffer, IO_ALIGN, IO_ALIGN);
    w_assert0(res == 0);

    auto bytesRead = ::pread(runFile->fd, buffer, IO_ALIGN, runFile->length - blockSize);
    CHECK_ERRNO(bytesRead);
    if (bytesRead != IO_ALIGN) { W_FATAL(stSHORTIO); }

    BlockHeader* header = (BlockHeader*) buffer;
#else
    BlockHeader* header = (BlockHeader*) runFile->getOffset(runFile->length - blockSize);
#endif

    if (indexBlocks) {
        *indexBlocks = header->blockNumber + 1;
    }
    if (dataBlocks) {
        *dataBlocks = (runFile->length / blockSize) - (header->blockNumber + 1);
        w_assert1(*dataBlocks > 0);
    }
#ifndef USE_MMAP
    free(buffer);
#endif
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

    // No runs at this level
    if (lf < 0) { return 0; }

    if(lsn >= runs[level][lf].lastLSN) {
        return lf + 1;
    }

    int result = lf;
    while (result > 0 && runs[level][result].firstLSN > lsn) {
        result--;
    }

    // skip empty runs
    while (runs[level][result].entries.size() == 0 && result < lf) {
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

    /* The if below is for the old index organization that used pages (as in a
     * normal B-tree) rather than buckets. In that case, we have found the pid
     * as soon as the current entry is <= and the next >= than the given pid.
     * In the bucket organization, entries never repeat the same pid, so the
     * search criteria becomes "current entry <= pid && next entry > pid"
     */
    // if (run->entries[i].pid <= pid &&
    //         (i == run->entries.size() - 1 || run->entries[i+1].pid >= pid))
    // {
    //     // found it! must first check if previous does not contain same pid
    //     while (i > 0 && run->entries[i].pid == pid)
    //             //&& run->entries[i].pid == run->entries[i-1].pid)
    //     {
    //         i--;
    //     }
    //     return i;
    // }

    if (run->entries[i].pid <= pid &&
            (i == run->entries.size() - 1 || run->entries[i+1].pid > pid))
    {
        // found it! -- previous cannot contain the same pid in bucket organization
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

lsn_t ArchiveIndex::roundToEndLSN(lsn_t lsn, unsigned level)
{
    size_t index = findRun(lsn, level);

    // LSN not archived yet -> must be exactly the lastLSN of that level
    if ((int) index > lastFinished[level]) {
        w_assert1(runs[level][lastFinished[level]].lastLSN == lsn);
        return lsn;
    }

    // LSN at the exact beginning of a run, which means that the given LSN
    // was already at an endLSN border and, becuase it is an open interval,
    // findRun returns the following run
    auto begin = runs[level][index].firstLSN;
    if (lsn == begin) { return lsn; }

    return runs[level][index].lastLSN;
}

void ArchiveIndex::dumpIndex(ostream& out)
{
    for (size_t l = 0; l <= maxLevel; l++) {
        for (int i = 0; i <= lastFinished[l]; i++) {
            size_t offset = 0, prevOffset = 0;
            for (size_t j = 0; j < runs[l][i].entries.size(); j++) {
                offset = runs[l][i].entries[j].offset;
                out << "level " << l << " run " << i
                    << " entry " << j <<
                    " pid " << runs[l][i].entries[j].pid <<
                    " offset " << offset <<
                    " delta " << offset - prevOffset <<
                    endl;
                prevOffset = offset;
            }
        }
    }
}

void ArchiveIndex::dumpIndex(ostream& out, const RunId& runid)
{
    size_t offset = 0, prevOffset = 0;
    auto index = findRun(runid.beginLSN, runid.level);
    auto& run = runs[runid.level][index];
    for (size_t j = 0; j < run.entries.size(); j++) {
        offset = run.entries[j].offset;
        out << "level " << runid.level
            << " run " << index
            << " entry " << j <<
            " pid " << run.entries[j].pid <<
            " offset " << offset <<
            " delta " << offset - prevOffset <<
            endl;
        prevOffset = offset;
    }
}
