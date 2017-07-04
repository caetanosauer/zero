#ifndef LOGARCHIVE_INDEX_H
#define LOGARCHIVE_INDEX_H

#include <vector>
#include <list>
#include <unordered_map>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include "basics.h"
#include "latches.h"
#include "lsn.h"
#include "sm_options.h"

class RunRecycler;

struct RunId {
    lsn_t beginLSN;
    lsn_t endLSN;
    unsigned level;

    bool operator==(const RunId& other) const
    {
        return beginLSN == other.beginLSN && endLSN == other.endLSN
            && level == other.level;
    }
};

// Controls access to a single run file through mmap
struct RunFile
{
    RunId runid;
    int fd;
    int refcount;
    char* data;
    size_t length;

    RunFile() : fd(-1), refcount(0), data(nullptr), length(0)
    {
    }

    char* getOffset(off_t offset) const { return data + offset; }
};

namespace std
{
    /// Hash function for RunId objects
    /// http://stackoverflow.com/q/17016175/1268568
    template<> struct hash<RunId>
    {
        using argument_type = RunId;
        using result_type = std::size_t;
        result_type operator()(argument_type const& a) const
        {
            result_type const h1 ( std::hash<lsn_t>()(a.beginLSN) );
            result_type const h2 ( std::hash<lsn_t>()(a.endLSN) );
            result_type const h3 ( std::hash<unsigned>()(a.level) );
            return ((h1 ^ (h2 << 1)) >> 1) ^ (h3 << 1);
        }
    };
}

/** \brief Encapsulates all file and I/O operations on the log archive
 *
 * The directory object serves the following purposes:
 * - Inspecting the existing archive files at startup in order to determine
 *   the last LSN persisted (i.e., from where to resume archiving) and to
 *   delete incomplete or already merged (TODO) files that can result from
 *   a system crash.
 * - Support run generation by providing operations to open a new run,
 *   append blocks of data to the current run, and closing the current run
 *   by renaming its file with the given LSN boundaries.
 * - Support scans by opening files given their LSN boundaries (which are
 *   determined by the archive index), reading arbitrary blocks of data
 *   from them, and closing them.
 * - In the near future, it should also support the new (i.e.,
 *   instant-restore-enabled) asynchronous merge daemon (TODO).
 * - Support auxiliary file-related operations that are used, e.g., in
 *   tests and experiments.  Currently, the only such operation is
 *   parseLSN.
 *
 * \author Caetano Sauer
 */
class ArchiveIndex {
public:
    ArchiveIndex(const sm_options& options);
    virtual ~ArchiveIndex();

    struct BlockEntry {
        size_t offset;
        PageID pid;
    };

    struct BlockHeader {
        uint32_t entries;
        uint32_t blockNumber;
    };

    struct RunInfo {
        lsn_t firstLSN;
        // lastLSN must be equal to firstLSN of the following run.  We keep
        // it redundantly so that index probes don't have to look beyond
        // the last finished run. We used to keep a global lastLSN field in
        // the index, but there can be a race between the writer thread
        // inserting new runs and probes on the last finished, so it was
        // removed.
        lsn_t lastLSN;

        // Used as a filter to avoid unneccessary probes on older runs
        PageID maxPID;

        std::vector<BlockEntry> entries;

        bool operator<(const RunInfo& other) const
        {
            return firstLSN < other.firstLSN;
        }
    };

    size_t getBlockSize() const { return blockSize; }
    std::string getArchDir() const { return archdir; }

    lsn_t getLastLSN();
    lsn_t getLastLSN(unsigned level);
    lsn_t getFirstLSN(unsigned level);

    // run generation methods
    rc_t openNewRun(unsigned level);
    rc_t append(char* data, size_t length, unsigned level);
    rc_t closeCurrentRun(lsn_t runEndLSN, unsigned level, PageID maxPID = 0);

    // run scanning methods
    RunFile* openForScan(const RunId& runid);
    void closeScan(const RunId& runid);
    rc_t readBlock(int fd, char* buf, size_t& offset, size_t readSize = 0);

    void listFiles(std::vector<std::string>& list, int level = -1);
    void listFileStats(std::list<RunId>& list, int level = -1);
    void deleteRuns(unsigned replicationFactor = 0);

    size_t getSkipLogrecSize() const;

    static bool parseRunFileName(string fname, RunId& fstats);
    static size_t getFileSize(int fd);

    void newBlock(const vector<pair<PageID, size_t> >& buckets, unsigned level);

    rc_t finishRun(lsn_t first, lsn_t last, PageID maxPID,
            int fd, off_t offset, unsigned level);

    template <class Input>
    void probe(std::vector<Input>&, PageID, PageID, lsn_t startLSN,
            lsn_t endLSN = lsn_t::null);

    void getBlockCounts(RunFile*, size_t* indexBlocks, size_t* dataBlocks);
    void loadRunInfo(RunFile*, const RunId&);
    void startNewRun(unsigned level);

    unsigned getMaxLevel() const { return maxLevel; }
    size_t getBucketSize() { return bucketSize; }
    size_t getRunCount(unsigned level) {
        if (level > maxLevel) { return 0; }
        return runs[level].size();
    }

    void dumpIndex(ostream& out);
    void dumpIndex(ostream& out, const RunId& runid);

    template <class OutputIter>
    void listRunsNonOverlapping(OutputIter out)
    {
        auto level = maxLevel;
        auto startLSN = lsn_t::null;

        // Start collecting runs on the max level, which has the largest runs
        // and therefore requires the least random reads
        while (level > 0) {
            auto index = findRun(startLSN, level);

            while ((int) index <= lastFinished[level]) {
                auto& run = runs[level][index];
                out = RunId{run.firstLSN, run.lastLSN, level};
                startLSN = run.lastLSN;
                index++;
            }

            level--;
        }
    }

private:

    void appendNewRun(unsigned level);
    size_t findRun(lsn_t lsn, unsigned level);
    // binary search
    size_t findEntry(RunInfo* run, PageID pid,
            int from = -1, int to = -1);
    rc_t serializeRunInfo(RunInfo&, int fd, off_t);

    lsn_t roundToEndLSN(lsn_t lsn, unsigned level);

private:
    std::string archdir;
    std::vector<int> appendFd;
    std::vector<off_t> appendPos;
    size_t blockSize;

    fs::path archpath;

    // Run information for each level of the index
    std::vector<std::vector<RunInfo>> runs;

    // Last finished run on each level -- this is required because runs are
    // generated asynchronously, so that a new one may be appended to the
    // index before the last one is finished. Thus, when calling finishRun(),
    // we cannot simply take the last run in the vector.
    std::vector<int> lastFinished;

    /** Whether this index uses variable-sized buckets, i.e., entries in
     * the index refer to fixed ranges of page ID for which the amount of
     * log records is variable. The number gives the size of a bucket in
     * terms of number of page ID's (or segment size in the restore case).
     * If this is zero, then the index behaves like a B-tree, in which a
     * bucket corresponds to a block, therefore having fixed sizes (but
     * variable number of log records, obviously).
     */
    size_t bucketSize;

    unsigned maxLevel;

    std::unique_ptr<RunRecycler> runRecycler;

    mutable srwlock_t _mutex;

    /// Cache for open files (for scans only)
    std::unordered_map<RunId, RunFile> _open_files;
    mutable srwlock_t _open_file_mutex;

    bool directIO;

    fs::path make_run_path(lsn_t begin, lsn_t end, unsigned level = 1) const;
    fs::path make_current_run_path(unsigned level) const;

public:
    const static string RUN_PREFIX;
    const static string CURR_RUN_PREFIX;
    const static string run_regex;
    const static string current_regex;
};

template <class Input>
void ArchiveIndex::probe(std::vector<Input>& inputs,
        PageID startPID, PageID endPID, lsn_t startLSN, lsn_t endLSN)
{
    spinlock_read_critical_section cs(&_mutex);

    Input input;
    input.endPID = endPID;
    unsigned level = maxLevel;
    inputs.clear();

    while (level > 0) {
        size_t index = findRun(startLSN, level);

        while ((int) index <= lastFinished[level]) {
            auto& run = runs[level][index];
            index++;
            startLSN = run.lastLSN;

            if (!endLSN.is_null() && startLSN >= endLSN) { return; }

            if (startPID > run.maxPID) {
                // INC_TSTAT(la_avoided_probes);
                continue;
            }

            if (run.entries.size() > 0) {
                size_t entryBegin = findEntry(&run, startPID);

                // CS TODO this if could just be run.entris[entryBEgin].pid >= endPID
                if (bucketSize == 1 && startPID == endPID-1 &&
                        run.entries[entryBegin].pid != startPID)
                {
                    // With bucket size one, we know precisely which PIDs are contained
                    // in this run, so what we have is a filter with 100% precision
                    // INC_TSTAT(la_avoided_probes);
                    continue;
                }

                input.pos = run.entries[entryBegin].offset;
                input.runFile =
                    openForScan(RunId{run.firstLSN, run.lastLSN, level});
                w_assert1(input.pos < input.runFile->length);
                inputs.push_back(input);
            }
        }

        level--;
    }
}

#endif
