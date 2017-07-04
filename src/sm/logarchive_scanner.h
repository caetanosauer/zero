#ifndef LOGARCHIVE_SCANNER_H
#define LOGARCHIVE_SCANNER_H

#include <iostream>
#include <memory>
#include <vector>

#include "basics.h"
#include "lsn.h"
#include "w_heap.h"
#include "logarchive_index.h"

class ArchiveIndex;
class LogScanner;
class RunFile;
class logrec_t;

struct MergeInput
{
    RunFile* runFile;
    size_t pos;
    lsn_t keyLSN;
    PageID keyPID;
    PageID endPID;


    logrec_t* logrec();
    bool open(PageID startPID);
    bool finished();
    void next();

    friend bool mergeInputCmpGt(const MergeInput& a, const MergeInput& b);
};


// Merge input should be exactly 1/2 of a cacheline
static_assert(sizeof(MergeInput) == 32, "Misaligned MergeInput");

class ArchiveScan {
public:
    ArchiveScan(std::shared_ptr<ArchiveIndex>);
    ~ArchiveScan();

    void open(PageID startPID, PageID endPID, lsn_t startLSN,
            lsn_t endLSN = lsn_t::null);
    bool next(logrec_t*&);
    bool finished();

    template <class Iter>
    void openForMerge(Iter begin, Iter end);

    void dumpHeap();

private:
    // Thread-local storage for merge inputs
    static thread_local std::vector<MergeInput> _mergeInputVector;

    std::vector<MergeInput>::iterator heapBegin;
    std::vector<MergeInput>::iterator heapEnd;

    std::shared_ptr<ArchiveIndex> archIndex;
    lsn_t prevLSN;
    PageID prevPID;
    bool singlePage;

    void clear();
};

bool mergeInputCmpGt(const MergeInput& a, const MergeInput& b);

template <class Iter>
void ArchiveScan::openForMerge(Iter begin, Iter end)
{
    w_assert0(archIndex);
    clear();
    auto& inputs = _mergeInputVector;

    for (Iter it = begin; it != end; it++) {
        MergeInput input;
        auto runid = *it;
        input.pos = 0;
        input.runFile = archIndex->openForScan(*it);
        inputs.push_back(input);
    }

    heapBegin = inputs.begin();
    auto it = inputs.rbegin();
    while (it != inputs.rend())
    {
        constexpr PageID startPID = 0;
        if (it->open(startPID)) { it++; }
        else {
            std::advance(it, 1);
            inputs.erase(it.base());
        }
    }

    heapEnd = inputs.end();
    std::make_heap(heapBegin, heapEnd, mergeInputCmpGt);
}

/** \brief Provides scans over the log archive for restore operations.
 *
 * More documentation to follow, as class is still under test and
 * development (TODO).
 *
 * \author Caetano Sauer
 */
class ArchiveScanner {
public:
    ArchiveScanner(ArchiveIndex* = nullptr);
    virtual ~ArchiveScanner() {};

    struct RunMerger;

    std::shared_ptr<RunMerger> open(PageID startPID, PageID endPID, lsn_t startLSN);

    struct RunScanner {
        const lsn_t runBegin;
        const lsn_t runEnd;
        const unsigned level;
        const PageID firstPID;
        const PageID lastPID;

        size_t offset;
        char* buffer;
        size_t bpos;
        RunFile* runFile;
        size_t blockCount;
        size_t bucketSize;
        size_t readSize;

        ArchiveIndex* archIndex;
        LogScanner* scanner;

        RunScanner(lsn_t b, lsn_t e, unsigned level, PageID f, PageID l, off_t o,
                ArchiveIndex* index, size_t readSize = 0);
        ~RunScanner();

        logrec_t* open();
        bool next(logrec_t*& lr);

        friend std::ostream& operator<< (std::ostream& os, const RunScanner& m);

        private:
        bool nextBlock();
    };

    struct MmapRunScanner {
        const lsn_t runBegin;
        const lsn_t runEnd;
        const unsigned level;
        const PageID firstPID;
        const PageID lastPID;

        size_t blockCount;
        size_t pos;

        RunFile* runFile;
        ArchiveIndex* archIndex;

        MmapRunScanner(lsn_t b, lsn_t e, unsigned level, PageID f, PageID l, off_t o,
                ArchiveIndex* index);
        ~MmapRunScanner();

        logrec_t* open();
        bool next(logrec_t*& lr);

        friend std::ostream& operator<< (std::ostream& os, const RunScanner& m);
    };

private:
    ArchiveIndex* archIndex;

    struct MergeHeapEntry {
        // store pid and lsn here to speed up comparisons
        bool active;
        PageID pid;
        lsn_t lsn;
        logrec_t* lr;
        RunScanner* runScan;

        MergeHeapEntry(RunScanner* runScan);

        // required by w_heap
        MergeHeapEntry() : runScan(NULL) {}

        virtual ~MergeHeapEntry() {}

        void moveToNext();
        PageID lastPIDinBlock();
    };

    friend std::ostream& operator<<(std::ostream& os, const MergeHeapEntry& e);

    struct MergeHeapCmp {
        bool gt(const MergeHeapEntry& a, const MergeHeapEntry& b) const
        {
            if (!a.active) return false;
            if (!b.active) return true;
            if (a.pid != b.pid) {
                return a.pid< b.pid;
            }
            return a.lsn < b.lsn;
        }
    };

public:
    // Scan interface exposed to caller
    struct RunMerger {
        RunMerger()
            : heap(cmp), started(false), endPID(0)
        {}

        virtual ~RunMerger()
        {
            close();
        }

        void addInput(RunScanner* r);
        bool next(logrec_t*& lr);
        void dumpHeap(ostream& out);
        void close();

        size_t heapSize() { return heap.NumElements(); }
        PageID getEndPID() { return endPID; }

        private:
        MergeHeapCmp cmp;
        Heap<MergeHeapEntry, MergeHeapCmp> heap;
        bool started;
        PageID endPID;
    };
};

#endif
