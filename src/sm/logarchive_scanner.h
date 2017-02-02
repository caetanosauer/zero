#ifndef LOGARCHIVE_SCANNER_H
#define LOGARCHIVE_SCANNER_H

#include <iostream>
#include <memory>

#include "basics.h"
#include "lsn.h"
#include "w_heap.h"

class ArchiveIndex;
class LogScanner;
class logrec_t;

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

    std::shared_ptr<RunMerger> open(PageID startPID, PageID endPID, lsn_t startLSN,
            size_t readSize);

    struct RunScanner {
        const lsn_t runBegin;
        const lsn_t runEnd;
        const unsigned level;
        const PageID firstPID;
        const PageID lastPID;

        size_t offset;
        char* buffer;
        size_t bpos;
        int fd;
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
