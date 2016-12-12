#include "logarchive_scanner.h"

#include "stopwatch.h"
#include "smthread.h"
#include "logarchive_index.h"
#include "log_consumer.h" // for LogScanner

// CS TODO: Aligning with the Linux standard FS block size
// We could try using 512 (typical hard drive sector) at some point,
// but none of this is actually standardized or portable
const size_t IO_ALIGN = 512;

ArchiveScanner::ArchiveScanner(ArchiveDirectory* directory)
    : directory(directory), archIndex(directory->getIndex())
{
    if (!archIndex) {
        W_FATAL_MSG(fcINTERNAL,
                << "ArchiveScanner requires a valid archive index!");
    }
}

ArchiveScanner::RunMerger*
ArchiveScanner::open(PageID startPID, PageID endPID,
        lsn_t startLSN, size_t readSize)
{
    RunMerger* merger = new RunMerger();
    vector<ArchiveIndex::ProbeResult> probes;

    // probe for runs
    archIndex->probe(probes, startPID, endPID, startLSN);

    // construct one run scanner for each probed run
    for (size_t i = 0; i < probes.size(); i++) {
        RunScanner* runScanner = new RunScanner(
                probes[i].runBegin,
                probes[i].runEnd,
                probes[i].level,
                probes[i].pidBegin,
                probes[i].pidEnd,
                probes[i].offset,
                directory,
                readSize
        );

        merger->addInput(runScanner);
    }

    if (merger->heapSize() == 0) {
        // all runs pruned from probe
        delete merger;
        return NULL;
    }

    INC_TSTAT(la_open_count);

    return merger;
}

ArchiveScanner::RunScanner::RunScanner(lsn_t b, lsn_t e, unsigned level,
        PageID f, PageID l, off_t o, ArchiveDirectory* directory, size_t rSize)
    : runBegin(b), runEnd(e), level(level), firstPID(f), lastPID(l), offset(o),
        fd(-1), blockCount(0), readSize(rSize), directory(directory)
{
    if (readSize == 0) {
        readSize = directory->getBlockSize();
    }

    // Using direct I/O
    int res = posix_memalign((void**) &buffer, IO_ALIGN, readSize + IO_ALIGN);
    w_assert0(res == 0);
    // buffer = new char[directory->getBlockSize()];

    w_assert0(directory->getIndex());
    bucketSize = directory->getIndex()->getBucketSize();

    // bpos at the end of block triggers reading of the first block
    // when calling next()
    bpos = readSize;
    w_assert1(bpos > 0);

    scanner = new LogScanner(readSize);
}

ArchiveScanner::RunScanner::~RunScanner()
{
    if (fd > 0) {
        W_COERCE(directory->closeScan(fd));
    }

    delete scanner;

    // Using direct I/O
    free(buffer);
    // delete[] buffer;
}

bool ArchiveScanner::RunScanner::nextBlock()
{
    size_t blockSize = directory->getBlockSize();

    if (fd < 0) {
        W_COERCE(directory->openForScan(fd, runBegin, runEnd, level));

        if (directory->getIndex()) {
            directory->getIndex()->getBlockCounts(fd, NULL, &blockCount);
        }
    }

    // do not read past data blocks into index blocks
    if (blockCount == 0 || offset >= blockCount * blockSize)
    {
        W_COERCE(directory->closeScan(fd));
        return false;
    }

    // offset is updated by readBlock
    W_COERCE(directory->readBlock(fd, buffer, offset, readSize));

    // offset set to zero indicates EOF
    if (offset == 0) {
        W_COERCE(directory->closeScan(fd));
        return false;
    }

    bpos = 0;

    return true;
}

bool ArchiveScanner::RunScanner::next(logrec_t*& lr)
{
    while (true) {
        if (scanner->nextLogrec(buffer, bpos, lr)) { break; }
        if (!nextBlock()) { return false; }
    }

    if (lr->type() == logrec_t::t_skip ||
                (lastPID != 0 && lr->pid() >= lastPID))
    {
        // end of scan
        return false;
    }

    return true;
}

std::ostream& operator<< (ostream& os,
        const ArchiveScanner::RunScanner& m)
{
    os << m.runBegin << "-" << m.runEnd << " endPID=" << m.lastPID;
    return os;
}

ArchiveScanner::MergeHeapEntry::MergeHeapEntry(RunScanner* runScan)
    : active(true), runScan(runScan)
{
    PageID startPID = runScan->firstPID;
    // bring scanner up to starting point
    logrec_t* next = NULL;
    if (runScan->next(next)) {
        lr = next;
        lsn = lr->lsn();
        pid = lr->pid();
        active = true;
        DBG(<< "Run scan opened on pid " << lr->pid() << " afer " << startPID);

        // advance index until startPID is reached
        if (pid < startPID) {
            bool hasNext = true;
            while (hasNext && lr->pid() < startPID) {
                hasNext = runScan->next(lr);
            }
            if (hasNext) {
                DBG(<< "Run scan advanced to pid " << lr->pid() << " afer " << startPID);
                pid = lr->pid();
                lsn = lr->lsn();
            }
            else { active = false; }
        }
    }
    else { active = false; }
}

void ArchiveScanner::MergeHeapEntry::moveToNext()
{
    if (runScan->next(lr)) {
        pid = lr->pid();
        lsn = lr->lsn_ck();
    }
    else {
        active = false;
    }
}

void ArchiveScanner::RunMerger::addInput(RunScanner* r)
{
    w_assert0(!started);
    MergeHeapEntry entry(r);
    heap.AddElementDontHeapify(entry);

    if (endPID == 0) {
        endPID = r->lastPID;
    }
    w_assert1(endPID == r->lastPID);
}

bool ArchiveScanner::RunMerger::next(logrec_t*& lr)
{
    stopwatch_t timer;

    if (heap.NumElements() == 0) {
        return false;
    }

    if (!started) {
        started = true;
        heap.Heapify();
    }
    else {
        /*
         * CS: Before returning the next log record, the scanner at the top of
         * the heap must be recomputed and the heap re-organized. This is
         * because the caller maintains a pointer into the scanner's buffer,
         * and calling next before the log record is consumed may cause the
         * pointer to be invalidated if a new block is read into the buffer.
         */
        heap.First().moveToNext();
        heap.ReplacedFirst();
    }

    if (!heap.First().active) {
        /*
         * CS: If top run is inactive, then all runs are and scan is done
         * Memory of each scanner must be released here instead of when
         * destructing heap, because the heap internally copies entries and
         * destructs these copies in operations like SiftDown(). Therefore the
         * underlying buffer may get wrongly deleted
         */
        close();
        return false;
    }

    ADD_TSTAT(la_merge_heap_time, timer.time_us());

    lr = heap.First().lr;
    return true;
}

void ArchiveScanner::RunMerger::close()
{
    while (heap.NumElements() > 0) {
        delete heap.RemoveFirst().runScan;
    }
}

void ArchiveScanner::RunMerger::dumpHeap(ostream& out)
{
    heap.Print(out);
}

std::ostream& operator<<(std::ostream& os, const ArchiveScanner::MergeHeapEntry& e)
{
    os << "[run " << *(e.runScan) << ", " << e.pid << ", " << e.lsn <<
        " active=" << e.active <<
        ", logrec=" << e.lr->lsn() << " " << e.lr->type_str()
        << ")]";
    return os;
}