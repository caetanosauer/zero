#include "logarchive_scanner.h"

#include "stopwatch.h"
#include "smthread.h"
#include "logarchive_index.h"
#include "log_consumer.h" // for LogScanner

// CS TODO: Aligning with the Linux standard FS block size
// We could try using 512 (typical hard drive sector) at some point,
// but none of this is actually standardized or portable
const size_t IO_ALIGN = 512;

ArchiveScanner::ArchiveScanner(ArchiveIndex* index)
    : archIndex(index)
{
    if (!archIndex) {
        W_FATAL_MSG(fcINTERNAL,
                << "ArchiveScanner requires a valid archive index!");
    }
}

std::shared_ptr<ArchiveScanner::RunMerger>
ArchiveScanner::open(PageID startPID, PageID endPID, lsn_t startLSN)
{
    auto merger = std::make_shared<RunMerger>();
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
                archIndex
        );

        merger->addInput(runScanner);
    }

    if (merger->heapSize() == 0) {
        // all runs pruned from probe
        merger = nullptr;
        return merger;
    }

    return merger;
}

ArchiveScanner::RunScanner::RunScanner(lsn_t b, lsn_t e, unsigned level,
        PageID f, PageID l, off_t o, ArchiveIndex* index, size_t rSize)
    : runBegin(b), runEnd(e), level(level), firstPID(f), lastPID(l), offset(o),
        runFile(nullptr), blockCount(0), readSize(rSize), archIndex(index)
{
    w_assert0(archIndex);
    // TODO get from sm options
    constexpr size_t dftReadSize = 32768;
    if (readSize == 0) { readSize = dftReadSize; }

    // Using direct I/O
    int res = posix_memalign((void**) &buffer, IO_ALIGN, readSize + IO_ALIGN);
    w_assert0(res == 0);
    // buffer = new char[directory->getBlockSize()];

    bucketSize = archIndex->getBucketSize();

    // bpos at the end of block triggers reading of the first block
    // when calling next()
    bpos = readSize;
    w_assert1(bpos > 0);

    scanner = new LogScanner(readSize);
}

ArchiveScanner::RunScanner::~RunScanner()
{
    if (runFile) {
        archIndex->closeScan(RunId{runBegin, runEnd, level});
    }

    delete scanner;

    // Using direct I/O
    free(buffer);
    // delete[] buffer;
}

logrec_t* ArchiveScanner::RunScanner::open()
{
    logrec_t* lr = nullptr;
    if (next(lr)) {
        lsn_t lsn = lr->lsn();
        PageID pid = lr->pid();
        DBG(<< "Run scan opened on pid " << lr->pid() << " afer " << firstPID);

        // advance index until firstPID is reached
        if (pid < firstPID) {
            bool hasNext = true;
            while (hasNext && lr->pid() < firstPID) {
		ADD_TSTAT(la_skipped_bytes, lr->length());
                hasNext = next(lr);
            }
            if (hasNext) {
                DBG(<< "Run scan advanced to pid " << lr->pid() << " afer " << firstPID);
                pid = lr->pid();
                lsn = lr->lsn();
            }
            else {
                INC_TSTAT(la_wasted_read);
                return nullptr;
            }
        }
    }
    else {
        INC_TSTAT(la_wasted_read);
        return nullptr;
    }

    return lr;
}

bool ArchiveScanner::RunScanner::nextBlock()
{
    size_t blockSize = archIndex->getBlockSize();

    if (!runFile) {
        runFile = archIndex->openForScan(RunId{runBegin, runEnd, level});
        archIndex->getBlockCounts(runFile, NULL, &blockCount);
    }

    // do not read past data blocks into index blocks
    if (blockCount == 0 || offset >= blockCount * blockSize)
    {
        archIndex->closeScan(RunId{runBegin, runEnd, level});
        return false;
    }

    // offset is updated by readBlock
    W_COERCE(archIndex->readBlock(runFile->fd, buffer, offset, readSize));

    // offset set to zero indicates EOF
    if (offset == 0) {
        archIndex->closeScan(RunId{runBegin, runEnd, level});
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

ArchiveScanner::MmapRunScanner::MmapRunScanner(lsn_t b, lsn_t e, unsigned level,
        PageID f, PageID l, off_t offset, ArchiveIndex* index)
    : runBegin(b), runEnd(e), level(level), firstPID(f), lastPID(l),
    blockCount(0), pos(offset), runFile(nullptr), archIndex(index)
{
    w_assert0(archIndex);
}

ArchiveScanner::MmapRunScanner::~MmapRunScanner()
{
    if (runFile) {
        archIndex->closeScan(RunId{runBegin, runEnd, level});
    }
}

// CS TODO: this is exactly the same as RunScanner::open
logrec_t* ArchiveScanner::MmapRunScanner::open()
{
    logrec_t* lr = nullptr;
    if (next(lr)) {
        lsn_t lsn = lr->lsn();
        PageID pid = lr->pid();
        DBG(<< "Run scan opened on pid " << lr->pid() << " afer " << firstPID);

        // advance index until firstPID is reached
        if (pid < firstPID) {
            bool hasNext = true;
            while (hasNext && lr->pid() < firstPID) {
		ADD_TSTAT(la_skipped_bytes, lr->length());
                hasNext = next(lr);
            }
            if (hasNext) {
                DBG(<< "Run scan advanced to pid " << lr->pid() << " afer " << firstPID);
                pid = lr->pid();
                lsn = lr->lsn();
            }
            else {
                INC_TSTAT(la_wasted_read);
                return nullptr;
            }
        }
    }
    else {
        INC_TSTAT(la_wasted_read);
        return nullptr;
    }

    return lr;
}

bool ArchiveScanner::MmapRunScanner::next(logrec_t*& lr)
{
    if (!runFile) {
        runFile = archIndex->openForScan(RunId{runBegin, runEnd, level});
        w_assert0(runFile);
        archIndex->getBlockCounts(runFile, nullptr, &blockCount);
    }

    if (runFile->length == 0) { return false; }

    lr = reinterpret_cast<logrec_t*>(runFile->getOffset(pos));
    w_assert1(lr->valid_header());

    pos += lr->length();
    return true;
}

std::ostream& operator<< (ostream& os,
        const ArchiveScanner::RunScanner& m)
{
    os << m.runBegin << "-" << m.runEnd << " endPID=" << m.lastPID;
    return os;
}

ArchiveScanner::MergeHeapEntry::MergeHeapEntry(RunScanner* runScan)
    :  lr(nullptr), runScan(runScan)
{
    // bring scanner up to starting point
    logrec_t* first_lr = runScan->open();
    if (!first_lr) {
        active = false;
    }
    else {
        active = true;
        lr = first_lr;
        pid = lr->pid();
        lsn = lr->lsn();
    }
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
