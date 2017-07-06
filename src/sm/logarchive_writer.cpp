#include "logarchive_writer.h"

#include "ringbuffer.h"
#include "logarchive_index.h"

// CS TODO: use option
const static int IO_BLOCK_COUNT = 8; // total buffer = 8MB

BlockAssembly::BlockAssembly(ArchiveIndex* index, unsigned level, bool compression)
    : dest(NULL), maxLSNInBlock(lsn_t::null), maxLSNLength(0),
    lastRun(-1), currentPID(0), bucketSize(0), nextBucket(0), level(level),
    maxPID(std::numeric_limits<PageID>::min())
{
    archIndex = index;
    blockSize = archIndex->getBlockSize();
    bucketSize = archIndex->getBucketSize();
    writebuf = new AsyncRingBuffer(blockSize, IO_BLOCK_COUNT);
    writer = new WriterThread(writebuf, index, level);
    writer->fork();

    index->openNewRun(level);
    spaceToReserve = index->getSkipLogrecSize();

    enableCompression = compression;
}

BlockAssembly::~BlockAssembly()
{
    if (!writebuf->isFinished()) {
        shutdown();
    }
    delete writer;
    delete writebuf;
}

bool BlockAssembly::hasPendingBlocks()
{
    return !writebuf->isEmpty();
}

run_number_t BlockAssembly::getRunFromBlock(const char* b)
{
    BlockHeader* h = (BlockHeader*) b;
    return h->run;
}

PageID BlockAssembly::getMaxPIDFromBlock(const char* b)
{
    BlockHeader* h = (BlockHeader*) b;
    return h->maxPID;
}

lsn_t BlockAssembly::getLSNFromBlock(const char* b)
{
    BlockHeader* h = (BlockHeader*) b;
    return h->lsn;
}

size_t BlockAssembly::getEndOfBlock(const char* b)
{
    BlockHeader* h = (BlockHeader*) b;
    return h->end;
}

bool BlockAssembly::start(run_number_t run)
{
    DBGTHRD(<< "Requesting write block for selection");
    dest = writebuf->producerRequest();
    if (!dest) {
        DBGTHRD(<< "Block request failed!");
        if (!writebuf->isFinished()) {
            W_FATAL_MSG(fcINTERNAL,
                    << "ERROR: write ring buffer refused produce request");
        }
        return false;
    }
    DBGTHRD(<< "Picked block for selection " << (void*) dest);

    if (run != lastRun) {
        archIndex->startNewRun(level);
        nextBucket = 0;
        fpos = 0;
        lastRun = run;
    }

    pos = sizeof(BlockHeader);
    currentPID = 0;
    currentPIDpos = pos;
    currentPIDfpos = fpos;
    currentPIDprevLSN = lsn_t::null;
    maxPID = std::numeric_limits<PageID>::min();

    buckets.clear();

    return true;
}

bool BlockAssembly::add(logrec_t* lr)
{
    w_assert0(dest);
    w_assert1(lr->valid_header());

    // Verify if we still have space for this log record
    size_t available = blockSize - (pos + spaceToReserve);
    if (lr->length() > available) {
        // If this is a page_img logrec, we might still have space for it because
        // the preceding log records of the same PID will be dropped
        if (enableCompression && lr->type() == t_page_img_format) {
            size_t imgAvailable = blockSize - (currentPIDpos + spaceToReserve);
            bool hasSpaceForPageImg = lr->pid() == currentPID && lr->length() < imgAvailable;
            if (!hasSpaceForPageImg) { return false; }
        }
        else { return false; }
    }

    // New PID coming in: reset current PID stuff and check if it's time to add new bucket
    if (lr->pid() != currentPID || currentPIDfpos == 0) {
        currentPID = lr->pid();
        currentPIDpos = pos;
        currentPIDfpos = fpos;
        currentPIDprevLSN = lr->page_prev_lsn();

        if (lr->pid() / bucketSize >= nextBucket) {
            PageID shpid = (lr->pid() / bucketSize) * bucketSize;
            buckets.emplace_back(shpid, fpos);
            nextBucket = shpid / bucketSize + 1;
        }

        if (currentPID > maxPID) { maxPID = currentPID; }
    }

    if (maxLSNInBlock < lr->lsn_ck()) {
        maxLSNInBlock = lr->lsn_ck();
        maxLSNLength = lr->length();
    }

    if (enableCompression && lr->type() == t_page_img_format) {
        // Keep track of compression efficicency
        ADD_TSTAT(la_img_compressed_bytes, pos - currentPIDpos);
        //  Simply discard all log records produced for the current PID do far
        pos = currentPIDpos;
        fpos = currentPIDfpos;
    }
    w_assert1(pos > 0 || fpos % blockSize == 0);

    memcpy(dest + pos, lr, lr->length());

    if (enableCompression && lr->type() == t_page_img_format) {
        // Adjust per-page log chain to satisfy redo assertions
        reinterpret_cast<logrec_t*>(dest + pos)->set_page_prev_lsn(currentPIDprevLSN);
    }

    pos += lr->length();
    fpos += lr->length();
    return true;
}

void BlockAssembly::finish()
{
    DBGTHRD("Selection produced block for writing " << (void*) dest <<
            " in run " << (int) lastRun << " with end " << pos);
    w_assert0(dest);

    w_assert0(archIndex);
    archIndex->newBlock(buckets, level);

    // write block header info
    BlockHeader* h = (BlockHeader*) dest;
    h->run = lastRun;
    h->end = pos;
    h->maxPID = maxPID;
    /*
     * CS: end LSN of a block/run has to be an exclusive boundary, whereas
     * lastLSN is an inclusive one (i.e., the LSN of the last logrec in this
     * block). To fix that, we simply add the length of the last log record to
     * its LSN, which yields the LSN of the following record in the recovery
     * log. It doesn't matter if this following record does not get archived or
     * if it is a skip log record, since the property that must be respected is
     * simply that run boundaries must match (i.e., endLSN(n) == beginLSN(n+1)
     */
    h->lsn = maxLSNInBlock.advance(maxLSNLength);

#if W_DEBUG_LEVEL>=3
    // verify that all log records are within end boundary
    size_t vpos = sizeof(BlockHeader);
    while (vpos < pos) {
        logrec_t* lr = (logrec_t*) (dest + vpos);
        w_assert3(lr->lsn_ck() < h->lsn);
        vpos += lr->length();
    }
#endif

    maxLSNInBlock = lsn_t::null;
    writebuf->producerRelease();
    dest = NULL;
}

void BlockAssembly::shutdown()
{
    w_assert0(!dest);
    writebuf->set_finished();
    writer->join();
}

void WriterThread::run()
{
    DBGTHRD(<< "Writer thread activated");

    while(true) {
        char* src = buf->consumerRequest();
        if (!src) {
            /* Is the finished flag necessary? Yes.
             * The reader thread stops once it reaches endLSN, and then it
             * sleeps and waits for the next activate signal. The writer
             * thread, on the other hand, does not need an activation signal,
             * because it runs indefinitely, just waiting for blocks to be
             * written. The only stop condition is when the write buffer itself
             * is marked finished, which is done in shutdown().
             * Nevertheless, a null block is only returned once the finished
             * flag is set AND there are no more blocks. Thus, we gaurantee
             * that all pending blocks are written out before shutdown.
             */
            DBGTHRD(<< "Finished flag set on writer thread");
            W_COERCE(index->closeCurrentRun(maxLSNInRun, level, maxPIDInRun));
            return; // finished is set on buf
        }

        DBGTHRD(<< "Picked block for write " << (void*) src);

        run_number_t run = BlockAssembly::getRunFromBlock(src);
        if (currentRun != run) {
            // when writer is restarted, currentRun resets to zero
            w_assert1(currentRun == 0 || run == currentRun + 1);
            /*
             * Selection (producer) guarantees that logrec fits in block.
             * lastLSN is the LSN of the first log record in the new block
             * -- it will be used as the upper bound when renaming the file
             *  of the current run. This same LSN will be used as lower
             *  bound on the next run, which allows us to verify whether
             *  holes exist in the archive.
             */
            W_COERCE(index->closeCurrentRun(maxLSNInRun, level, maxPIDInRun));
            w_assert1(index->getLastLSN(level) == maxLSNInRun);
            currentRun = run;
            DBGTHRD(<< "Opening file for new run " << run
                    << " starting on LSN " << maxLSNInRun);
            maxLSNInRun = lsn_t::null;
        }

        lsn_t blockLSN = BlockAssembly::getLSNFromBlock(src);
        if (blockLSN > maxLSNInRun) { maxLSNInRun = blockLSN; }

        PageID maxPID = BlockAssembly::getMaxPIDFromBlock(src);
        if (maxPID > maxPIDInRun) { maxPIDInRun = maxPID; }

        size_t blockEnd = BlockAssembly::getEndOfBlock(src);
        size_t actualBlockSize= blockEnd - sizeof(BlockAssembly::BlockHeader);
        memmove(src, src + sizeof(BlockAssembly::BlockHeader), actualBlockSize);

        W_COERCE(index->append(src, actualBlockSize, level));

        DBGTHRD(<< "Wrote out block " << (void*) src
                << " with max LSN " << blockLSN);

        buf->consumerRelease();
    }
}
