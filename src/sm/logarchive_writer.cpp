#include "logarchive_writer.h"

#include "ringbuffer.h"
#include "logarchive_index.h"

// CS TODO: use option
const static int IO_BLOCK_COUNT = 8; // total buffer = 8MB

BlockAssembly::BlockAssembly(ArchiveDirectory* directory, unsigned level)
    : dest(NULL), maxLSNInBlock(lsn_t::null), maxLSNLength(0),
    lastRun(-1), bucketSize(0), nextBucket(0), level(level)
{
    archIndex = directory->getIndex();
    w_assert0(archIndex);
    blockSize = directory->getBlockSize();
    bucketSize = archIndex->getBucketSize();
    writebuf = new AsyncRingBuffer(blockSize, IO_BLOCK_COUNT);
    writer = new WriterThread(writebuf, directory, level);
    writer->fork();

    spaceToReserve = directory->getSkipLogrecSize();
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

    pos = sizeof(BlockHeader);

    if (run != lastRun) {
        if (archIndex) {
            archIndex->appendNewEntry(level);
        }
        nextBucket = 0;
        fpos = 0;
        lastRun = run;
    }

    buckets.clear();

    return true;
}

bool BlockAssembly::add(logrec_t* lr)
{
    w_assert0(dest);

    size_t available = blockSize - (pos + spaceToReserve);
    if (lr->length() > available) {
        return false;
    }

    if (firstPID == 0) {
        firstPID = lr->pid();
    }

    if (maxLSNInBlock < lr->lsn_ck()) {
        maxLSNInBlock = lr->lsn_ck();
        maxLSNLength = lr->length();
    }

    if (lr->pid() / bucketSize >= nextBucket) {
        PageID shpid = (lr->pid() / bucketSize) * bucketSize;
        buckets.push_back(
                pair<PageID, size_t>(shpid, fpos));
        nextBucket = shpid / bucketSize + 1;
    }

    w_assert1(pos > 0 || fpos % blockSize == 0);

    memcpy(dest + pos, lr, lr->length());
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
    firstPID = 0;

    // write block header info
    BlockHeader* h = (BlockHeader*) dest;
    h->run = lastRun;
    h->end = pos;
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
            W_COERCE(directory->closeCurrentRun(maxLSNInRun, level));
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
            W_COERCE(directory->closeCurrentRun(maxLSNInRun, level));
            w_assert1(directory->getLastLSN() == maxLSNInRun);
            currentRun = run;
            maxLSNInRun = lsn_t::null;
            DBGTHRD(<< "Opening file for new run " << run
                    << " starting on LSN " << directory->getLastLSN());
        }

        lsn_t blockLSN = BlockAssembly::getLSNFromBlock(src);
        if (blockLSN > maxLSNInRun) {
            maxLSNInRun = blockLSN;
        }

        size_t blockEnd = BlockAssembly::getEndOfBlock(src);
        size_t actualBlockSize= blockEnd - sizeof(BlockAssembly::BlockHeader);
        memmove(src, src + sizeof(BlockAssembly::BlockHeader), actualBlockSize);

        W_COERCE(directory->append(src, actualBlockSize, level));

        DBGTHRD(<< "Wrote out block " << (void*) src
                << " with max LSN " << blockLSN);

        buf->consumerRelease();
    }
}
