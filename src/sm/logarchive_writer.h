#ifndef LOGARCHIVE_WRITER_H
#define LOGARCHIVE_WRITER_H

#include <vector>

#include "basics.h"
#include "lsn.h"
#include "thread_wrapper.h"
#include "btree_page.h"
#include "btree_page_h.h"

class AsyncRingBuffer;
class ArchiveIndex;
class logrec_t;

/** \brief Asynchronous writer thread to produce run files on disk
 *
 * Consumes blocks of data produced by the BlockAssembly component and
 * writes them to the corresponding run files on disk. Metadata on each
 * block is used to control to which run each block belongs and what LSN
 * ranges are contained in each run (see BlockAssembly).
 *
 * \author Caetano Sauer
 */
class WriterThread : public thread_wrapper_t {
private:

    AsyncRingBuffer* buf;
    ArchiveIndex* index;
    lsn_t maxLSNInRun;
    run_number_t currentRun;
    unsigned level;

public:
    virtual void run();

    ArchiveIndex* getIndex() { return index; }

    /*
     * Called by processFlushRequest to forcibly start a new run
     */
    void resetCurrentRun()
    {
        currentRun++;
        maxLSNInRun = lsn_t::null;
    }

    WriterThread(AsyncRingBuffer* writebuf, ArchiveIndex* index, unsigned level)
        :
            buf(writebuf), index(index),
            maxLSNInRun(lsn_t::null), currentRun(0), level(level)
    {
    }

    virtual ~WriterThread() {}
};

/** \brief Component that consumes a partially-sorted log record stream and
 * generates indexed runs from it.
 *
 * This class serves two purposes:
 * - It assembles individual log records into blocks which are written to
 *   persistent storage using an asynchronous writer thread (see
 *   WriterThread).
 * - For each block generated, it generates an entry on the archive index,
 *   allowing direct access to each block based on log record attributes
 *   (page id & lsn).
 *
 * The writer thread is controlled solely using an asynchronous ring
 * buffer. This works because the writer should keep writing as long as
 * there are blocks available -- unlike the reader thread, which must stop
 * once a certain LSN is reached.
 *
 * Each generated block contains a <b>header</b>, which specifies the run
 * number, the offset up to which valid log records are found within that
 * block, and the LSN of the last log record in the block. The run number
 * is used by the writer thread to write blocks to the correct run file --
 * once it changes from one block to another, it must close the currently
 * generated run file an open a new one. The LSN in the last block header
 * is then used to rename the file with the correct LSN range. (We used to
 * control these LSN boundaries with an additional queue structure, but it
 * required too many dependencies between modules that are otherwise
 * independent)
 *
 * \author Caetano Sauer
 */
class BlockAssembly {
public:
    BlockAssembly(ArchiveIndex* index, unsigned level = 1, bool compression = true);
    virtual ~BlockAssembly();

    bool start(run_number_t run);
    bool add(logrec_t* lr);
    void finish();
    void shutdown();
    bool hasPendingBlocks();

    void resetWriter()
    {
        writer->resetCurrentRun();
    }

    // methods that abstract block metadata
    static run_number_t getRunFromBlock(const char* b);
    static lsn_t getLSNFromBlock(const char* b);
    static size_t getEndOfBlock(const char* b);
private:
    char* dest;
    AsyncRingBuffer* writebuf;
    WriterThread* writer;
    ArchiveIndex* archIndex;
    size_t blockSize;
    size_t pos;
    size_t fpos;

    lsn_t maxLSNInBlock;
    int maxLSNLength;
    run_number_t lastRun;

    PageID currentPID;
    size_t currentPIDpos;
    size_t currentPIDfpos;
    lsn_t currentPIDLSN;
    lsn_t currentPIDprevLSN;
    lsn_t lastLSN;

    bool enableCompression;
    btree_page_h btree_p;
    btree_page compressedPage;
    logrec_t lrBuffer;
    bool isCompressing;

    // if using a variable-bucket index, this is the number of page IDs
    // that will be stored within a bucket (aka restore's segment)
    size_t bucketSize;
    // list of buckets beginning in the current block
    std::vector<pair<PageID, size_t> > buckets;
    // number of the nex bucket to be indexed
    size_t nextBucket;

    unsigned level;

    // Amount of space to reserve in each block (e.g., for skip log record)
    size_t spaceToReserve;

    bool hasSpace(logrec_t* lr);
    void copyLogrec(logrec_t* lr);
    void copyCompressedLogrec();

public:
    struct BlockHeader {
        lsn_t lsn;
        uint32_t end;
        run_number_t run;
    };

};

#endif
