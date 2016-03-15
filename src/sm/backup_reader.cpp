#include "backup_reader.h"

#include "vol.h"
#include "logarchiver.h"

#include <algorithm>

const std::string DummyBackupReader::IMPL_NAME = "dummy";
const std::string BackupOnDemandReader::IMPL_NAME = "ondemand";
const std::string BackupPrefetcher::IMPL_NAME = "prefetcher";

const size_t IO_ALIGN = LogArchiver::IO_ALIGN;

// Time (in usec) for which to wait until a segment is requested/prefetched
const unsigned WAIT_TIME = 500;

BackupReader::BackupReader(size_t bufferSize)
{
    // Using direct I/O
    w_assert1(bufferSize % IO_ALIGN == 0);
    posix_memalign((void**) &buffer, IO_ALIGN, bufferSize);
    // buffer = new char[bufferSize];
}

BackupReader::~BackupReader()
{
    // Using direct I/O
    free(buffer);
    // delete[] buffer;
}

BackupOnDemandReader::BackupOnDemandReader(vol_t* volume, size_t segmentSize)
    : BackupReader(segmentSize * sizeof(generic_page)),
      volume(volume), segmentSize(segmentSize)
{
    w_assert1(volume);
    firstDataPid = volume->first_data_pageid();
    W_IFDEBUG1(fixedSegment = -1);
}

char* BackupOnDemandReader::fix(unsigned segment)
{
    INC_TSTAT(restore_backup_reads);
    w_assert1(fixedSegment < 0);

    // CS: TODO call getPidForSegment
    // PageID offset = PageID(segment * segmentSize) + firstDataPid;
    PageID offset = PageID(segment * segmentSize);
    W_COERCE(volume->read_backup(offset, segmentSize, buffer));

    W_IFDEBUG1(fixedSegment = segment);

    return buffer;
}

void BackupOnDemandReader::unfix(unsigned segment)
{
    w_assert1(fixedSegment == (int) segment);
    W_IFDEBUG1(fixedSegment = -1);
}

BackupPrefetcher::BackupPrefetcher(vol_t* volume, size_t numSegments,
        size_t segmentSize)
    : BackupReader(segmentSize * sizeof(generic_page) * numSegments),
      volume(volume), numSegments(numSegments), segmentSize(segmentSize),
      segmentSizeBytes(segmentSize * sizeof(generic_page)),
      fixWaiting(false), shutdownFlag(false), lastEvicted(numSegments - 1)
{
    w_assert1(volume);
    firstDataPid = volume->first_data_pageid();

    // initialize all slots as free
    slots = new int[numSegments];
    status = new int[numSegments];
    for (size_t i = 0; i < numSegments; i++) {
        status[i] = SLOT_FREE;
    }

    DO_PTHREAD(pthread_mutex_init(&mutex, NULL));
}

BackupPrefetcher::~BackupPrefetcher()
{
    DO_PTHREAD(pthread_mutex_destroy(&mutex));

    delete[] slots;
}

void BackupPrefetcher::prefetch(unsigned segment, int priority)
{
    CRITICAL_SECTION(cs, &mutex);

    // if segment was already fetched, ignore
    for (size_t i = 0; i < numSegments; i++)
    {
        if (slots[i] == (int) segment && status[i] != SLOT_FREE) {
            return;
        }
    }

    // if segment was already requested, ignore
    for (std::deque<unsigned>::iterator iter = requests.begin();
            iter != requests.end(); iter++)
    {
        if (*iter == segment) { return; }
    }

    // add request to the queue
    if (priority > 0) {
        requests.push_front(segment);
    }
    else {
        requests.push_back(segment);
    }
    DBG(<< "Requested prefetch of " << segment);
}

char* BackupPrefetcher::fix(unsigned segment)
{
    bool wait = false;
    bool statIncremented = false;
    while (true) {

        if (wait) {
            usleep(WAIT_TIME);
            wait = false;
        }

        CRITICAL_SECTION(cs, &mutex);

        // look for segment in buffer -- majority of calls should end here
        // on the first try, otherwise prefetch was not effective
        for (size_t i = 0; i < numSegments; i++) {
            if (slots[i] == (int) segment && status[i] != SLOT_FREE) {
                if (status[i] == SLOT_READING) {
                    // Segment is curretly being read -- let's just wait
                    DBG(<< "Segment " << segment << " fix missed. "
                            << "Waiting for read");
                    wait = true;
                    break;
                }
                w_assert0(status[i] == SLOT_UNFIXED);
                status[i] = SLOT_FIXED;
                DBG(<< "Fixed segment " << segment << " into slot " << i);
                fixWaiting = false;
                return buffer + (i * segmentSizeBytes);
            }
        }

        if (!statIncremented) {
            INC_TSTAT(backup_not_prefetched);
            statIncremented = true;
        }

        if (wait) { continue; }

        // segment not in buffer -- move request to the front and wait
        // for prefetch to catch up
        std::deque<unsigned>::iterator iter
            = std::find(requests.begin(), requests.end(), segment);
        if (iter != requests.end()) {
            requests.erase(iter);
        }
        requests.push_front(segment);

        DBG(<< "Segment " << segment << " fix missed. Wake up prefetcher");
        fixWaiting = true;
        wait = true;
    }

    // should never reach this
    W_FATAL_MSG(fcINTERNAL,
            << "Invalid state in backup prefetcher");
}

void BackupPrefetcher::unfix(unsigned segment)
{
    CRITICAL_SECTION(cs, &mutex);

    for (size_t i = 0; i < numSegments; i++) {
        if (slots[i] == (int) segment && status[i] != SLOT_FREE) {
            w_assert1(status[i] == SLOT_FIXED);
            // since each segment is used only once, it goes directly to
            // free instead of unfixed
            DBG(<< "Unfixed segment " << segment <<  " from slot " << i);
            status[i] = SLOT_FREE;
            return;
        }
    }

    // Segment not found -- error!
    W_FATAL_MSG(fcINTERNAL,
            << "Attempt to unfix segment which was not fixed: " << segment);
}

void BackupPrefetcher::run()
{
    bool wait = false;
    while (true) {
        size_t slotIdx = 0;
        char* readSlot = NULL;
        unsigned next = numSegments; // invalid value
        PageID firstPage;

        if (wait) {
            usleep(WAIT_TIME);
            wait = false;
        }

        {
            CRITICAL_SECTION(cs, &mutex);

            if (shutdownFlag) { return; }

            if (requests.size() == 0) {
                wait = true;
                continue;
            }

            next = requests.front();

            firstPage = PageID(next * segmentSize);
            if (firstPage >= volume->num_used_pages()) {
                // prefetch request beyond end of volume -- ignore
                requests.pop_front();
                continue;
            }

            bool alreadyFetched = false;
            // look if segment is already in buffer
            for (size_t i = 0; i < numSegments; i++) {
                if (slots[i] == (int) next && status[i] != SLOT_FREE) {
                    alreadyFetched = true;
                    requests.pop_front();
                    break;
                }
            }
            if (alreadyFetched) { continue; }

            bool freeFound = false;
            // segment not in buffer, look for a free slot for reading into
            for (size_t i = 0; i < numSegments; i++) {
                if (status[i] == SLOT_FREE) {
                    slotIdx = i;
                    freeFound = true;
                    break;
                }
            }

            if (!freeFound) {
                if (!fixWaiting) {
                    // We're not in a hurry, so let's not evict prematurely
                    wait = true;
                    continue;
                }

                // A fix is waiting and the buffer is full -- must evict.
                // Do one round and start over if no segment can be evicted
                size_t start = lastEvicted;
                do {
                    if (lastEvicted == 0) { lastEvicted = numSegments - 1; }
                    else { lastEvicted--; }

                    if (lastEvicted == start) {
                        // no evictable segments found -- wait some more
                        wait = true;
                        INC_TSTAT(backup_eviction_stuck);
                        break;
                    }
                } while (status[lastEvicted] == SLOT_FIXED ||
                        status[lastEvicted] == SLOT_READING);

                if (wait) { continue; }

                DBG(<< "Evicted " << slots[lastEvicted]);
                requests.push_front(slots[lastEvicted]);
                slotIdx = lastEvicted;
                INC_TSTAT(backup_evict_segment);
            }

            // Found a slot to read into!
            slots[slotIdx] = next;
            status[slotIdx] = SLOT_READING;
            readSlot = buffer + (slotIdx * segmentSizeBytes);

            w_assert0(readSlot);
            requests.pop_front();

        } // end of critical section

        DBG(<< "Prefetching segment " << next);
        // perform the read into the slot found
        // PageID firstPage = PageID(next * segmentSize) + firstDataPid;
        firstPage = PageID(next * segmentSize);
        if (firstPage >= volume->num_used_pages()) {
            return;
        }

        INC_TSTAT(restore_backup_reads);
        W_COERCE(volume->read_backup(firstPage, segmentSize, readSlot));

        {
            // Re-acquire mutex to mark slot as read, i.e., unfixed
            CRITICAL_SECTION(cs, &mutex);
            status[slotIdx] = SLOT_UNFIXED;
            DBG(<< "Read segment " << next << " into  slot " << slotIdx);
        }

        DBG(<< "Segment " << next << " read finished");
    }
}

void BackupPrefetcher::finish()
{
    {
        CRITICAL_SECTION(cs, &mutex);
        shutdownFlag = true;
    }
    join();
}
