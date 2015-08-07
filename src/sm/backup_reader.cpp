#include "backup_reader.h"

#include "vol.h"
#include "logarchiver.h"

const std::string DummyBackupReader::IMPL_NAME = "dummy";
const std::string BackupOnDemandReader::IMPL_NAME = "ondemand";
const std::string BackupPrefetcher::IMPL_NAME = "prefetcher";

const size_t IO_ALIGN = LogArchiver::IO_ALIGN;

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

    shpid_t offset = shpid_t(segment * segmentSize) + firstDataPid;
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
    : BackupReader(segmentSize * sizeof(generic_page)),
      volume(volume), numSegments(numSegments), segmentSize(segmentSize),
      segmentSizeBytes(segmentSize * sizeof(generic_page)),
      fixWaiting(false), lastEvicted(0)
{
    w_assert1(volume);
    firstDataPid = volume->first_data_pageid();

    // initialize all slots with -1, i.e., free
    slots = new int[numSegments];
    status = new int[numSegments];
    for (size_t i = 0; i < numSegments; i++) {
        status[i] = SLOT_FREE;
    }

    DO_PTHREAD(pthread_cond_init(&readCond, NULL));
    DO_PTHREAD(pthread_mutex_init(&readCondMutex, NULL));
    DO_PTHREAD(pthread_cond_init(&prefetchCond, NULL));
    DO_PTHREAD(pthread_mutex_init(&prefetchCondMutex, NULL));
}

BackupPrefetcher::~BackupPrefetcher()
{
    DO_PTHREAD(pthread_mutex_destroy(&readCondMutex));
    DO_PTHREAD(pthread_cond_destroy(&readCond));
    DO_PTHREAD(pthread_mutex_destroy(&prefetchCondMutex));
    DO_PTHREAD(pthread_cond_destroy(&prefetchCond));

    delete[] slots;
}

void BackupPrefetcher::prefetch(unsigned segment, int priority)
{
    {
        spinlock_write_critical_section cs(&requestMutex);

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
            if (*iter == segment) {
                return;
            }
        }

        // add request to the queue
        if (priority > 0) {
            requests.push_front(segment);
        }
        else {
            requests.push_back(segment);
        }
    }

    // wake up prefetcher
    CRITICAL_SECTION(cs, &prefetchCondMutex);
    DO_PTHREAD(pthread_cond_broadcast(&prefetchCond));
}

char* BackupPrefetcher::fix(unsigned segment)
{
    while (true) {
        {
            spinlock_write_critical_section cs(&requestMutex);

            // look for segment in buffer -- majority of calls should end here
            // on the first try, otherwise prefetch was not effective
            for (size_t i = 0; i < numSegments; i++) {
                if (slots[i] == (int) segment && status[i] != SLOT_FREE) {
                    w_assert0(status[i] == SLOT_UNFIXED);
                    fixWaiting = false;
                    lintel::atomic_thread_fence(lintel::memory_order_release);
                    return buffer + (i * segmentSizeBytes);
                }
            }

            INC_TSTAT(backup_not_prefetched);

            // segment not in buffer -- move request to the front and wait
            // for prefetch to catch up
            if (requests.front() != segment) {
                for (std::deque<unsigned>::iterator iter = requests.begin();
                        iter != requests.end(); iter++)
                {
                    if (*iter == segment) {
                        requests.erase(iter);
                    }
                }
                requests.push_front(segment);
            }

            // signalize to prefetcher that we are waiting
            // (i.e., it may evict segments if necessary)
            fixWaiting = true;
            lintel::atomic_thread_fence(lintel::memory_order_release);
        }

        // wait for prefetcher thread to read a segment
        DO_PTHREAD(pthread_mutex_lock(&readCondMutex));
        DO_PTHREAD(pthread_cond_wait(&readCond, &readCondMutex));
        DO_PTHREAD(pthread_mutex_unlock(&readCondMutex));
    }

    // should never reach this
    return NULL;
}

void BackupPrefetcher::unfix(unsigned segment)
{
    spinlock_write_critical_section cs(&requestMutex);
    for (size_t i = 0; i < numSegments; i++) {
        if (slots[i] == (int) segment) {
            w_assert1(status[i] != SLOT_FREE);
            // since each segment is used only once, it goes directly to
            // free instead of unfixed
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
    bool sleep = false;
    while (volume->is_failed()) {
        {
            // read spinlock to check if queue is empty
            spinlock_write_critical_section cs(&requestMutex);

            if (requests.size() == 0) {
                // Wait for activation signal or timeout
                CRITICAL_SECTION(cs, &prefetchCondMutex);
                struct timespec timeout;
                sthread_t::timeout_to_timespec(100, timeout); // 100ms
                int code = pthread_cond_timedwait(&prefetchCond, &prefetchCondMutex,
                        &timeout);
                DO_PTHREAD_TIMED(code);
            }
        }

        char* readSlot = NULL;
        unsigned next = numSegments; // invalid value

        if (sleep) {
            usleep(1000); // 1ms
            sleep = false;
        }

        {
            spinlock_write_critical_section cs(&requestMutex);

            if (requests.size() == 0) { continue; }

            next = requests.front();

            // look if segment is already in buffer
            for (size_t i = 0; i < numSegments; i++) {
                if (slots[i] == (int) next && status[i] != SLOT_FREE) {
                    requests.pop_front();
                    continue;
                }
            }

            // segment not in buffer, look for a free slot for reading into
            for (size_t i = 0; i < numSegments; i++) {
                if (status[i] == SLOT_FREE) {
                    readSlot = buffer + (i * segmentSizeBytes);
                    slots[i] = next;
                    status[i] = SLOT_UNFIXED;
                }
            }

            if (!readSlot) {
                lintel::atomic_thread_fence(lintel::memory_order_consume);
                if (!fixWaiting) {
                    sleep = true;
                    continue;
                }

                // A fix is waiting and the buffer is full -- must evict.
                // Do one round and start over if no segment can be evicted
                size_t start = lastEvicted;
                do {
                    if (lastEvicted == 0) { lastEvicted = numSegments - 1; }
                    else { lastEvicted--; }

                    if (lastEvicted == start) {
                        sleep = true;
                        break;
                    }
                } while (status[lastEvicted] == SLOT_FIXED);

                if (sleep) { continue; }

                slots[lastEvicted] = next;
                status[lastEvicted] = SLOT_UNFIXED;
                readSlot = buffer + (lastEvicted * segmentSizeBytes);
            }

            w_assert0(readSlot);
            requests.pop_front();
        }

        // perform the read into the slot found
        shpid_t firstPage = shpid_t(next * segmentSize) + firstDataPid;
        INC_TSTAT(restore_backup_reads);
        W_COERCE(volume->read_backup(firstPage, segmentSize, readSlot));

        // signalize read to waiting threads
        CRITICAL_SECTION(cs2, &readCondMutex);
        DO_PTHREAD(pthread_cond_broadcast(&readCond));
    }
}
