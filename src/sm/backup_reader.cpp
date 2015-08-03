#include "backup_reader.h"

#include "vol.h"

const std::string DummyBackupReader::IMPL_NAME = "dummy";
const std::string BackupOnDemandReader::IMPL_NAME = "ondemand";
const std::string BackupPrefetcher::IMPL_NAME = "prefetcher";

BackupOnDemandReader::BackupOnDemandReader(vol_t* volume, size_t segmentSize)
    : BackupReader(segmentSize * sizeof(generic_page)),
      volume(volume), segmentSize(segmentSize)
{
    w_assert1(volume);
    W_IFDEBUG1(fixedSegment = -1);
}

char* BackupOnDemandReader::fix(unsigned segment)
{
    INC_TSTAT(restore_backup_reads);
    w_assert1(fixedSegment < 0);

    W_COERCE(volume->read_backup(shpid_t(segment * segmentSize), segmentSize,
                buffer));

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
    // initialize all slots with -1, i.e., free
    slots = new int[numSegments];
    for (size_t i = 0; i < numSegments; i++) {
        slots[i] = -1;
    }
    fixedSegment = -1;

    DO_PTHREAD(pthread_cond_init(&readCond, NULL));
    DO_PTHREAD(pthread_mutex_init(&readCondMutex, NULL));
}

BackupPrefetcher::~BackupPrefetcher()
{
    DO_PTHREAD(pthread_mutex_destroy(&readCondMutex));
    DO_PTHREAD(pthread_cond_destroy(&readCond));
}

void BackupPrefetcher::prefetch(unsigned segment, int priority)
{
    spinlock_write_critical_section cs(&requestMutex);

    // if segment was already fetched, ignore
    for (size_t i = 0; i < numSegments; i++)
    {
        if (slots[i] == (int) segment) {
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

char* BackupPrefetcher::fix(unsigned segment)
{
    while (true) {
        {
            spinlock_write_critical_section cs(&requestMutex);

            // only one fixed segment at a time (i.e., sequential restore)
            w_assert0(fixedSegment < 0);

            // look for segment in buffer -- majority of calls should end here
            for (size_t i = 0; i < numSegments; i++) {
                if (slots[i] == (int) segment) {
                    fixWaiting = false;
                    return buffer + (i * segmentSizeBytes);
                }
            }

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
    w_assert0((int) segment == fixedSegment);
    fixedSegment = -1;
}

void BackupPrefetcher::run()
{
    while (volume->is_failed()) {
        char* readSlot = NULL;
        unsigned next = numSegments;
        {
            spinlock_write_critical_section cs(&requestMutex);
            next = requests.front();

            // look if segment is already in buffer
            for (size_t i = 0; i < numSegments; i++) {
                if (slots[i] == (int) next) {
                    requests.pop_front();
                    continue;
                }
            }

            // segment not in buffer, look for a free slot for reading into
            for (size_t i = 0; i < numSegments; i++) {
                if (slots[i] < 0) {
                    readSlot = buffer + (i * segmentSizeBytes);
                    slots[i] = next;
                }
            }

            if (!readSlot) {
                if (!fixWaiting) {
                    // buffer full -- wait and try again
                    usleep(1000); // 1ms
                    continue;
                }

                // A fix is waiting and the buffer is full -- must evict.
                // If buffer is size 1 and a segment is already fixed,
                // we have a bug.
                w_assert0(numSegments > 1 || fixedSegment > 0);
                do {
                    if (lastEvicted == 0) {
                        lastEvicted = numSegments - 1;
                    }
                    else {
                        lastEvicted--;
                    }
                } while (slots[lastEvicted] == fixedSegment);

                slots[lastEvicted] = next;
                readSlot = buffer + (lastEvicted * segmentSizeBytes);
            }

            w_assert0(readSlot);
            requests.pop_front();
        }

        // perform the read into the slot found
        shpid_t firstPage = shpid_t(next * segmentSize);
        INC_TSTAT(restore_backup_reads);
        W_COERCE(volume->read_backup(firstPage, segmentSize, readSlot));

        // signalize read to waiting threads
        DO_PTHREAD(pthread_mutex_lock(&readCondMutex));
        DO_PTHREAD(pthread_cond_broadcast(&readCond));
        DO_PTHREAD(pthread_mutex_unlock(&readCondMutex));
    }
}
