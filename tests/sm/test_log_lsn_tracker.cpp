#include <pthread.h>
#include <AtomicCounter.hpp>
#include "gtest/gtest.h"
#include "log_lsn_tracker.h"
#include "../common/local_random.h"

TEST(LogLsnTrackerTest, Simple) {
    PoorMansOldestLsnTracker tracker(1 << 10);
    tracker.enter(333, lsn_t(123));
    tracker.enter(4323, lsn_t(140));
    EXPECT_EQ(lsn_t(123), tracker.get_oldest_active_lsn(100000));
    tracker.leave(333);
    EXPECT_EQ(lsn_t(140), tracker.get_oldest_active_lsn(100000));
    tracker.leave(4323);
    EXPECT_EQ(lsn_t(100000), tracker.get_oldest_active_lsn(100000));
}

const int THREAD_COUNT = 6;
#ifdef DEBUG
const int REP_COUNT = 30000;
#else // DEBUG
const int REP_COUNT = 100000;
#endif // DEBUG

struct TestSharedContext {
    TestSharedContext(PoorMansOldestLsnTracker &tracker_arg) : tracker(&tracker_arg), next(1) {}
    PoorMansOldestLsnTracker*   tracker;
    lsndata_t                   next;
};

struct TestThreadContext {
    int id;
    TestSharedContext *shared;
};

void *test_work(void *t) {
    TestThreadContext &context = *reinterpret_cast<TestThreadContext*>(t);
    TestSharedContext &shared = *context.shared;
    tlr_t rand (context.id);
    std::cout << "Worker-" << context.id << " started" << std::endl;
    for (int i = 0; i < REP_COUNT; ++i) {
        uint32_t xct = rand.nextInt32();
        lsndata_t lsn = lintel::unsafe::atomic_fetch_add<lsndata_t>(&(shared.next), 1);
        if (xct == 0 || lsn == 0) {
            continue;
        }
        shared.tracker->enter(xct, lsn_t(lsn));
        shared.tracker->leave(xct);
        if (i % 10000 == (context.id * 1000)) {
            std::cout << "current oldest="
                << shared.tracker->get_oldest_active_lsn(shared.next).data() << std::endl;
        }
    }
    std::cout << "done:" << context.id << std::endl;
    ::pthread_exit(NULL);
    return NULL;
}

TEST(LogLsnTrackerTest, Parallel) {
    PoorMansOldestLsnTracker tracker(1 << 6); // there will be many collisions, testing spins.
    TestSharedContext shared(tracker);

    pthread_attr_t join_attr;
    void *join_status;
    ::pthread_attr_init(&join_attr);
    ::pthread_attr_setdetachstate(&join_attr, PTHREAD_CREATE_JOINABLE);

    TestThreadContext contexts[THREAD_COUNT];
    pthread_t *threads = new pthread_t[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i) {
        contexts[i].id = i;
        contexts[i].shared = &shared;
        int rc = ::pthread_create(threads + i, &join_attr, test_work, contexts + i);
        EXPECT_EQ(0, rc) << "pthread_create failed";
    }

    for (int i = 0; i < THREAD_COUNT; ++i) {
        int rc = ::pthread_join(threads[i], &join_status);
        EXPECT_EQ(0, rc) << "pthread_join failed";
    }

    ::pthread_attr_destroy(&join_attr);
    delete[] threads;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
