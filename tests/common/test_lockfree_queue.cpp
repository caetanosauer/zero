#include "w_markable_pointer.h"
#include "w_lockfree_queue.h"
#include <memory.h>
#include <set>
#include <list>
#include <pthread.h>
#include <AtomicCounter.hpp>
#include "gtest/gtest.h"
#include "local_random.h"

void mfence() {
    lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
}

struct DummyEntry : public MarkablePointerChain< DummyEntry > {
    DummyEntry() : _key(0) {}
    DummyEntry(uint32_t key) : _key(key) {}
    uint32_t _key;
};

void check_size(size_t expected, LockFreeQueue<DummyEntry> &the_queue) {
    // at least in single-thread situation, all of them should be correct
    EXPECT_EQ(expected, the_queue.unsafe_size());
    EXPECT_EQ(expected, the_queue.safe_size());
    EXPECT_EQ(expected, the_queue.approximate_size());
}

bool contains(uint32_t key, LockFreeQueue<DummyEntry> &the_queue) {
    std::vector<DummyEntry*> out;
    the_queue.unsafe_as_vector(out);
    for (size_t i = 0; i < out.size(); ++i) {
        if (out[i]->_key == key) {
            return true;
        }
    }
    return false;
}

void enqueue(LockFreeQueue<DummyEntry> &the_queue, std::list<DummyEntry*> &enqueued,
             uint32_t key) {
    DummyEntry* entry = new DummyEntry(key);
    SCOPED_TRACE(testing::Message() << "new pointer=0x" << std::hex <<
        reinterpret_cast<uintptr_t>(entry) << std::dec << ", key=" << key);
    enqueued.push_back(entry);
    EXPECT_FALSE(contains(key, the_queue));

    the_queue.enqueue(entry);

    the_queue.unsafe_dump(std::cout);
    EXPECT_TRUE(contains(key, the_queue));
    EXPECT_TRUE(the_queue.unsafe_consistent());
    check_size(enqueued.size(), the_queue);

    the_queue.unsafe_dump(std::cout);
}

void dequeue(LockFreeQueue<DummyEntry> &the_queue, std::list<DummyEntry*> &enqueued) {
    DummyEntry* expected = enqueued.front(); enqueued.pop_front();
    SCOPED_TRACE(testing::Message() << "expected=0x" << std::hex <<
        reinterpret_cast<uintptr_t>(expected) << std::dec << ", key=" << expected->_key);
    EXPECT_TRUE(contains(expected->_key, the_queue));

    DummyEntry* ret = the_queue.dequeue();
    EXPECT_EQ(expected->_key, ret->_key);
    EXPECT_EQ(expected, ret);

    EXPECT_FALSE(contains(expected->_key, the_queue));
    EXPECT_TRUE(the_queue.unsafe_consistent());
    check_size(enqueued.size(), the_queue);

    the_queue.unsafe_dump(std::cout);
    delete ret;
}

////////////////////// Single-threaded tests BEGIN ///////////////////////////////////

TEST(LockFreeQueueTest, SingleThreadMixed) {
    LockFreeQueue<DummyEntry> the_queue;
    std::list<DummyEntry*> enqueued;

    check_size(0, the_queue);
    EXPECT_TRUE(the_queue.unsafe_consistent());
    EXPECT_EQ(NULL, the_queue.dequeue());
    EXPECT_TRUE(the_queue.unsafe_consistent());

    enqueue(the_queue, enqueued, 4);
    enqueue(the_queue, enqueued, 3);
    enqueue(the_queue, enqueued, 7);
    EXPECT_TRUE(contains(3, the_queue));
    EXPECT_TRUE(contains(4, the_queue));
    EXPECT_TRUE(contains(7, the_queue));
    dequeue(the_queue, enqueued); // pop 4
    enqueue(the_queue, enqueued, 1);
    enqueue(the_queue, enqueued, 2);
    EXPECT_TRUE(contains(1, the_queue));
    EXPECT_TRUE(contains(2, the_queue));
    EXPECT_FALSE(contains(3, the_queue));
    EXPECT_TRUE(contains(4, the_queue));
    EXPECT_TRUE(contains(7, the_queue));

    dequeue(the_queue, enqueued); // pop 3
    dequeue(the_queue, enqueued); // pop 7
    dequeue(the_queue, enqueued); // pop 1
    EXPECT_FALSE(contains(1, the_queue));
    EXPECT_TRUE(contains(2, the_queue));
    EXPECT_FALSE(contains(3, the_queue));
    EXPECT_FALSE(contains(4, the_queue));
    EXPECT_FALSE(contains(7, the_queue));
    dequeue(the_queue, enqueued); // pop 2

    EXPECT_EQ(NULL, the_queue.dequeue());

    the_queue.unsafe_clear();
    check_size(0, the_queue);
    EXPECT_TRUE(the_queue.unsafe_consistent());
}

TEST(LockFreeQueueTest, SingleThreadRandom) {
    tlr_t tlr(1234); // fixed seed for repeatability
    std::list<DummyEntry*> answer;
    LockFreeQueue<DummyEntry> the_queue;

    for (int i = 0; i < 1000; ++i) {
        bool del = (tlr.nextInt32() % 5) == 0;
        uint32_t key = tlr.nextInt32() % 500;
        if (del) {
            if (answer.size() == 0) {
                EXPECT_EQ(NULL, the_queue.dequeue());
            } else {
                DummyEntry* erased = the_queue.dequeue();
                DummyEntry* correct = answer.front(); answer.pop_front();
                EXPECT_EQ(correct, erased) << "del i=" << i << ", key=" << key;
                delete erased;
            }
        } else {
            DummyEntry* entry = new DummyEntry(key);
            answer.push_back(entry);
            the_queue.enqueue(entry);
        }
        EXPECT_EQ(answer.size(), the_queue.unsafe_size()) << "i=" << i << ", key=" << key;
        EXPECT_TRUE(the_queue.unsafe_consistent()) << "i=" << i << ", key=" << key;
    }

    EXPECT_TRUE(the_queue.unsafe_consistent());
    std::vector<DummyEntry*> result;
    the_queue.unsafe_as_vector(result);
    EXPECT_EQ(answer.size(), result.size());
    for (size_t i = 0; i < result.size(); ++i) {
        delete result[i];
    }

    the_queue.unsafe_clear();
    check_size(0, the_queue);
}

////////////////////// Single-threaded tests END ///////////////////////////////////

////////////////////// Multi-threaded tests BEGIN ///////////////////////////////////
// Queue provides less functionality than list-based set, so we have only 2 tests.
// 1) insert-only test.
// 2) mixed test. We concurrently run insert/delete.
// In both cases, we can do only count check
// Further, in 2) we make sure each thread doesn't dequeue more than it enqueued so far.

const int THREAD_COUNT = 6;
#ifdef DEBUG
const int REP_COUNT = 3000;
#else // DEBUG
const int REP_COUNT = 10000;
#endif // DEBUG

struct TestSharedContext {
    TestSharedContext(bool ins)
        : the_queue(), insert_only(ins) {
    }
    LockFreeQueue<DummyEntry> the_queue;
    bool            insert_only;
};
struct TestThreadContext {
    int id;
    TestSharedContext *shared;
    /** What this worker inserted. can be dup, but fine. */
    uint32_t    inserted_count;
    /** How many this worker deleted. */
    uint32_t    deleted_count;
};

void *test_work(void *t) {
    TestThreadContext &context = *reinterpret_cast<TestThreadContext*>(t);
    TestSharedContext &shared = *context.shared;
    tlr_t rand (context.id);

    mfence();
    std::cout << "Worker-" << context.id << " started" << std::endl;
    context.inserted_count = 0;
    context.deleted_count = 0;
    // start!
    for (int i = 0; i < REP_COUNT; ++i) {
        uint32_t key = rand.nextInt32() % (REP_COUNT * 3);
        bool del = (rand.nextInt32() % 5) == 0;
        if (!shared.insert_only && del && context.inserted_count < context.deleted_count) {
            ++context.deleted_count;
            delete shared.the_queue.dequeue();
        } else {
            ++context.inserted_count;
            shared.the_queue.enqueue(new DummyEntry(key));
        }
    }

    std::cout << "Worker-" << context.id << " inserted " << context.inserted_count
        << " entries. deleted=" << context.deleted_count << std::endl;
    ::pthread_exit(NULL);
    return NULL;
}


void multi_thread_test(bool insert_only) {
    TestSharedContext shared (insert_only);

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

    mfence();

    ::pthread_attr_destroy(&join_attr);
    delete[] threads;

    std::cout << "done all! checking results..." << std::endl;
    EXPECT_TRUE(shared.the_queue.unsafe_consistent());

    for (int i = 0; i < THREAD_COUNT; ++i) {
        int rc = ::pthread_join(threads[i], &join_status);
        EXPECT_EQ(0, rc) << "pthread_join failed";
    }

    mfence();
    uint32_t total_inserted = 0;
    uint32_t total_deleted = 0;
    for (int i = 0; i < THREAD_COUNT; ++i) {
        total_inserted += contexts[i].inserted_count;
        total_deleted += contexts[i].deleted_count;
    }

    EXPECT_EQ(total_inserted - total_deleted, shared.the_queue.unsafe_size());

    uint32_t dequeued = 0;
    while (true) {
        DummyEntry* deq = shared.the_queue.dequeue();
        if (deq == NULL) {
            break;
        }
        delete deq;
        ++dequeued;
    }
    EXPECT_EQ(total_inserted - total_deleted, dequeued);
}

TEST(LockFreeQueueTest, MultiThreadInsertOnly) {
    multi_thread_test(true);
}
TEST(LockFreeQueueTest, MultiThreadMixed) {
    multi_thread_test(false);
}

////////////////////// Multi-threaded tests END ///////////////////////////////////
