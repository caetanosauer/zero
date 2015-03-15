#include "w_gc_pool_forest.h"
#include "w_lockfree_list.h"
#include <memory.h>
#include <set>
#include <pthread.h>
#include <AtomicCounter.hpp>
#include "gtest/gtest.h"
#include "local_random.h"

void mfence() {
    lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
}

struct DummyEntry : public GcPoolEntry {
    DummyEntry() : key(0), _deleted_count(0), _allocated(false) {}

    GcPointer<DummyEntry>   next;
    uint32_t                key;
    uint32_t _deleted_count;
    bool     _allocated;
};

////////////////////// Single-threaded tests BEGIN ///////////////////////////////////

TEST(LockFreeListTest, SingleThreadMixed) {
    GcPoolForest<DummyEntry> pool("DummyEntry", 10, 1, 100);
    LockFreeList<DummyEntry, uint32_t> the_list(&pool);
    gc_pointer_raw next;
    next.word = 0;
    gc_thread_id self = 0;

    EXPECT_EQ(0, the_list.unsafe_size());
    EXPECT_FALSE(the_list.contains(4));
    EXPECT_TRUE(the_list.unsafe_sorted());

    DummyEntry* item4 = the_list.get_or_add(4, next, self);
    EXPECT_EQ(4, item4->key);
    EXPECT_EQ(1, the_list.unsafe_size());
    EXPECT_TRUE(the_list.contains(4));
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);

    EXPECT_FALSE(the_list.contains(3));
    DummyEntry* item3 = the_list.get_or_add(3, next, self);
    EXPECT_TRUE(the_list.contains(3));
    EXPECT_TRUE(the_list.contains(4));
    EXPECT_EQ(3, item3->key);
    EXPECT_TRUE(item3 != item4);
    EXPECT_EQ(2, the_list.unsafe_size());
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);

    DummyEntry* item4_again = the_list.get_or_add(4, next, self);
    EXPECT_TRUE(the_list.contains(3));
    EXPECT_TRUE(the_list.contains(4));
    EXPECT_EQ(4, item4_again->key);
    EXPECT_TRUE(item4 == item4_again);
    EXPECT_EQ(2, the_list.unsafe_size());
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);

    DummyEntry* item3_again = the_list.get_or_add(3, next, self);
    EXPECT_TRUE(the_list.contains(3));
    EXPECT_TRUE(the_list.contains(4));
    EXPECT_EQ(3, item3_again->key);
    EXPECT_TRUE(item3 == item3_again);
    EXPECT_EQ(2, the_list.unsafe_size());
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);

    DummyEntry* item7 = the_list.get_or_add(7, next, self);
    EXPECT_TRUE(the_list.contains(3));
    EXPECT_TRUE(the_list.contains(4));
    EXPECT_TRUE(the_list.contains(7));
    EXPECT_EQ(7, item7->key);
    EXPECT_EQ(3, the_list.unsafe_size());
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);
    EXPECT_TRUE(the_list.remove(7));
    EXPECT_FALSE(the_list.contains(7));
    the_list.unsafe_dump_keys(std::cout);

    DummyEntry* item1 = the_list.get_or_add(1, next, self);
    EXPECT_EQ(1, item1->key);
    EXPECT_TRUE(the_list.contains(1));
    EXPECT_FALSE(the_list.contains(2));
    EXPECT_TRUE(the_list.contains(3));
    EXPECT_TRUE(the_list.contains(4));
    EXPECT_EQ(3, the_list.unsafe_size());
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);

    DummyEntry* item2 = the_list.get_or_add(2, next, self);
    EXPECT_EQ(2, item2->key);
    EXPECT_TRUE(the_list.contains(1));
    EXPECT_TRUE(the_list.contains(2));
    EXPECT_TRUE(the_list.contains(3));
    EXPECT_TRUE(the_list.contains(4));
    EXPECT_EQ(4, the_list.unsafe_size());
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);

    EXPECT_TRUE(the_list.contains(2));
    EXPECT_TRUE(the_list.remove(2));
    EXPECT_FALSE(the_list.contains(2));
    EXPECT_EQ(3, the_list.unsafe_size());
    EXPECT_FALSE(the_list.remove(2));
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);

    EXPECT_TRUE(the_list.contains(1));
    EXPECT_TRUE(the_list.remove(1));
    EXPECT_FALSE(the_list.contains(1));
    EXPECT_EQ(2, the_list.unsafe_size());
    EXPECT_FALSE(the_list.contains(1));
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);

    EXPECT_FALSE(the_list.remove(5));
    EXPECT_EQ(2, the_list.unsafe_size());
    EXPECT_TRUE(the_list.remove(4));
    EXPECT_TRUE(the_list.contains(3));
    EXPECT_FALSE(the_list.contains(4));
    EXPECT_EQ(1, the_list.unsafe_size());
    EXPECT_TRUE(the_list.unsafe_sorted());
    the_list.unsafe_dump_keys(std::cout);

    the_list.unsafe_clear();
    EXPECT_EQ(0, the_list.unsafe_size());
    EXPECT_TRUE(the_list.unsafe_sorted());
}

TEST(LockFreeListTest, SingleThreadRandom) {
    GcPoolForest<DummyEntry> pool("DummyEntry", 10, 1, 1000);
    gc_pointer_raw next;
    next.word = 0;
    gc_thread_id self = 0;

    tlr_t tlr(1234); // fixed seed for repeatability
    std::set<uint32_t> answer;
    LockFreeList<DummyEntry, uint32_t> the_list(&pool);

    for (int i = 0; i < 1000; ++i) {
        bool del = (tlr.nextInt32() % 5) == 0;
        uint32_t key = tlr.nextInt32() % 500;
        if (del) {
            bool erased_correct = answer.erase(key) != 0;
            bool erased = the_list.remove(key);
            EXPECT_EQ(erased_correct, erased) << "del i=" << i << ", key=" << key;
        } else {
            answer.insert(key);
            the_list.get_or_add(key, next, self);
        }
        EXPECT_EQ(answer.size(), the_list.unsafe_size()) << "i=" << i << ", key=" << key;
        EXPECT_TRUE(the_list.unsafe_sorted()) << "i=" << i << ", key=" << key;
    }

    EXPECT_TRUE(the_list.unsafe_sorted());
    std::set<uint32_t> result;
    the_list.unsafe_as_set(result);
    EXPECT_EQ(answer, result);

    the_list.unsafe_clear();
    EXPECT_EQ(0, the_list.unsafe_size());
}

////////////////////// Single-threaded tests END ///////////////////////////////////

////////////////////// Multi-threaded tests BEGIN ///////////////////////////////////
// Below, we invoke the list from multi-threads and verify the result with "correct"
// answer. However, the "correct" depends on the execution order if there is a deletion.
// Thus, here we have 3 tests.
// 1) insert-only test. here we can do exact result comparison.
// 2) insert-pause-delete test. because we do delete after pause, we can exactly check, too.
// 3) mixed test. We concurrently run insert/delete, so we can't do exact result comparison.

const int THREAD_COUNT = 6;
#ifdef DEBUG
const int REP_COUNT = 3000;
#else // DEBUG
const int REP_COUNT = 10000;
#endif // DEBUG

struct TestSharedContext {
    TestSharedContext(GcPoolForest<DummyEntry> &pool, bool pause, bool ins)
        : the_list(&pool), pause_before_delete(pause), insert_only(ins) {
        workers_pausing = 0;
        ::pthread_mutex_init(&workers_mutex, NULL);
        ::pthread_cond_init (&workers_cond, NULL);
    }
    ~TestSharedContext() {
        ::pthread_mutex_destroy(&workers_mutex);
        ::pthread_cond_destroy(&workers_cond);
    }
    LockFreeList<DummyEntry, uint32_t> the_list;
    pthread_mutex_t workers_mutex; // used to change condition variables below
    pthread_cond_t  workers_cond;
    int             workers_pausing; // when this becomes THREAD_COUNT, pause ends.

    bool            pause_before_delete;
    bool            insert_only;
};
struct TestThreadContext {
    int id;
    TestSharedContext *shared;
    /** What this worker inserted. can be dup, but fine. */
    std::vector<uint32_t> inserted;
    /** What this worker deleted. can be dup, but fine. */
    std::vector<uint32_t> deleted;
};

void *test_work(void *t) {
    TestThreadContext &context = *reinterpret_cast<TestThreadContext*>(t);
    TestSharedContext &shared = *context.shared;
    tlr_t rand (context.id);
    gc_pointer_raw next;
    next.word = 0;
    gc_thread_id self = context.id;

    mfence();
    std::cout << "Worker-" << context.id << " started" << std::endl;
    std::vector<uint32_t> planned_deletes;
    // start!
    for (int i = 0; i < REP_COUNT; ++i) {
        uint32_t key = rand.nextInt32() % (REP_COUNT * 3);
        bool del = (rand.nextInt32() % 5) == 0;
        if (!shared.insert_only && del) {
            if (shared.pause_before_delete) {
                planned_deletes.push_back(key);
            } else {
                shared.the_list.remove(key); // delete along with insert
            }
        } else {
            context.inserted.push_back(key);
            shared.the_list.get_or_add(key, next, self);
        }
    }

    std::cout << "Worker-" << context.id << " inserted " << context.inserted.size()
        << " entries. planned_deletes.size()=" << planned_deletes.size() << std::endl;
    mfence();

    // done with insert. should we pause before delete?
    if (planned_deletes.size() > 0) {
        std::cout << "Worker-" << context.id << " went into sleep" << std::endl;
        ::pthread_mutex_lock(&shared.workers_mutex);
        ++shared.workers_pausing;
        while (shared.workers_pausing != THREAD_COUNT) {
            ::pthread_cond_wait(&shared.workers_cond, &shared.workers_mutex);
        }
        ::pthread_cond_broadcast(&shared.workers_cond); // let others know, too
        ::pthread_mutex_unlock(&shared.workers_mutex);
        std::cout << "Worker-" << context.id << " waked up. now deleting.." << std::endl;

        mfence();
        for (std::vector<uint32_t>::iterator it = planned_deletes.begin();
             it != planned_deletes.end(); ++it) {
            uint32_t key = *it;
            context.deleted.push_back(key);
            shared.the_list.remove(key);
        }
    }

    std::cout << "done:" << context.id << std::endl;
    ::pthread_exit(NULL);
    return NULL;
}


void multi_thread_test(bool pause_before_delete, bool insert_only) {
    GcPoolForest<DummyEntry> pool("DummyEntry", 10, THREAD_COUNT * 2, REP_COUNT);
    TestSharedContext shared (pool, pause_before_delete, insert_only);

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
    EXPECT_TRUE(shared.the_list.unsafe_sorted());
    if (!pause_before_delete && !insert_only) {
        // then we can't actually check results.
        return;
    }

    // because deletion happends after pause, we can check the result
    std::set<uint32_t> answer;
    for (int i = 0; i < THREAD_COUNT; ++i) {
        for (std::vector<uint32_t>::iterator it = contexts[i].inserted.begin();
             it != contexts[i].inserted.end(); ++it) {
            answer.insert(*it);
        }
    }
    // inserts/deletes might have dup, but fine as far as we do them separately
    for (int i = 0; i < THREAD_COUNT; ++i) {
        for (std::vector<uint32_t>::iterator it = contexts[i].deleted.begin();
             it != contexts[i].deleted.end(); ++it) {
            answer.erase(*it);
        }
    }

    EXPECT_EQ(answer.size(), shared.the_list.unsafe_size());
    std::set<uint32_t> std_set;
    shared.the_list.unsafe_as_set(std_set);
    EXPECT_EQ(answer, std_set);
    if (answer != std_set) {
        // shared.the_list.unsafe_dump_keys(std::cout);
        std::cout << "Redundant entries:";
        std::set<uint32_t> diff(std_set);
        for (std::set<uint32_t>::iterator it = answer.begin(); it != answer.end(); ++it) {
            diff.erase(*it);
        }
        for (std::set<uint32_t>::iterator it = diff.begin(); it != diff.end(); ++it) {
            std::cout << *it << ",";
        }
        std::cout << std::endl;

        std::cout << "Missing entries:";
        diff = answer;
        for (std::set<uint32_t>::iterator it = std_set.begin(); it != std_set.end(); ++it) {
            diff.erase(*it);
        }
        for (std::set<uint32_t>::iterator it = diff.begin(); it != diff.end(); ++it) {
            std::cout << *it << ",";
        }
        std::cout << std::endl;
    }
}

TEST(LockFreeListTest, MultiThreadInsertOnly) {
    multi_thread_test(false, true);
}
TEST(LockFreeListTest, MultiThreadDeleteAfterPause) {
    multi_thread_test(true, false);
}
TEST(LockFreeListTest, MultiThreadMixed) {
    multi_thread_test(false, false);
}

////////////////////// Multi-threaded tests END ///////////////////////////////////
