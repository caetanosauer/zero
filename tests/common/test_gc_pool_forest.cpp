#include <pthread.h>
#include <AtomicCounter.hpp>
#include "w_gc_pool_forest.h"
#include "gtest/gtest.h"

struct DummyEntry : public GcPoolEntry {
    DummyEntry() : _key(0) {}
    DummyEntry(uint32_t key) : _key(key) {}
    uint32_t _key;
};

void mfence() {
    lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
}

gc_thread_id self() {
    return static_cast<gc_thread_id>(::pthread_self());
}


TEST(GcPoolForestTest, SingleThread) {
    gc_pointer_raw next;
    next.word = 0; // don't bother __thread or C++11.
    GcPoolForest<DummyEntry> forest("unnamed", 5, 0, 0);
    const size_t SEGMENTS = 2;
    const size_t SEGMENT_SIZE = 3;
    forest.resolve_generation(forest.curr())->preallocate_segments(SEGMENTS , SEGMENT_SIZE);
    DummyEntry *entry1 = forest.allocate(next, self());
    EXPECT_EQ(1, entry1->gc_pointer.components.generation);
    EXPECT_EQ(0, entry1->gc_pointer.components.segment);
    EXPECT_EQ(0, entry1->gc_pointer.components.offset);
    EXPECT_EQ(1, next.components.generation);
    EXPECT_EQ(0, next.components.segment);
    EXPECT_EQ(1, next.components.offset);

    GcGeneration<DummyEntry> *gen1 = forest.resolve_generation(entry1->gc_pointer);
    GcSegment<DummyEntry> *seg1 = forest.resolve_segment(entry1->gc_pointer);
    EXPECT_EQ(SEGMENTS, gen1->total_segments);
    EXPECT_EQ((uint32_t) 1, gen1->allocated_segments);

    DummyEntry *entry2 = forest.allocate(next, self());
    EXPECT_EQ(1, entry2->gc_pointer.components.generation);
    EXPECT_EQ(0, entry2->gc_pointer.components.segment);
    EXPECT_EQ(1, entry2->gc_pointer.components.offset);
    EXPECT_EQ(1, next.components.generation);
    EXPECT_EQ(0, next.components.segment);
    EXPECT_EQ(2, next.components.offset);

    EXPECT_EQ(seg1, forest.resolve_segment(entry2->gc_pointer));

    DummyEntry *entry3 = forest.allocate(next, self());
    EXPECT_EQ(1, entry3->gc_pointer.components.generation);
    EXPECT_EQ(0, entry3->gc_pointer.components.segment);
    EXPECT_EQ(2, entry3->gc_pointer.components.offset);
    EXPECT_EQ(1, next.components.generation);
    EXPECT_EQ(0, next.components.segment);
    EXPECT_EQ(3, next.components.offset);

    EXPECT_EQ(seg1, forest.resolve_segment(entry3->gc_pointer));

    // now we should switch to next segment
    DummyEntry *entry4 = forest.allocate(next, self());
    EXPECT_EQ(1, entry4->gc_pointer.components.generation);
    EXPECT_EQ(1, entry4->gc_pointer.components.segment);
    EXPECT_EQ(0, entry4->gc_pointer.components.offset);
    EXPECT_EQ(1, next.components.generation);
    EXPECT_EQ(1, next.components.segment);
    EXPECT_EQ(1, next.components.offset);

    EXPECT_NE(seg1, forest.resolve_segment(entry4->gc_pointer));
    EXPECT_EQ(gen1, forest.resolve_generation(entry4->gc_pointer));

    EXPECT_EQ((uint32_t) SEGMENTS, gen1->total_segments);
    EXPECT_EQ((uint32_t) 2, gen1->allocated_segments);

    EXPECT_NE(entry1, entry2);
    EXPECT_NE(entry2, entry3);
    EXPECT_NE(entry1, entry3);
    EXPECT_NE(entry1, entry4);

    forest.deallocate(entry1);
    forest.deallocate(entry2);

    EXPECT_EQ(SEGMENTS, gen1->total_segments);
    EXPECT_EQ((uint32_t) 2, gen1->allocated_segments);
    forest.deallocate(entry3);

    EXPECT_EQ(SEGMENTS, gen1->total_segments);
    EXPECT_EQ((uint32_t) 2, gen1->allocated_segments);

    forest.deallocate(entry4);

    EXPECT_EQ(SEGMENTS, gen1->total_segments);
    EXPECT_EQ((uint32_t) 2, gen1->allocated_segments);

    // create a few new generations
    for (int i = 0; i < 5; ++i) {
        lsn_t now(100 * (i + 1));
        EXPECT_TRUE(forest.advance_generation(lsn_t::null, now, 0, 0));
    }

    // start retiring
    {
        lsn_t now(50);
        EXPECT_EQ((uint32_t) 6, forest.active_generations());
        forest.retire_generations(now);
        EXPECT_EQ((uint32_t) 6, forest.active_generations());
    }
    {
        lsn_t now(520);
        EXPECT_EQ((uint32_t) 6, forest.active_generations());
        forest.retire_generations(now);
        EXPECT_GT((uint32_t) 6, forest.active_generations());
    }

    // let's also wrap. try recycle.
    for (size_t i = 0; i < GC_MAX_GENERATIONS; ++i) {
        lsn_t now(1000 + 100 * (i + 1));
        lsn_t then(1000 + 100 * (i + 2));
        EXPECT_TRUE(forest.advance_generation(lsn_t::null, now, 0, 0));
        forest.retire_generations(now, then);
        EXPECT_GT((uint32_t) 20, forest.active_generations());
    }
    EXPECT_GT((uint32_t) 20, forest.active_generations());
}

const int THREAD_COUNT = 6;

struct TestSharedContext {
    TestSharedContext(GcPoolForest<DummyEntry> *the_forest) {
        stop_requested = false;
        forest = the_forest;
    }
    GcPoolForest<DummyEntry> *forest;
    gc_generation       recommended_generation;
    bool                stop_requested;
};
struct TestThreadContext {
    int id;
    TestSharedContext *shared;
    bool stopped;
};

void *test_work(void *t) {
    TestThreadContext &context = *reinterpret_cast<TestThreadContext*>(t);
    context.stopped = false;
    TestSharedContext &shared = *context.shared;
    mfence();

    std::cout << "Worker-" << context.id << " started" << std::endl;
    size_t allocated = 0;
    gc_pointer_raw next;
    next.word = 0;
    while (!shared.stop_requested) {
        mfence();
        if (shared.recommended_generation != next.components.generation) {
            next.word = 0;
        }
        DummyEntry* p = shared.forest->allocate(next, context.id);
        EXPECT_TRUE(p != NULL);
        shared.forest->deallocate(p);
    }

    std::cout << "Worker-" << context.id << " done. allocated " << allocated << std::endl;
    context.stopped = true;
    ::pthread_exit(NULL);
    return NULL;
}

TEST(GcPoolForestTest, MultiThread) {
    GcPoolForest<DummyEntry> forest("unnamed", 5, 0, 0);

    TestSharedContext shared(&forest);

    lsn_t now(2000);
    EXPECT_TRUE(forest.advance_generation(lsn_t::null, now, 0, 0));
    shared.recommended_generation = forest.curr();
    forest.resolve_generation(forest.curr())->preallocate_segments(100, 1000);

    TestThreadContext contexts[THREAD_COUNT];
    pthread_t *threads = new pthread_t[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i) {
        contexts[i].id = i;
        contexts[i].shared = &shared;
        int rc = ::pthread_create(threads + i, NULL, test_work, contexts + i);
        EXPECT_EQ(0, rc) << "pthread_create failed";
    }

    // while the workers are allocating, let's keep making new generations
    for (int i = 0; i < 100; ++i) {
        mfence();
        if (i % 10 == 0) {
            std::cout << "[master thread] Advancing generation " << i << std::endl;
            lsn_t now(100 * (i + 30));
            EXPECT_TRUE(forest.advance_generation(lsn_t::null, now, 10, 1000));
            // let clients know, too
            shared.recommended_generation = forest.curr();
            mfence();
        }

        // pre-allocate a few new segments
        EXPECT_TRUE(forest.resolve_generation(forest.curr())->preallocate_segments(3, 1000));

        if (i % 10 == 8) {
            std::cout << "[master thread] Retiring old generation" << std::endl;
            lsn_t back_then(100 * (i + 30 - 2));
            forest.retire_generations(back_then);
        }

        std::cout << "[master thread] sleeping for a bit.." << std::endl;
        ::usleep(1000);
    }

    shared.stop_requested = true;
    mfence();
    std::cout << "[master thread] announced the end. waiting for workers to end" << std::endl;

    // in case some thread is now waiting for a new segment, keep creating
    for (int i = 100;; ++i) {
        mfence();
        if (i % 10 == 0) {
            std::cout << "[master thread] Advancing generation " << i << std::endl;
            lsn_t now(100 * (i + 30));
            EXPECT_TRUE(forest.advance_generation(lsn_t::null, now, 10, 1000));
            // let clients know, too
            shared.recommended_generation = forest.curr();
            mfence();
        }

        // pre-allocate a few new segments
        EXPECT_TRUE(forest.resolve_generation(forest.curr())->preallocate_segments(3, 1000));

        if (i % 10 == 8) {
            std::cout << "[master thread] Retiring old generation" << std::endl;
            lsn_t back_then(100 * (i + 30 - 2));
            forest.retire_generations(back_then);
        }

        std::cout << "[master thread] sleeping for a bit.." << std::endl;

        bool all_stopped = true;
        for (int j = 0; j < THREAD_COUNT; ++j) {
            if (!contexts[j].stopped) {
                all_stopped = false;
            }
        }
        if (all_stopped) {
            break;
        }
    }

    delete[] threads;
    std::cout << "All done!" << std::endl;
}
