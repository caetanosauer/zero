/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#ifndef W_GC_POOL_FOREST_H
#define W_GC_POOL_FOREST_H

#include <stdint.h>
#include <memory>
#include <AtomicCounter.hpp>
#include "w_defines.h"
#include "w_debug.h"
#include "lsn.h"

/**
 * \brief Garbage-collected Object-Pool Forests as in [JUNG13].
 * \defgroup GCFOREST Garbage-collected Object-Pool Forests
 * \ingroup LOCKFREE
 * \details
 * \section OVERVIEW Overview
 * We maintain a \e forest of object pools with garbage-collector for each type of object.
 * Such an object pool is quite effective to address the following issues in lock-free
 * algorithms:
 *  \li Garbage collection and Safe Memory Reclamation (SMR)
 *  \li ABA Problem
 *  \li Portability (c.f. "bit-stealing" pointer packing techniques, and double-word CAS algo)
 *  \li Object allocation/deallocation costs
 *
 * The usual drawback of this approach is that there are few one-size-fits-all GC design,
 * so it has either unwanted overheads or some restrictions on users.
 * However, this is our Roll-Your-Own Garbage Collector implementation optimized for
 * long-running services with small number of object types, such as filesystems and databases.
 *
 * Rather than general and more advanced techniques such as Hazard Pointers,
 * this is much more simple, robust, efficient, and satisfactory for our usecase.
 *
 * \section HIERARCHY Hierarchy
 * The GC Forest consists of the following three levels.
 *  \li \b Generation, which groups objects in similar age and is retired together.
 *  \li \b Segement, which is the unit of bulk allocation/deallocation.
 *  \li \b Object, which is the pooled object.
 *
 * \section REF References
 *   \li [JUNG13] "A scalable lock manager for multicores"
 *   Hyungsoo Jung, Hyuck Han, Alan D. Fekete, Gernot Heiser, Heon Y. Yeom. SIGMOD'13.
 *
 * \section DEP Dependency
 * All the classes are header-only template classes. Just include w_gc_pool_forest.h to use.
 */

template <class T> class GcPointer;
template <class T> struct GcSegment;
template <class T> struct GcGeneration;
template <class T> struct GcPoolForest;

/**
 * \brief Status bits in gc_pointer.
 * \ingroup GCFOREST
 * \details
 * This is logically not part of the pointer, but additional bits for lock-free algorithms.
 *   \li The higest bit (sign bit) is \b mark-for-death. TRUE means the object is
 * logically deactivated in lock-free data structures.
 *   \li The remaining 31 bits are \b ABA-counter. Used to prevent ABA issues in
 * lock-free data structures. 31 bits should be fairly safe.
 */
typedef int32_t gc_status;

/**
 * ABA counter bits in gc_pointer.
 * \ingroup GCFOREST
 */
typedef uint32_t gc_aba;

/**
 * Generation bits in gc_pointer (0 means an invalid generation = NULL).
 * \ingroup GCFOREST
 */
typedef uint8_t gc_generation;
const size_t GC_MAX_GENERATIONS = (1 << (sizeof(gc_generation) * 8));

/**
 * Segment bits in gc_pointer.
 * \ingroup GCFOREST
 */
typedef uint8_t gc_segment;
const size_t GC_MAX_SEGMENTS = (1 << (sizeof(gc_segment) * 8));
/**
 * Offset bits in gc_pointer (not in bytes but in \e objects).
 * \ingroup GCFOREST
 */
typedef uint16_t gc_offset;
const size_t GC_MAX_OFFSETS = (1 << (sizeof(gc_offset) * 8));
/**
 * Identifier of threads (which might be pthread_self() or something else).
 * \ingroup GCFOREST
 */
typedef uint64_t gc_thread_id;

/**
 * \brief A portable and logical pointer with mark-for-death, ABA counter,
 * and generation/segement bits.
 * \ingroup GCFOREST
 * \details
 * \section MARK Marked For Death
 * Same as MarkablePointer.
 * \section WHY What it is for
 * As described in Chap 10.6 of [HERLIHY], there is the \b ABA \b problem.
 * When we new/delete classes/structs, they reuse the same memory area.
 * Hence, we need to make sure the same pointer is differentiable.
 * The common solution is to implant some counter in the 64bit pointer like the mark for death.
 * Intel64/AMD64 so far uses only 48 bits (http://en.wikipedia.org/wiki/X86-64),
 * so we can steal the most significant 16 bits to store a stamp value.
 * However, 16 bits is not perfect for avoiding ABA problems, but double-word CAS is quite
 * environment dependant. So, we chose to implement our own logical pointer.
 * @see GcPointer
 * @see http://stackoverflow.com/questions/19389243/stealing-bits-from-a-pointer
 * @see http://www.1024cores.net/home/lock-free-algorithms/tricks/pointer-packing
 */
union gc_pointer_raw {
    // so far 4/1/1/2. Maybe 2/2/2/2 might make sense if we need more generations/segments.
    struct {
        gc_status       status;
        gc_generation   generation;
        gc_segment      segment;
        gc_offset       offset;
    } components;

    /** Integer representation, which is handy for single-word CAS. */
    uint64_t word;
};

/**
 * \brief Wrapper for gc_pointer_raw
 * \ingroup LOCKFREE
 * @tparam T Type of the pointed class/struct.
 * \details
 * \section ATOMIC Atomic Functions and Non-Atomic Functions
 * Methods whose name start with "atomic_" provide atomic semantics.
 * Other method are \e NOT atomic. So, appropriately use memory barriers to protect your
 * access. However, other methods are \e regular, meaning you will not see an utter
 * garbage but only see either the old value or new value (some valid value other thread set).
 * This is possible because this class contains only 8 bytes information.
 * In x86_64, an aligned 8 byte access is always regular though not atomic.
 *
 * When you want to call more than one non-atomic methods of GcPointer,
 * it's a good idea to \e copy a shared GcPointer variable to a local
 * GcPointer variable then access the local one.
 * For example,
 * \code{.cpp}
 * // This might be bad! (unless only used as inputs for atomic_cas)
 * bool marked = some_shared_gc_pointer.is_marked();
 *   ... many lines in-between
 * gc_aba stamp = some_shared_gc_pointer.get_aba();
 * \endcode
 * \code{.cpp}
 * // This is at least regular.
 * GcPointer<Hoge> copied(some_shared_gc_pointer);
 * bool marked = copied.is_marked();
 *   ... many lines in-between
 * gc_aba stamp = copied.get_aba();
 * \endcode
 * The copy/assignment operators of this class use the ACCESS_ONCE semantics to prohibit
 * compiler from doing something dangerous for this purpose.
 *
 * Still, CPU might do something and this is not atomic either.
 * In case it matters, use the atomic_cas() or atomic_swap().
 */
template <class T>
class GcPointer {
public:
    /** Empty NULL constructor. */
    GcPointer() { _raw.word = 0; }

    /** Constructs with gc_pointer_raw. */
    explicit GcPointer(gc_pointer_raw raw) : _raw(raw) {}

    /** Copy constructor. This is regular though might not be atomic. */
    GcPointer(const GcPointer &other) {
        operator=(other);
    }
    /** Copy assignment. This is regular though might not be atomic. */
    GcPointer& operator=(const GcPointer &other) {
        // ACCESS_ONCE semantics to make it at least regular.
        _raw.word = static_cast<const volatile gc_pointer_raw&>(other._raw).word;
        return *this;
    }
    /**
     * [Non-atomic] Equality operator on the contained pointer value.
     */
    bool operator==(const GcPointer &other) const {
        return _raw == other._raw;
    }
    /**
     * [Non-atomic] Inequality operator on the contained pointer value.
     */
    bool operator!=(const GcPointer &other) const {
        return _raw != other._raw;
    }
    /**
     * [Non-atomic] Equality operator that compares only the address
     * part without ABA counter and mark.
     */
    bool    is_equal_address(const GcPointer &other) const {
        return _raw.components.generation == other._raw.components.generation
            && _raw.components.segment == other._raw.components.segment
            && _raw.components.offset == other._raw.components.offset;
    }

    /**
     * De-references the pointer to return the actual object.
     */
    T*      dereference(GcPoolForest<T> &pool) const;

    /**
     * [Non-atomic] Marks the pointer for death, stashing TRUE into the pointer.
     * @see atomic_cas()
     */
    void        set_mark(bool on) {
        _raw.components.status = get_aba() | (on ? 0x80000000 : 0);
    }
    /** [Non-atomic] Returns if the pointer is marked in the stashed boolen flag. */
    bool        is_marked() const { return _raw.components.status < 0; }

    /** [Non-atomic] Returns the ABA counter. */
    gc_aba      get_aba() const {
        return static_cast<gc_aba>(_raw.components.status & 0x7FFFFFFF);
    }
    /** [Non-atomic] Sets the ABA counter. */
    void        set_aba(gc_aba stamp) {
        _raw.components.status = (_raw.components.status & 0x80000000)
            | static_cast<gc_status>(stamp);
    }

    /** [Non-atomic] Returns generation. */
    gc_generation   get_generation() const { return _raw.components.generation; }
    /** [Non-atomic] Returns segment. */
    gc_segment      get_segment() const { return _raw.components.segment; }
    /** [Non-atomic] Returns offset. */
    gc_offset       get_offset() const { return _raw.components.offset; }

    /** [Non-atomic] Tells if the pointer is null. */
    bool            is_null() const { return get_generation() == 0; }

    /** [Non-atomic] Returns single integer representation. */
    gc_pointer_raw&         raw() { return _raw; }
    /** [Non-atomic] Returns single integer representation. */
    gc_pointer_raw          raw() const { return _raw; }

    /**
     * \brief [Atomic] Compare and set for pointer, mark, and ABA counter altogether.
     * See Figure 9.23 of [HERLIHY]
     * @param[in] expected test value
     * @param[in] desired if succeeds this value is set
     * @return whether the CAS succeeds.
     */
    bool                atomic_cas(const GcPointer &expected, const GcPointer &desired) {
        gc_pointer_raw expected_tmp = expected._raw;
        return lintel::unsafe::atomic_compare_exchange_strong<gc_pointer_raw>(
            &_raw, &expected_tmp, desired._raw);
    }

    /**
     * \brief [Atomic] Atomic swap.
     * @param[in] new_ptr the value to set
     * @return old value before swap
     */
    GcPointer           atomic_swap(const GcPointer &new_ptr) {
        return GcPointer(lintel::unsafe::atomic_exchange<gc_pointer_raw>(&_raw, new_ptr._raw));
    }

protected:
    /** The pointer and stashed flags. */
    gc_pointer_raw _raw;
};


/**
 * \brief A segment in each generation.
 * \ingroup GCFOREST
 * \details
 * A \e segment is allocated in bulk and assigned to a single thread.
 * So, no race on allocation. The owner thread can safely allocate without atomic operations
 * nor barriers.
 * We do not reuse any objects or segments, so what we just need is a per-thread counter.
 * Deallocation might be called from any thread, so [JUNG13] uses a kind of \e counting
 * \e network.
 *
 * On the other hand, this implementation does absolutely nothing on deallocation.
 * The caller even don't have to call the deallocation method (although we do so for future
 * change). The GC generation will retire based on LSN when it is safe.
 *
 * Hence, we don't need any atomic/barrier for allocation/deallocation.
 */
template <class T>
struct GcSegment {
    GcSegment(gc_offset size) {
        ::memset(this, 0, sizeof(GcSegment));
        objects = new T[size];
        ::memset(objects, 0, sizeof(T) * size); // for easier debugging
        w_assert1(objects != NULL);
        total_objects = size;
    }
    ~GcSegment() {
        delete[] objects;
    }
    /**
     * Recycle this generation \b assuming there is no transaction that uses any of the
     * objects in this segment. If the assumption does not hold, this might cause ABA
     * and other corruption.
     */
    void recycle() {
        owner = 0;
        allocated_objects = 0;
        ::memset(objects, 0, sizeof(T) * total_objects);
    }
    /** ID of the thread that can exclusively own this segment. */
    gc_thread_id    owner;
    /** size of objects array. */
    gc_offset       total_objects;
    /** How many objects we allocated. Stop allocation when this reaches total_objects. */
    gc_offset       allocated_objects;
    /** The bulk-allocated objects. T[total_objects]. */
    T*              objects;
};

/**
 * \brief A generation of objects.
 * \ingroup GCFOREST
 * \details
 * A \e generation groups objects in similar age and controls
 * when we can start deallocating segments in it.
 */
template <class T>
struct GcGeneration {
    GcGeneration(uint32_t generation_nowrap_arg) {
        ::memset(this, 0, sizeof(GcGeneration));
        generation_nowrap = generation_nowrap_arg;
    }
    ~GcGeneration() {
        DBGOUT1(<<"Destroying a GC Generation " << generation_nowrap
            << ". total_segments=" << total_segments
            << ", allocated_segments=" << allocated_segments);
        for (gc_segment i = 0; i < total_segments; ++i) {
            delete segments[i];
        }
    }

    /** Returns the number of segments up for grab. */
    uint32_t        get_free_count() const { return total_segments - allocated_segments; }

    /**
     * Pre-allocate a few segments and make them up for grab.
     * @param[in] segment_count How many segments we should reserve now
     * @param[in] segment_size How many objects we should allocate per segment
     * @return whether we preallocated the segments. false if the generation is already full.
     */
    bool            preallocate_segments(size_t segment_count, gc_offset segment_size);

    /**
     * Recycle this generation \b assuming there is no transaction that uses any of the
     * segments in this generation. If the assumption does not hold, this might cause ABA
     * and other corruption.
     */
    void            recycle(uint32_t generation_nowrap_arg) {
        retire_suggested = false;
        generation_nowrap = generation_nowrap_arg;
        for (uint32_t seg = 0; seg < total_segments; ++seg) {
            if (segments[seg]->owner != 0) {
                segments[seg]->recycle();
            }
        }
        allocated_segments = 0;
    }

    /**
     * A loose request for threads to start retiring this generation.
     * Threads that saw this stop making a new segment in this generation.
     * It's loose. Threads might not take barriers to check this.
     */
    bool            retire_suggested;

    /**
     * Number of total segments. We don't reuse segments, so this monotonically increases.
     * We use atomic operation to increase this.
     * 32bits for CAS.
     */
    uint32_t        total_segments;

    /**
     * Number of allocated segments owned by some thread.
     * If allocated_segments < total_segments, there are some segments other threads
     * can try occupying.
     * We don't allow "holes" in segment allocation, so this monotonically increases.
     * We use atomic operation to increase this (=occupy the segment).
     * 32bits for CAS.
     */
    uint32_t        allocated_segments;

    /** ID of this generation. */
    uint32_t        generation_nowrap;

    /** Active segments in this generation. The first total_segments elements are non-NULL. */
    GcSegment<T>*   segments[GC_MAX_SEGMENTS];
};

/**
 * Derive from this or define "gc_pointer_raw gc_pointer" in your class to put into the pool.
 * \ingroup GCFOREST
 */
struct GcPoolEntry {
    gc_pointer_raw gc_pointer;
};

/**
 * Callback interface used when pre-allocation or retiring gets behind.
 * \ingroup GCFOREST
 */
struct GcWakeupFunctor {
    virtual ~GcWakeupFunctor() {}
    virtual void wakeup() = 0;
};

/**
 * \brief Garbage-collected Pool Forest
 * \ingroup GCFOREST
 * @tparam T Type of the managed class/struct. It must have a member
 * "gc_pointer_raw gc_pointer" accessible to this class.
 * \details
 * This is a Roll-Your-Own Garbage Collector implementation optimized for long-running services
 * such as filesystems and databases.
 */
template <class T>
struct GcPoolForest {
    GcPoolForest(const char* debug_name, uint32_t desired_gens,
        size_t initial_segment_count, gc_offset initial_segment_size) {
        ::memset(this, 0, sizeof(GcPoolForest));
        desired_generations = desired_gens;
        name = debug_name;
        // generation=0 is an invalid generation, so we start from 1.
        head_nowrap = 1;
        curr_nowrap = 1;
        // We always have at least one active generation
        epochs[1].set(0);
        generations[1] = new GcGeneration<T>(1);
        tail_nowrap = 2;
        if (initial_segment_count > 0) {
            generations[1]->preallocate_segments(initial_segment_count, initial_segment_size);
        }
        gc_wakeup_functor = NULL;
    }
    ~GcPoolForest() {
        DBGOUT1(<< name << ": Destroying a GC Pool Forest. head_nowrap=" << head_nowrap
            << ", tail_nowrap=" << tail_nowrap);
        for (size_t i = head_nowrap; i < tail_nowrap; ++i) {
            delete generations[wrap(i)];
        }
    }
    /** Returns a physical pointer represented by the logical pointer. */
    T*  resolve_pointer(const GcPointer<T> &pointer) { return pointer.raw(); }
    /** Returns a physical pointer represented by the logical pointer. */
    T*  resolve_pointer(gc_pointer_raw pointer);

    GcGeneration<T>*    resolve_generation(gc_pointer_raw pointer) {
        return resolve_generation(pointer.components.generation);
    }
    GcGeneration<T>*    resolve_generation(gc_generation gen) {
        return generations[gen];
    }
    GcSegment<T>*       resolve_segment(gc_pointer_raw pointer) {
        return resolve_segment(pointer.components.generation, pointer.components.segment);
    }
    GcSegment<T>*       resolve_segment(gc_generation gen, gc_segment seg) {
        w_assert1(is_valid_generation(gen));
        GcGeneration<T>* generation = resolve_generation(gen);
        w_assert1(seg < generation->total_segments);
        return generation->segments[seg];
    }

    /**
     * Allocate a new object from this pool.
     * @param[in,out] next this is a thread-local hint to allocate from. we also increment
     * or even switch to new segment. the caller probably uses a thread-local for this.
     * @param[in] self Some ID of the calling thread. eg, pthread_self().
     */
    T*                  allocate(gc_pointer_raw &next, gc_thread_id self);
    /**
     * Return an object to this pool.
     * @param[in] pointer The object to return.
     */
    void                deallocate(T* pointer) { deallocate(pointer->gc_pointer); }
    /**
     * Return an object to this pool.
     * @param[in] pointer Logical pointer to the object to return.
     */
    void                deallocate(gc_pointer_raw pointer);
    /** Occupy a segment in latest generation and return a pointer to its first object. */
    gc_pointer_raw      occupy_segment(gc_thread_id self);

    /** The oldest active generation. wrapped. */
    gc_generation       head() const { return wrap(head_nowrap); }
    /** The exclusive end of active generations. wrapped. */
    gc_generation       tail() const { return wrap(tail_nowrap); }
    /** The yougnest active generation in which we should allocate new segments. wrapped. */
    gc_generation       curr() const { return wrap(curr_nowrap); }
    /** Returns the number of active generations. */
    size_t              active_generations() const { return tail_nowrap - head_nowrap; }
    GcGeneration<T>*    head_generation() { return generations[head()]; }
    GcGeneration<T>*    curr_generation() { return generations[curr()]; }
    bool                is_valid_generation(gc_generation generation) const {
        return generation != 0 && generation >= head() && generation < tail();
    }

    /**
     * Create a new generation for the given starting LSN.
     * @param[in] low_water_mark LSN of the oldest transaction in the system. Used to recycle
     * old generations. lsn_t::null prohibits recycling.
     * @param[in] now higest LSN as of now, which means every thread that newly allocates
     * from this new generation would have this LSN or higher.
     * @return whether we could advance the generation or there is already a generation
     * for the starting LSN (or more recent). false means too many generations.
     */
    bool                advance_generation(lsn_t low_water_mark, lsn_t now,
                size_t initial_segment_count, gc_offset segment_size);

    /**
     * Retire old generations that are no longer needed.
     * @param[in] low_water_mark LSN of the oldest transaction in the system
     * @param[in] recycle_now higest LSN as of now. This is used to recycle retired generations.
     * lsn_t::null means prohibiting recycle.
     */
    void                retire_generations(lsn_t low_water_mark, lsn_t recycle_now = lsn_t::null);

    /** full load-store fence. */
    void                mfence() const {
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
    }

    static gc_generation wrap(size_t i) { return static_cast<gc_generation>(i); }

    /** Only used to put the name of this forest in debug print. */
    const char*         name;
    /** The oldest active generation. No wrap, so take modulo before use. */
    uint32_t            head_nowrap;
    /** The latest active generation. No wrap, so take modulo before use. */
    uint32_t            curr_nowrap;
    /** The exclusive end of active generation. No wrap, so take modulo before use. */
    uint32_t            tail_nowrap;
    /** Desired number of generations. Don't add more than it unless can't retire old ones. */
    uint32_t            desired_generations;
    /** Active (or being-retired) generation objects. */
    GcGeneration<T>*    generations[GC_MAX_GENERATIONS];
    /** LSN as of starting each generation. */
    lsn_t               epochs[GC_MAX_GENERATIONS];

    /** Function pointer to wakeup GC when pre-allocation or retiring gets behind. */
    GcWakeupFunctor*    gc_wakeup_functor;
};

template <class T>
inline T* GcPoolForest<T>::resolve_pointer(gc_pointer_raw pointer) {
    gc_generation generation = pointer.components.generation;
    if (generation == 0) {
        return NULL;
    }

    lintel::atomic_thread_fence(lintel::memory_order_consume);
    w_assert1(is_valid_generation(generation));
    GcGeneration<T>* gen = generations[generation];
    w_assert1(gen != NULL);

    w_assert1(pointer.components.segment < gen->allocated_segments);
    GcSegment<T>* segment = gen->segments[pointer.components.segment];
    w_assert1(segment != NULL);

    gc_offset offset = pointer.components.offset;
    w_assert1(offset < segment->total_objects);
    w_assert1(offset < segment->allocated_objects);

    return segment->objects + offset;
}

template <class T>
inline T* GcPoolForest<T>::allocate(gc_pointer_raw &next, gc_thread_id self) {
    if (!is_valid_generation(next.components.generation)
        || generations[next.components.generation]->retire_suggested
        || next.components.generation != curr()) {
        next = occupy_segment(self);
    }

    w_assert1(is_valid_generation(next.components.generation));
    GcGeneration<T>* gen = generations[next.components.generation];
    w_assert1(gen != NULL);

    if (next.components.segment >= gen->allocated_segments
        || gen->segments[next.components.segment]->owner != self
        || gen->segments[next.components.segment]->allocated_objects
            >= gen->segments[next.components.segment]->total_objects) {
        next = occupy_segment(self);
        gen = generations[next.components.generation];
    }
    w_assert1(next.components.segment < gen->allocated_segments);
    GcSegment<T>* segment = gen->segments[next.components.segment];
    w_assert1(segment != NULL);
    w_assert1(segment->owner == self);
    w_assert1(segment->allocated_objects < segment->total_objects);

    next.components.offset = segment->allocated_objects;
    T* ret = segment->objects + next.components.offset;
    ret->gc_pointer = next;

    ++segment->allocated_objects;
    next.components.offset = segment->allocated_objects;
    return ret;
}

template <class T>
inline void GcPoolForest<T>::deallocate(gc_pointer_raw pointer) {
    // We don't do anything in deallocate.
    w_assert1(is_valid_generation(pointer.components.generation));
    GcGeneration<T>* gen = generations[pointer.components.generation];
    w_assert1(gen != NULL);

    w_assert1(pointer.components.segment < gen->allocated_segments);
    GcSegment<T>* segment = gen->segments[pointer.components.segment];
    w_assert1(segment != NULL);

    // we don't check double-free or anything
    w_assert1(pointer.components.offset < segment->allocated_objects);
}

template <class T>
inline gc_pointer_raw GcPoolForest<T>::occupy_segment(gc_thread_id self) {
    // this method is relatively infrequently called, so we can afford barriers and atomics.
    while (true) {
        mfence();
        gc_generation generation = curr();
        GcGeneration<T>* gen = generations[generation];
        if (gen->allocated_segments >= gen->total_segments) {
            // allocator threads are not catching up. This must be rare. let's sleep.
            DBGOUT0(<< name << ": GC Thread is not catching up. have to sleep. me=" << self);
            if (gc_wakeup_functor != NULL) {
                gc_wakeup_functor->wakeup();
            }
            const uint32_t SLEEP_MICROSEC = 10000;
            ::usleep(SLEEP_MICROSEC);
            continue;
        }
        uint32_t alloc_segment = gen->allocated_segments;
        mfence();
        if (lintel::unsafe::atomic_compare_exchange_strong<uint32_t>(
            &gen->allocated_segments, &alloc_segment, alloc_segment + 1)) {
            // Okay, we are surely the only winner of the pre-allocated segment.
            GcSegment<T>* segment = gen->segments[alloc_segment];
            while (segment == NULL) {
                // possible right after CAS in preallocate_segments. wait.
                mfence();
                DBGOUT3(<< name << ": Waiting for segment " << alloc_segment << " in generation "
                    << gen->generation_nowrap << ". me=" << self);
                segment = gen->segments[alloc_segment];
            }
            w_assert1(segment->owner == 0);
            w_assert1(segment->allocated_objects == 0);
            w_assert1(segment->total_objects > 0);

            // Occupied!
            DBGOUT1(<< name << ": Occupied a pre-allocated Segment " << alloc_segment
                << " in generation " << gen->generation_nowrap << ". me=" << self << ".");
            segment->owner = self;
            mfence(); // let the world know (though the CAS above should be enough..)
            gc_pointer_raw ret;
            ret.components.status = 0;
            ret.components.generation = generation;
            ret.components.segment = alloc_segment;
            ret.components.offset = 0;
            return ret;
        } else {
            DBGOUT1("Oops, CAS failed");
            continue;
        }
    }
}


template <class T>
inline bool GcPoolForest<T>::advance_generation(lsn_t low_water_mark, lsn_t now,
    size_t segment_count, gc_offset segment_size) {
    while (true) {
        mfence();
        if (low_water_mark != lsn_t::null && active_generations() >= desired_generations) {
            // try recycling oldest generation
            uint32_t old_tail_nowrap = tail_nowrap;
            retire_generations(low_water_mark, now);
            if (old_tail_nowrap < tail_nowrap) {
                return true; // it worked!
            }
        }
        if (tail_nowrap - head_nowrap >= GC_MAX_GENERATIONS - 1) {
            ERROUT(<< name << ": Too many generations!");
            return false; // too many generations
        }
        uint32_t new_generation_nowrap = tail_nowrap;
        uint32_t new_tail = new_generation_nowrap + 1;
        if (wrap(new_tail) == 0) {
            DBGOUT1(<< name << ": Generation wrapped!");
            ++new_tail; // skip generation==0
        }
        if (lintel::unsafe::atomic_compare_exchange_strong<uint32_t>(
            &tail_nowrap, &new_generation_nowrap, new_tail)) {
            // okay, let's create the generation
            gc_generation new_generation = wrap(new_generation_nowrap);
            generations[new_generation] = new GcGeneration<T>(new_generation_nowrap);
            epochs[new_generation] = now;
            generations[new_generation]->preallocate_segments(segment_count, segment_size);
            DBGOUT1(<< name << ": Generation " << new_generation_nowrap
                << " created. epoch=" << now.data());
            curr_nowrap = new_generation_nowrap;
            mfence();
            return true;
        }
        // else retry
    }
}

template <class T>
inline bool GcGeneration<T>::preallocate_segments(size_t segment_count, gc_offset segment_size) {
    // this method is infrequently called by background thread,
    // so barriers/atomics and big allocations are fine.
    while (segment_count > 0) {
        w_assert1(!retire_suggested);
        if (total_segments >= GC_MAX_SEGMENTS - 1) {
            return false; // already full!
        }

        uint32_t new_segment = total_segments;
        if (lintel::unsafe::atomic_compare_exchange_strong<uint32_t>(
            &total_segments, &new_segment, new_segment + 1)) {
            // okay, we exclusively own this segment index
            GcSegment<T> *seg = new GcSegment<T>(segment_size);
            segments[new_segment] = seg;
            --segment_count;
            DBGOUT1(<<"Pre-allocated Segment " << new_segment << " in generation "
                << generation_nowrap << ". segment size=" << segment_size << ".");
        }
        // else, someone else has just changed. retry
    }
    return true;
}

template <class T>
inline void GcPoolForest<T>::retire_generations(lsn_t low_water_mark, lsn_t recycle_now) {
    // this method is infrequently called by background thread,
    // so barriers/atomics and big deallocations are fine.
    w_assert1(tail_nowrap > 0);
    while (true) {
        mfence();
        uint32_t oldest_nowrap = head_nowrap;
        uint32_t next_oldest_nowrap = oldest_nowrap + 1;
        if (wrap(next_oldest_nowrap) == 0) {
            ++next_oldest_nowrap; // skip generation==0
        }
        const uint32_t MIN_HEALTHY_GENERATIONS = 2;
        if (tail_nowrap <= next_oldest_nowrap + MIN_HEALTHY_GENERATIONS) {
            return;
        }

        GcGeneration<T>* oldest = generations[wrap(oldest_nowrap)];
        oldest->retire_suggested = true; // this softly requests threads to move onto new gen
        mfence();
        if (low_water_mark >= epochs[wrap(next_oldest_nowrap)]) {
            // if even the next generation's beginning LSN is older than low water mark,
            // there is no chance that the oldest generation has anything used.
            // Let _me_ retire that.
            if (lintel::unsafe::atomic_compare_exchange_strong<uint32_t>(
                &head_nowrap, &oldest_nowrap, next_oldest_nowrap)) {
                // okay, I'm exclusively retiring this generation.
                generations[wrap(oldest_nowrap)] = NULL;
                epochs[wrap(oldest_nowrap)].set(0);

                DBGOUT1(<< name << ": Successfully retired generation " << oldest_nowrap);
                if (recycle_now != lsn_t::null && active_generations() <= desired_generations) {
                    DBGOUT1(<< "Now recycling it as new generation ...");
                    mfence();
                    uint32_t new_generation_nowrap = tail_nowrap;
                    uint32_t new_tail = new_generation_nowrap + 1;
                    if (wrap(new_tail) == 0) {
                        DBGOUT1(<< name << ": Generation wrapped!");
                        ++new_tail; // skip generation==0
                    }

                    if (lintel::unsafe::atomic_compare_exchange_strong<uint32_t>(
                        &tail_nowrap, &new_generation_nowrap, new_tail)) {
                        oldest->recycle(new_generation_nowrap);
                        generations[wrap(new_generation_nowrap)] = oldest;
                        epochs[wrap(new_generation_nowrap)].set(recycle_now.data());
                        curr_nowrap = new_generation_nowrap;
                        DBGOUT1(<< name << ": Successfully recycled as gen " << new_generation_nowrap);
                        mfence();
                        return;
                    } else {
                        DBGOUT1(<< name << ": Oops, others incremented generation. couldn't "
                            << " reuse the retired generation");
                        delete oldest; // well, no other way.
                    }
                } else {
                    delete oldest;
                }
            } else {
                // someone else has retired it.
                DBGOUT1(<< name << ": Oops, CAS failed, someone has retired it?");
                continue;
            }
        } else {
            // then, we can't safely retire this generation. yet.
            return;
        }
    }
}

template <class T>
inline T* GcPointer<T>::dereference(GcPoolForest<T> &pool) const {
    T* pointer = pool.resolve_pointer(_raw);
    w_assert1(pointer != NULL);
    return pointer;
}

#endif // W_GC_POOL_FOREST_H
