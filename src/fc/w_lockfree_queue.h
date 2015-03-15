/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#ifndef W_LOCKFREE_QUEUE_H
#define W_LOCKFREE_QUEUE_H

#include <cassert>
#include <ostream>
#include <vector>
#include <set>
#include <AtomicCounter.hpp>
#include "w_markable_pointer.h"

/**
 * \brief A lock-free unbounded FIFO queue (\e pool) described in [HERLIHY] Chap 10.5,
 *  commonly known as \b Michael-Scott-Algorithm [MICH96].
 * \ingroup LOCKFREE
 * @tparam CHAIN class of each entry in the queue. should derive from MarkablePointerChain.
 * \details
 * FIFO queue is commonly used as an object pool, such as OS's memory allocator.
 * To make allocation/deallocation from many-threads efficient, this queue provides
 * enque/deque operations as lock-free methods for multi-producers and multi-consumers.
 *
 * \section GUARANTEES Guarantees of this Queue
 * This queue implementation is:
 *  \li \e Linenarlizable, although we might later relax it in dequeue() for performance.
 *  \li \e First-In-First-Out, meaning the entry that has been just enqueued will be dequeued
 * much later (as far as the queue has many entries), which is ideal to avoid contentions.
 *  \li \e Unbounded, meaning the maximum capacity is unlimited.
 *  \li Appropriate for concurrent \e multi-producers and \e multi-consumers.
 *  \li enqueue() is \e total, meaning it never waits for a certain condition because the
 * capacity is unlimited.
 *  \li dequeue() is also \e total, meaning it immediately returns with NULL if the queue
 * has nothing to return. This is approproate for the use as an object pool. Due to lack of
 * needs, so far there is no \e partial version of dequeue(), meaning waits for enqueue().
 *  \li \b NOT safe for ABA if the capacity is running low. Make sure the capacity is
 * large enough at all time by checking approximate_size().
 * See the later section for details.
 *
 * \section EXAMPLE Example
 * For example, the following code shows the use of this lock-free queue.
 * Notice that the entry class derives from MarkablePointerChain.
 * \code{.cpp}
 * struct DummyEntry : public MarkablePointerChain<DummyEntry> {
 *     ...
 * };
 *
 * LockFreeQueue the_queue;
 * the_queue.enqueue(new DummyEntry());
 * the_queue.enqueue(new DummyEntry());
 * DummyEntry* item = the_queue.dequeue();
 * ..
 * \endcode
 * \section UNSAFE Unsafe Methods
 * Methods whose name starts with "unsafe_" are not thread-safe. They are provided for
 * easier use in debugging and single-threaded situation.
 *
 * \section ABA ABA Issue
 * See the section in LockFreeList. The same issue, the same 16-bit ABA counter.
 * However, the issue happens much less often because this is a FIFO pool and
 * allocations/deallocations of pooled objects will/should be batched and asynchronous.
 * The chance of same objects leaving/entering this queue in a short period is relatively low.
 * Thus, the 16-bit ABA counter should be enough as far as we maintain a healthy capacity
 * of the queue.
 *
 * Therefore, we didn't bother implementing Hazard Pointers [MICH04] or other advanced Safe
 * Memory Reclamation (SMR) in these classes. We might revisit this later, but we might want to
 * just use third patry library like libcds or boost::lockfree if we need them (which has other
 * drawbacks, though).
 *
 * \section WAITFREE Wait Free Queue
 * There is a newer work that makes every operation wait-free [KOGAN11], but this class
 * uses the traditional [MICH96]. The reason is that, although [KOGAN11] is wait-free,
 * it is not as fast as lock-free version [MICH96]. It is faster in a certain configuration,
 * but not enough to justify its complexity and even significantly slower in some
 * settings (see [KOGAN11] to verify yourself).
 * Analogous to "O(1) might be slower than O(N)" sort of issue.
 * Having said that, we might revisit this later.
 *
 * \section DEP Dependency
 * This class is completely header-only. To use, just include w_lockfree_queue.h.
 */
template <class CHAIN>
class LockFreeQueue {
public:
    LockFreeQueue() {
        // both head and tail initially refer to sentinel node
        _sentinel = CHAIN();
        _head = MarkablePointer<CHAIN>(&_sentinel, false, 0);
        // _tail could also start with stamp==0 and it shouldn't be an issue, but let's
        // make the initial stamp different from _head for easier debugging.
        _tail = MarkablePointer<CHAIN>(&_sentinel, false, 0x8000);
        _approximate_size = 0;
    }

    /**
     * \brief A lock-free enqueue method to add an entry in the pool.
     * @param[in] value the entry to add to this pool.
     * @pre value != NULL
     * \details
     * enqueue() does its job \e lazily in two steps.
     *  \li The first step atomically adds the new node next to _tail.
     *  \li The second step atomically addvances the _tail to the new node.
     * The key point is that the second step can fail. Even if that happens, other threads
     * eventually do it on behalf. So, this thread immediately returns.
     */
    void  enqueue(CHAIN* value) {
        assert(value != NULL);
        while(true) {
            MarkablePointer<CHAIN>   last = _tail;
            assert(!last.is_null());
            MarkablePointer<CHAIN>   next = last->next;
            mfence();
            if (last == _tail) {
                if (next.is_null()) {
                    // okay, it looks like we can insert next to tail.
                    MarkablePointer<CHAIN> node(value, false, next.get_aba_stamp() + 1);
                    // NOTE: Q: Why this "last->next" is safe? SEGFAULT possible?
                    // A: we observed last->next BEFORE mfence and last is still _tail.
                    // There is no case we dequeue entries AFTER _tail.
                    // Further, "last == _tail" is protected by ABA counter, too. Thus, safe.
                    // Other code below are also very careful when de-referencing a pointer.
                    // (well, except the method prefixed with "unsafe_")
                    if (last->next.atomic_cas(next, node)) {
                        // yes, we put a new entry next to tail. let's modify tail too.
                        // even if this CAS fails, fine because other threads will do that.
                        ++_approximate_size;
                        tail_cas(last, node);
                        return; // done. Linearlization Point is the 1st CAS, not 2nd CAS.
                    }
                    // else, CAS failed, so someone has just inserted next to tail. retry
                }
            } else {
                // some one has just inserted next to tail but not yet switched the tail.
                // let's help him. whether it succeeds or not, we retry.
                if (!next.is_null()) {
                    next.set_aba_stamp(last.get_aba_stamp() + 1);
                    tail_cas(last, next);
                }
            }
        }
    }

    /**
     * \brief A lock-free dequeue method to get an entry in the pool.
     * @return an entry. NULL if the pool is empty.
     * \details
     * dequeue() also helps enqueue() to finish the CAS, and it \e must do so because
     * otherwise it might drop an enqueued entry by advancing head. See [HERLIHY] Chap 10.5.
     */
    CHAIN*  dequeue() {
        while (true) {
            MarkablePointer<CHAIN> first = _head;
            MarkablePointer<CHAIN> last  = _tail;
            assert(!first.is_null());
            assert(!last.is_null());
            MarkablePointer<CHAIN> next  = first->next;
            mfence();
            if (first == _head) {
                // okay, head is unchanged (including ABA stamp) even after barrier
                if (first.get_pointer() == last.get_pointer()) {
                    // head and tail are same, probably empty. but let's make sure.
                    // (if we are okay to relax Linearlizability, we can return NULL right now)
                    if (next.is_null()) {
                        // we don't have anything after the sentinel node, meaning empty.
                        return NULL; // done. Linearlization Point is mfence.
                    } else {
                        // someone is yet to set _tail. let's help it. anyway retry myself.
                        if (_tail == last) { // pre-check for ABA stamp. just an optimization
                            next.set_aba_stamp(last.get_aba_stamp() + 1);
                            tail_cas(last, next);
                        }
                    }
                } else {
                    // head and tail are different, so most likely there is something to pop.
                    CHAIN *value = next.get_pointer();
                    next.set_aba_stamp(first.get_aba_stamp() + 1);
                    if (head_cas(first, next)) {
                        assert(value != &_sentinel);
                        assert(value != NULL);
                        assert(!_head.is_null());
                        --_approximate_size;
                        return value; // done. Linearlization Point is the CAS.
                    }
                }
            }
            // else, someone has just dequeued and advanced head. retry.
        }
    }

    /**
     * \brief Returns the number of entries safely, but very slowly (not lock-free at all).
     * \details
     * This method scans the queue to give a count that is much more accurate than
     * approximate_size() (though still not accurate in the sense of linearlizablity).
     * However, this method takes VERY long time when the queue has many entries because it has
     * to scan the entire queue, take memory barrier before each pointer de-referencing
     * to avoid crash due to concurrent dequeue, and possibly restart for many times.
     * Do NOT call this frequently.
     * @see unsafe_size()
     * @see approximate_size()
     */
    size_t        safe_size() {
        size_t size = 0;
        // whenever _head changes, we must restart to be safe.
        MarkablePointer<CHAIN> first = _head;
        assert(!first.is_null());
        MarkablePointer<CHAIN> second = _head->next; // slight optimization
        MarkablePointer<CHAIN> current = _head->next;
        while (!current.is_null()) {
            mfence();
            if (first != _head) {
                // argh, to be safe, we now have to restart. but...
                if (_head == second) {
                    // lucky, the head is not moving that fast to pass us.
                    if (size > 0) {
                        --size;
                    }
                } else {
                    // no, we have to start from scratch
                    size = 0;
                    current = _head->next;
                }
            } else {
                // then, we know current is still a valid pointer
                current = current->next;
                ++size;
            }
            first = _head;
            second = _head->next;
        }
        return size;
    }

    /**
     * \brief [NOT thread-safe] Returns the number of entries.
     * \details
     * This method is NOT thread-safe nor accurate.
     * Do not rely on this other than a debug/safe situation.
     * There is no really accurate size() semantics in lock-free queue because it
     * inherently needs to block others.
     * @see safe_size()
     * @see approximate_size()
     */
    size_t  unsafe_size() const {
        mfence(); // doesn't solve all issues, but better than nothing
        size_t ret = 0;
        for (MarkablePointer<CHAIN> e = _head->next; !e.is_null(); e = e->next) {
            ++ret;
        }
        return ret;
    }

    /**
     * \brief [NOT thread-safe] Removes all entries.
     * \details
     * This method is NOT thread-safe. Also, you have to garbarge-collect entries in the queue
     * your self, too. Call this only in a safe situation.
     */
    void    unsafe_clear() {
        _sentinel = CHAIN();
        _head = MarkablePointer<CHAIN>(&_sentinel, false, 0);
        _tail = MarkablePointer<CHAIN>(&_sentinel, false, 0x8000);
        _approximate_size = 0;
        mfence();
    }


    /**
     * \brief [NOT thread-safe] Dumps out all existing entries to the given stream.
     * \details
     * This method is NOT thread-safe. Call this only in a safe/debug situation.
     */
    void    unsafe_dump(std::ostream &out) const {
        mfence(); // doesn't solve all issues, but better than nothing
        out << "LockFreeQueue (size=" << unsafe_size() << "):" << std::endl;
        for (MarkablePointer<CHAIN> e = _head->next; !e.is_null(); e = e->next) {
            out << "  e=0x" << std::hex << e.as_int() << std::endl;
        }
    }

    /**
     * \brief [NOT thread-safe] Checks if we can correctly track from _head to _tail.
     * \details
     * This method is NOT thread-safe. Call this only in a safe/debug situation.
     */
    bool    unsafe_consistent() const {
        mfence(); // doesn't solve all issues, but better than nothing
        assert(!_head.is_null() && !_tail.is_null());
        if (_head.is_null() || _tail.is_null()) {
            return false;
        }

        if (_head.get_pointer() == _tail.get_pointer()) {
            assert(_head.get_pointer() == &_sentinel);
            return (_head.get_pointer() == &_sentinel);
        }

        // can we reach _tail from _head?
        bool reached_tail = false;
        for (MarkablePointer<CHAIN> e = _head->next; !e.is_null(); e = e->next) {
            if (e == _tail) {
                reached_tail = true;
                break;
            } else if (e.get_pointer() == &_sentinel) {
                assert(false);
                return false;
            }
        }
        assert(reached_tail);
        if (!reached_tail) {
            return false;
        }

        // can we NOT reach _head from _tail?
        bool reached_head = false;
        for (MarkablePointer<CHAIN> e = _tail->next; !e.is_null(); e = e->next) {
            if (e == _head) {
                reached_head = true;
                break;
            }
        }
        assert(!reached_head);
        if (reached_head) {
            return false;
        }

        // duplicates?
        std::set<CHAIN*> observed;
        for (MarkablePointer<CHAIN> e = _head->next; !e.is_null(); e = e->next) {
            CHAIN* value = e.get_pointer();
            if (observed.find(value) != observed.end()) {
                assert(false);
                return false;
            }
            observed.insert(value);
        }

        return true;
    }

    /**
     * \brief [NOT thread-safe] Returns the contents of this queue as std::vector.
     * \details
     * This method is NOT thread-safe. Call this only in a safe/debug situation.
     */
    void   unsafe_as_vector(std::vector<CHAIN*> &out) const {
        mfence(); // doesn't solve all issues, but better than nothing
        out.clear();
        for (MarkablePointer<CHAIN> e = _head->next; !e.is_null(); e = e->next) {
            out.push_back(e.get_pointer());
        }
    }

    /**
     * Returns the approximate count of entries. The number is just an approximate and
     * can be even a negative number. Instead, this method is safe and very fast.
     * @see safe_size()
     * @see unsafe_size()
     * @see refresh_approximate_size()
     */
    int64_t     approximate_size() const {
        return _approximate_size;
    }

    /**
     * Refreshes the approximate count by safe_size(), which is SO slow.
     * Use this infrequently.
     * @see safe_size()
     * @see unsafe_size()
     * @see approximate_size()
     */
    void        refresh_approximate_size() {
        _approximate_size = static_cast<int64_t>(safe_size());
    }

protected:
    /**
     * full load-store fence.
     */
    void    mfence() const {
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
    }

    /**
     * Atomic CAS on _tail.
     */
    bool    tail_cas(const MarkablePointer<CHAIN>& expected,
                     const MarkablePointer<CHAIN>& desired) {
        return _tail.atomic_cas(expected, desired);
    }

    /**
     * Atomic CAS on _head.
     */
    bool    head_cas(const MarkablePointer<CHAIN>& expected,
                     const MarkablePointer<CHAIN>& desired) {
        return _head.atomic_cas(expected, desired);
    }

    /** The \e sentinel node which always exists and is not a valid entry itself. */
    CHAIN                    _sentinel;

    /**
     * The queue head from which we dequeue. Initially points to sentinel.
     */
    MarkablePointer<CHAIN>   _head;

    /**
     * The queue tail to which we enqueue. Initially points to sentinel.
     */
    MarkablePointer<CHAIN>   _tail;

    /**
     * Approximate size which is not protected by any means.
     * Uses to tell if this pool is under low water mark, needing more enqueues.
     */
    int64_t     _approximate_size;
};

#endif // W_LOCKFREE_QUEUE_H
