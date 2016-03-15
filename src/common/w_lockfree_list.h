/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#ifndef W_LOCKFREE_LIST_H
#define W_LOCKFREE_LIST_H

#include <cassert>
#include <ostream>
#include <set>
#include "w_markable_pointer.h"
#include "w_gc_pool_forest.h"

/**
 * \brief A lock-free singly-linked list described in [HERLIHY] Chap 9.8,
 *  commonly known as \b Harris-Michael-Algorithm [MICH02].
 * \ingroup LOCKFREE
 * @tparam T class of each entry in the list.
 * @tparam KEY type of key to identify entry. Must be a comparable type.
 * @tparam POOL class that can instantiate T class object.
 * \details
 * The entry class (T) must define key and next member.
 *  \li key must hold KEY type.
 *  \li next must hold GcPointer<T>, next entry in the linked list.
 * GcPointer has a bit to mark \b this (not next) for death.
 *
 * \section EXAMPLE Example
 * For example, the following code shows the use of this lock-free linked list with
 * a dummy pool object. A real pool object would do smarter alloc/dealloc.
 * \code{.cpp}
 * struct DummyEntry : public GcPoolEntry {
 *    DummyEntry() : key(0) {}
 *    GcPointer<DummyEntry>   next;
 *    uint32_t key;
 * };
 *
 * struct DummyPool {
 *    DummyEntry* allocate_instance(uint32_t key) {
 *        ... // constructs DummyEntry from object pool
 *    }
 *    void        deallocate_instance(DummyEntry* entry) {
 *        ... // returns DummyEntry to object pool
 *    }
 *    ... // object pooling for DummyEntry
 * };
 *
 * GcPoolForest<DummyEntry> pool(10, 1000);
 * LockFreeList<DummyEntry, uint32_t> the_list(&pool);
 * DummyEntry* item4 = the_list.get_or_add(4);
 * DummyEntry* item3 = the_list.get_or_add(3);
 * ..
 * \endcode
 * \section UNSAFE Unsafe Methods
 * Methods whose name starts with "unsafe_" are not thread-safe. They are provided for
 * easier use in debugging and single-threaded situation.
 *
 * \section ABAPROBLEM ABA Issue
 * This implementation uses GcPointer to atomically modify an entry, which has
 * one bit to mark for death and also 31 bits ABA counter.
 * 31 bits should be plenty to avoid ABA problem.
 * For more details, see GcPointer and gc_pointer_raw.
 *
 * \section DEP Dependency
 * This class is completely header-only. To use, just include w_lockfree_list.h.
 */
template <class T, typename KEY>
class LockFreeList {
public:
    /** Predecessor/current tuple. */
    struct Window {
        T*                  predecessor;
        GcPointer<T>        current;
        KEY                 current_key;
    };

    /** Instantiated with the pool to allocate/deallocate entries. */
    LockFreeList(GcPoolForest<T> *pool) : _pool(pool) {}
    ~LockFreeList() { unsafe_clear(); }

    /**
     * \brief Returns the entry with the key, creating if not exists.
     * @param[in] key the key to look for
     * \details
     * This is a bit different from the algorithm in textbook because we need to control
     * how entries are instantiated. In short, this method combines find() and add().
     * @return the found entry. Never NULL because we create then.
     */
    T*  get_or_add(KEY key, gc_pointer_raw &tls_pool_next, gc_thread_id self) {
        while (true) {
            Window window = find(key);
            if (!window.current.is_null() && window.current_key == key) {
                return window.current.dereference(*_pool); // found
            } else {
                // not found. need to create between predecessor and current.
                T* v = _pool->allocate(tls_pool_next, self);
                v->key = key;
                v->next = window.current;

                // insert the created entry by CAS.
                // For correctness, we add a new node only if predecessor is
                // still unmarked and points to current.
                GcPointer<T> new_pointer(v->gc_pointer);
                new_pointer.set_aba(window.predecessor->next.get_aba() + 1);
                if (window.predecessor->next.atomic_cas(window.current, new_pointer)) {
                    // succeeded
                    return v;
                } else {
                    // CAS failed, so we need to retry.
                    _pool->deallocate(v);
                }
            }
        }
    }

    /**
     * \brief Returns the entry with the key.
     * @param[in] key the key to look for
     * @return the found entry. NULL if not found.
     */
    T*  get(KEY key) const {
        Window window = find(key);
        if (window.current.is_null() && window.current_key == key) {
            return window.current.dereference(*_pool); // found
        } else {
            return NULL; // not found.
        }
    }

    /**
     * \brief Removes the given key from this list.
     * \details
     * This might only mark the pointer, letting following traversals to do the cleaning.
     */
    bool    remove(KEY key) {
        while (true) {
            Window window = find(key);
            if (window.current.is_null() || window.current_key != key) {
                return false;
            } else {
                // remove current. so, mark current->next for death.
                T* current = window.current.dereference(*_pool);
                GcPointer<T> successor_old(current->next);
                GcPointer<T> successor_new(current->next);
                successor_new.set_mark(true);
                successor_new.set_aba(successor_new.get_aba() + 1);
                if (current->next.atomic_cas(successor_old, successor_new)) {
                    // CAS succeeded. now really delink it.
                    // even if this fails, fine. The later traversals will clean it up.
                    delink(window.predecessor, window.current, successor_new);
                    return true;
                } else {
                    // CAS failed. start over.
                    continue;
                }
            }
        }
    }

    /**
     * Wait-free method to tell if the given key exists in this list.
     */
    bool    contains(KEY key) const {
        for (GcPointer<T> current = _head.next; !current.is_null(); ) {
            T* value = current.dereference(*_pool);
            if (value->key == key) {
                return !value->next.is_marked();
            } else if (value->key > key) {
                return false;
            }
            current = value->next;
        }
        return false;
    }

    /**
     * \brief [NOT thread-safe] Removes all entries.
     * \details
     * This method is NOT thread-safe. Call this only in a safe situation.
     * This class calls this method in destructor, but, depending on destruction order,
     * the pool object might be already revoked then.
     * To avoid such errors, call this method as soon as you are done.
     */
    void    unsafe_clear() {
        mfence(); // doesn't solve all issues, but better than nothing
        for (GcPointer<T> current = _head.next; !current.is_null(); current = _head.next) {
            T *v = current.dereference(*_pool);
            v->next.set_mark(true); // this is not atomic
            delink(&_head, current, v->next);
        }
    }

    /**
     * \brief [NOT thread-safe] Returns the number of active entries.
     * \details
     * This method is NOT thread-safe nor accurate.
     * Do not rely on this other than a debug/safe situation.
     * There is no really accurate size() semantics in lock-free linked list because it
     * inherently needs to block others.
     */
    size_t  unsafe_size() const {
        mfence(); // doesn't solve all issues, but better than nothing
        size_t ret = 0;
        for (GcPointer<T> e = _head.next; !e.is_null(); e = e.dereference(*_pool)->next) {
            if (!e.is_marked()) {
                ++ret;
            }
        }
        return ret;
    }

    /**
     * \brief [NOT thread-safe] Dumps out all existing keys to the given stream.
     * \details
     * This method is NOT thread-safe. Call this only in a safe/debug situation.
     */
    void    unsafe_dump_keys(std::ostream &out) const {
        mfence(); // doesn't solve all issues, but better than nothing
        out << "LockFreeList (size=" << unsafe_size() << "):" << std::endl;

        for (GcPointer<T> e = _head.next; !e.is_null(); ) {
            T* v = e.dereference(*_pool);
            out << "  Key=" << v->key << ", marked=" << v->next.is_marked() << std::endl;
            e = v->next;
        }
    }

    /**
     * \brief [NOT thread-safe] Checks if all entries are sorted by key.
     * \details
     * This method is NOT thread-safe. Call this only in a safe/debug situation.
     */
    bool    unsafe_sorted() const {
        mfence(); // doesn't solve all issues, but better than nothing
        bool first = true;
        KEY prev_key;
        for (GcPointer<T> e = _head.next; !e.is_null(); ) {
            T* v = e.dereference(*_pool);
            if (first) {
                first = false;
            } else {
                if (prev_key >= v->key) {
                    return false; // not sorted!!
                }
            }
            prev_key = v->key;
            e = v->next;
        }
        return true;
    }

    /**
     * \brief [NOT thread-safe] Returns the contents of this list as std::set.
     * \details
     * This method is NOT thread-safe. Call this only in a safe/debug situation.
     */
    void   unsafe_as_set(std::set<KEY> &out) const {
        mfence(); // doesn't solve all issues, but better than nothing
        out.clear();
        for (GcPointer<T> e = _head.next; !e.is_null(); ) {
            T* v = e.dereference(*_pool);
            if (!v->next.is_marked()) {
                out.insert(v->key);
            }
            e = v->next;
        }
    }

protected:

    /**
     * \brief Returns the predecessor/current tuple on either side of the key.
     * This removes marked entries it encounters.
     * @return a window where current is the first entry whose key >= given key and NULL
     * if not such entry exists. predecessor is the last entry whose key is smaller than
     * given key and never NULL because of head.
     */
    Window  find(KEY key) const {
        Window result;
        while (find_retry_loop(key, result));
        assert(result.predecessor != NULL); // because of head, this is never NULL.
        return result;
    }

    /**
     * Main routine of find()
     * @return whether we must retry
     */
    bool    find_retry_loop(KEY key, Window &result) const {
        result.predecessor = &_head;
        result.current = _head.next;
        while (!result.current.is_null()) {
            T* current = result.current.dereference(*_pool);
            GcPointer<T> successor = current->next;
            while (successor.is_marked()) {
                // current marked for removal. let's delink it
                if (delink(result.predecessor, result.current, successor)) {
                    result.current = successor;
                    current = result.current.dereference(*_pool);
                    if (current == NULL) {
                        successor = GcPointer<T>();
                    } else {
                        successor = current->next;
                    }
                } else {
                    // CAS failed. someone might have done something in predecessor, retry.
                    return true;
                }
            }

            result.current_key = current->key;
            if (result.current_key >= key) {
                return false;
            }
            result.predecessor = current;
            result.current = successor;
        }
        // we reached the tail
        return false;
    }

    /**
     * \brief Delinks a marked entry from the list and deallocates the entry object.
     * @return whether the delink was really done by this thread
     * \details
     * target should be marked for death (target->next.is_marked()), but this is not
     * a contract because of concurrent delinks. If it isn't (say target is already delinked),
     * then the atomic CAS fails, returning false.
     * \NOTE Although might sound weird, this method \e is const.
     * This method \b physically delinks an already-marked entry, so \b logically it does
     * nothing.
     */
    bool    delink(T* predecessor, const GcPointer<T> &target,
                   const GcPointer<T> &successor) const {
        GcPointer<T> successor_after(successor);
        successor_after.set_aba(target.get_aba() + 1);
        successor_after.set_mark(false);
        if (predecessor->next.atomic_cas(target, successor_after)) {
            // we just have delinked. now deallocate the entry object.
            assert(target.dereference(*_pool)->next.is_marked()); // now we are sure.
            _pool->deallocate(target.raw());
            return true;
        } else {
            // delink failed. someone has done it.
            return false;
        }
    }

    /**
     * As this linked-list is based on atomic CAS, we don't need memory barriers.
     * However, some unsafe_ methods might benefit from full mfence though it won't solve
     * all the issues.
     */
    void    mfence() const {
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
    }

    /**
     * The always-existing dummy entry as head.
     * _head is never marked for death.
     * Mutable because even find() physically removes something (though logically nothing).
     */
    mutable T           _head;

    /**
     * Pool object to instantiate/release entries.
     */
    GcPoolForest<T>*    _pool;
};

#endif // W_LOCKFREE_LIST_H
