/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#ifndef W_LOCKFREE_LIST_H
#define W_LOCKFREE_LIST_H

#include <stdint.h>
#include "w_markable_pointer.h"

/**
 * \brief A lock-free singly-linked list described in [HERLIHY] Chap 9.8.
 * @tparam VALUE class of each entry in the list.
 * @tparam KEY type of key to identify entry. Must be a comparable type.
 * @tparam POOL class that can instantiate VALUE class object.
 * \details
 * The entry class (VALUE) must define get_key(), is_obsolete(), next().
 *  \li key() must return KEY type.
 *  \li is_obsolete() must return bool, telling if the entry should be removed.
 *  \li next() must return MarkablePointer<VALUE>&, next entry in the linked list.
 *  \li In addition, POOL must define allocate_instance(KEY) and deallocate_instance(VALUE*).
 * This implementation uses the \ref MARKPTR to safely remove an entry in lock-free fashion.
 */
template <class VALUE, typename KEY, class POOL>
class LockFreeList {
public:
    /** Predecessor/current tuple. */
    struct Window {
        Window() : predecessor(NULL), current(NULL) {}
        VALUE*  predecessor;
        VALUE*  current;
    };

    /** Instantiated with the pool to allocate/deallocate entries. */
    LockFreeList(POOL &pool) : _pool(pool) {}
    ~LockFreeList() { clear(); }

    /**
     * \brief Returns the entry with the key, creating if not exists.
     * \details
     * This is a bit different from the algorithm in textbook because we need to control
     * how entries are instantiated. In short, this method combines find() and add().
     * In order to do them atomically/efficiently/flexibly, this method receives a pool object
     * to instantiate a new entry if the key doesn't exist in this list.
     */
    VALUE*  get_or_add(KEY key, POOL &pool) {
        while (true) {
            Window window = find(key);
            if (window.current != NULL && window.current->key == key) {
                return window.current; // found
            } else {
                // not found. need to insert.

            }
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
            if (window.current == NULL) {
                return false;
            } else if (window.currnet->key() != key) {
                return false;
            } else {
                MarkablePointer<VALUE> &successor = window.current->next();
                VALUE* succ = successor.get_pointer;
                if (successor.atomic_cas(succ, succ, false, true)) {
                    // CAS succeeded. now really delink it.
                    // even if this fails, fine. The later traversals will clean it up.
                    window.predecessor->next().atomic_cas(window.current, succ, false, false);
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
    bool    contains(KEY key) {
        MarkablePointer<VALUE> &current = _head.next();
        for (;!current.is_null() && current->key() < key; current = current->next());
        return !current.is_null() && current->key() == key && !current.is_marked();
    }

    /**
     * \brief Removes all entries.
     * \details
     * This class calls this method in destructor,
     * but, depending on destruction order, the pool object might be already revoked then.
     * To avoid such errors, call this method as soon as you are done.
     */
    void    clear() {
        while (_head.next().is_null()) {
            remove(_head.next()->key());
        }
    }

private:

    /**
     * \brief Returns the predecessor/current tuple on either side of the key.
     * This removes obsolete entries it encounters.
     */
    Window  find(KEY key) {
        Window result;
        while (find_retry_loop(key, result));
        return result;
    }

    /**
     * Main routine of find()
     * @return whether we must retry
     */
    bool    find_retry_loop(KEY key, Window &result) {
        VALUE* predecessor = &_head;
        VALUE* current = _head.get_next().get_pointer();
        while (current != NULL) {
            MarkablePointer<VALUE> &successor = current->next();
            while (!successor.is_null() && successor.is_marked()) {
                // marked for removal. let's remove it
                if (predecessor->next().atomic_cas(current, successor.get_pointer(),
                    false, false)) {
                    current = successor.get_pointer();
                    successor = current->get_next();
                } else {
                    // CAS failed. someone might have done something in predecessor, retry.
                    return true;
                }
            }

            if (current->key() >= key) {
                result.predecessor = predecessor;
                result.current = current;
                return false;
            }
            predecessor = current;
            current = successor.get_pointer();
        }
        return false;
    }

    /**
     * The always-existing dummy entry as head.
     */
    VALUE                   _head;

    /**
     * Functor to instantiate/release entries.
     */
    POOL&                   _pool;
};

#endif // W_LOCKFREE_LIST_H
