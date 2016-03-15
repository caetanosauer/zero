/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */
#ifndef BF_HASHTABLE_CPP
#define BF_HASHTABLE_CPP

#include "w_defines.h"
#include "w_base.h"
#include "basics.h"
#include "w_hashing.h"
#include "bf_hashtable.h"
#include "latch.h"
#include <string.h>

const size_t HASHBUCKET_INITIAL_CHUNK_SIZE = 4;
const uint32_t BF_HASH_SEED = 0x35D0B891;
const uint32_t HASHBUCKET_INITIAL_EXPANSION = 16;
const uint32_t HASHBUCKET_SUBSEQUENT_EXPANSION = 4;

inline uint32_t bf_hash(PageID x) {
    // CS TODO: use stdlib hashing
    return w_hashing::uhash::hash32(BF_HASH_SEED, x);
}

/** this one allows variable size in case of highly skewed hash. */
template<class T>
struct bf_hashbucket_chunk_linked {
    bf_hashbucket_chunk_linked (uint32_t size_)
        : size (size_),
            values (new T[size]),
            keys (new PageID[size]),
            next_chunk (NULL) {
        ::memset (values, 0, sizeof(T) * size);
        ::memset (keys, 0, sizeof(PageID) * size);
    }
    uint32_t  size;
    T*   values;
    PageID* keys;
    bf_hashbucket_chunk_linked* next_chunk;

    void delete_chain() {
        if (next_chunk != NULL) {
            next_chunk->delete_chain();
            delete next_chunk;
            next_chunk = NULL;
        }
    }
};

/**
 * Simply chaining each single entry costs way too much and
 * also we have to call new/delete frequently, which is not acceptable at all.
 * This chunk class reduces the frequency and also allows initialization
 * by malloc and memset.
 */
template<class T>
struct bf_hashbucket_chunk {
    T   values[HASHBUCKET_INITIAL_CHUNK_SIZE];
    PageID keys[HASHBUCKET_INITIAL_CHUNK_SIZE];
    /** chains to next chunk if (as a rare occasion) this chunk is not enough. */
    bf_hashbucket_chunk_linked<T>* next_chunk;

    void delete_chain() {
        if (next_chunk != NULL) {
            next_chunk->delete_chain();
            delete next_chunk;
            next_chunk = NULL;
        }
    }
};

/**
 * Hash bucket with chaining entries.
 */
template<class T>
class bf_hashbucket {
public:
    srwlock_t                   _lock;
    bf_hashbucket_chunk<T>         _chunk;
    uint32_t _used_count;

    bool find (PageID key, T& value);
    bool append_if_not_exists (PageID key, T value);
    bool update (PageID key, T value);
    bool remove (PageID key);
private:
    bf_hashbucket(); // prohibited (not implemented). this class should be bulk-initialized by memset
};

template<class T>
bool bf_hashbucket<T>::find(PageID key, T& value) {
    spinlock_read_critical_section cs(&_lock);

    //first, take a look at initial chunk
    for (uint32_t i = 0; i < _used_count && i < HASHBUCKET_INITIAL_CHUNK_SIZE; ++i) {
        if (_chunk.keys[i] == key) {
            value = _chunk.values[i];
            return true;
        }
    }

    //then, probe chained chunks if exists
    uint32_t cur_count = HASHBUCKET_INITIAL_CHUNK_SIZE;
    for (bf_hashbucket_chunk_linked<T>* cur_chunk
            = _chunk.next_chunk; cur_count < _used_count;
            cur_chunk = cur_chunk->next_chunk)
    {
        w_assert1(cur_chunk != NULL);
        for (uint32_t i = 0; i < cur_chunk->size && cur_count < _used_count;
                ++i, ++cur_count) {
            if (cur_chunk->keys[i] == key) {
                value = cur_chunk->values[i];
                return true;
            }
        }
    }

    return false; // not found
}

template<class T>
bool bf_hashbucket<T>::update (PageID key, T value) {
    spinlock_write_critical_section cs(&_lock);

    for (uint32_t i = 0; i < _used_count && i < HASHBUCKET_INITIAL_CHUNK_SIZE; ++i) {
        if (_chunk.keys[i] == key) {
            _chunk.values[i] = value;
            return true;
        }
    }

    uint32_t cur_count = HASHBUCKET_INITIAL_CHUNK_SIZE;
    for (bf_hashbucket_chunk_linked<T>* cur_chunk = _chunk.next_chunk; cur_count < _used_count; cur_chunk = cur_chunk->next_chunk) {
        w_assert1(cur_chunk != NULL);
        for (uint32_t i = 0; i < cur_chunk->size && cur_count < _used_count; ++i, ++cur_count) {
            if (cur_chunk->keys[i] == key) {
                cur_chunk->values[i] = value;
                return true;
            }
        }
    }

    return false;
}

template<class T>
bool bf_hashbucket<T>::append_if_not_exists (PageID key, T value) {
    spinlock_write_critical_section cs(&_lock);

    for (uint32_t i = 0; i < _used_count && i < HASHBUCKET_INITIAL_CHUNK_SIZE; ++i) {
        if (_chunk.keys[i] == key) {
            return false;
        }
    }

    if (_used_count < HASHBUCKET_INITIAL_CHUNK_SIZE) {
        _chunk.values[_used_count] = value;
        _chunk.keys[_used_count] = key;
        ++_used_count;
        return true;
    }
    if (_chunk.next_chunk == NULL) {
        _chunk.next_chunk = new bf_hashbucket_chunk_linked<T>
            (HASHBUCKET_INITIAL_CHUNK_SIZE * HASHBUCKET_INITIAL_EXPANSION);
    }

    uint32_t cur_count = HASHBUCKET_INITIAL_CHUNK_SIZE;
    for (bf_hashbucket_chunk_linked<T>* cur_chunk = _chunk.next_chunk;; cur_chunk = cur_chunk->next_chunk) {
        for (uint32_t i = 0; i < _used_count - cur_count && i < cur_chunk->size; ++i) {
            if (cur_chunk->keys[i] == key) {
                return false;
            }
        }

        if (_used_count - cur_count < cur_chunk->size) {
            cur_chunk->values[_used_count - cur_count] = value;
            cur_chunk->keys[_used_count - cur_count] = key;
            ++_used_count;
            return true;
        }
        cur_count += cur_chunk->size;
        w_assert1(cur_count <= _used_count);
        if (cur_chunk->next_chunk == NULL) {
            cur_chunk->next_chunk = new bf_hashbucket_chunk_linked<T>
                (cur_chunk->size * HASHBUCKET_SUBSEQUENT_EXPANSION);
        }
    }
    // shouldn't reach here
    w_assert0(false);
    return false;
}

template<class T>
bool bf_hashbucket<T>::remove (PageID key) {
    spinlock_write_critical_section cs(&_lock);
    bool found = false;
    //first, take a look at initial chunk
    for (uint32_t i = 0; i < _used_count && i < HASHBUCKET_INITIAL_CHUNK_SIZE; ++i) {
        if (found) {
            _chunk.values[i - 1] = _chunk.values[i];
            _chunk.keys[i - 1] = _chunk.keys[i];
            continue;
        }
        if (_chunk.keys[i] == key) {
            found = true;
        }
    }
    if (found && _used_count > HASHBUCKET_INITIAL_CHUNK_SIZE) {
        _chunk.values[HASHBUCKET_INITIAL_CHUNK_SIZE - 1] = _chunk.next_chunk->values[0];
        _chunk.keys[HASHBUCKET_INITIAL_CHUNK_SIZE - 1] = _chunk.next_chunk->keys[0];
    }

    //then, probe chained chunks if exists
    uint32_t cur_count = HASHBUCKET_INITIAL_CHUNK_SIZE;
    for (bf_hashbucket_chunk_linked<T>* cur_chunk = _chunk.next_chunk; cur_count < _used_count; cur_chunk = cur_chunk->next_chunk) {
        w_assert1(cur_chunk != NULL);
        for (uint32_t i = 0; i < cur_chunk->size && cur_count < _used_count; ++i, ++cur_count) {
            if (found) {
                if (i > 0) {
                    cur_chunk->values[i - 1] = cur_chunk->values[i];
                    cur_chunk->keys[i - 1] = cur_chunk->keys[i];
                }
                continue;
            }
            if (cur_chunk->keys[i] == key) {
                found = true;
            }
        }
        if (found && _used_count > cur_count) {
            cur_chunk->values[cur_chunk->size - 1] = cur_chunk->next_chunk->values[0];
            cur_chunk->keys[cur_chunk->size - 1] = cur_chunk->next_chunk->keys[0];
        }
    }

    if (found) {
        --_used_count;
    }
    return found;
}

template<class T>
bf_hashtable<T>::bf_hashtable(uint32_t size) : _size(size) {
    _table = reinterpret_cast<bf_hashbucket<T>*>
        (new char[sizeof(bf_hashbucket<T>) * size]);
    ::memset (_table, 0, sizeof(bf_hashbucket<T>) * size);
}

template<class T>
bf_hashtable<T>::~bf_hashtable() {
    if (_table != NULL) {
        for (uint32_t i = 0; i < _size; ++i) {
            _table[i]._chunk.delete_chain();
        }
        delete[] reinterpret_cast<char*>(_table);
    }
}

template<class T>
bool bf_hashtable<T>::update(PageID key, T value) {
    uint32_t hash = bf_hash(key);
    return _table[hash % _size].update(key, value);
}

template<class T>
bool bf_hashtable<T>::insert_if_not_exists(PageID key, T value) {
    uint32_t hash = bf_hash(key);
    return _table[hash % _size].append_if_not_exists(key, value);
}

template<class T>
bool bf_hashtable<T>::lookup(PageID key, T& value) const {
    uint32_t hash = bf_hash(key);
    return _table[hash % _size].find(key, value);
}

template<class T>
bool bf_hashtable<T>::remove(PageID key) {
    uint32_t hash = bf_hash(key);
    return _table[hash % _size].remove(key);
}

#endif
