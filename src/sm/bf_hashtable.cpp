/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"
#include "w_base.h"
#include "basics.h"
#include "sm_s.h"
#include "w_hashing.h"
#include "bf_hashtable.h"
#include "latch.h"
#include <string.h>

const size_t HASHBUCKET_INITIAL_CHUNK_SIZE = 4;
const uint32_t BF_HASH_SEED = 0x35D0B891;
const uint32_t HASHBUCKET_INITIAL_EXPANSION = 16;
const uint32_t HASHBUCKET_SUBSEQUENT_EXPANSION = 4;

inline uint32_t bf_hash(uint64_t x) {
    return w_hashing::uhash::hash32(BF_HASH_SEED, x);
}

/** this one allows variable size in case of highly skewed hash. */
struct bf_hashbucket_chunk_linked {
    bf_hashbucket_chunk_linked (uint32_t size_)
        : size (size_),
            values (new bf_idx[size]),
            keys (new uint64_t[size]),
            next_chunk (NULL) {
        ::memset (values, 0, sizeof(bf_idx) * size);
        ::memset (keys, 0, sizeof(uint64_t) * size);
    }
    uint32_t  size;
    bf_idx*   values;
    uint64_t* keys;
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
struct bf_hashbucket_chunk {
    bf_idx   values[HASHBUCKET_INITIAL_CHUNK_SIZE];
    uint64_t keys[HASHBUCKET_INITIAL_CHUNK_SIZE];
    /** chains to next chunk if (as a rare occasion) this chunk is not enough. */
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
 * Hash bucket with chaining entries.
 */
class bf_hashbucket {
public:
    srwlock_t                   _lock;
    bf_hashbucket_chunk         _chunk;
    uint32_t _used_count;

    bf_idx find (uint64_t key);
    bool append_if_not_exists (uint64_t key, bf_idx value);
    bool remove (uint64_t key);
private:
    bf_hashbucket(); // prohibited (not implemented). this class should be bulk-initialized by memset
};

bf_idx bf_hashbucket::find(uint64_t key) { 
    spinlock_read_critical_section cs(&_lock);

    //first, take a look at initial chunk
    for (uint32_t i = 0; i < _used_count && i < HASHBUCKET_INITIAL_CHUNK_SIZE; ++i) {
        if (_chunk.keys[i] == key) {
            return _chunk.values[i];
        }
    }

    //then, probe chained chunks if exists
    uint32_t cur_count = HASHBUCKET_INITIAL_CHUNK_SIZE;
    for (bf_hashbucket_chunk_linked* cur_chunk = _chunk.next_chunk; cur_count < _used_count; cur_chunk = cur_chunk->next_chunk) {
        w_assert1(cur_chunk != NULL);
        for (uint32_t i = 0; i < cur_chunk->size && cur_count < _used_count; ++i, ++cur_count) {
            if (cur_chunk->keys[i] == key) {
                return cur_chunk->values[i];
            }
        }
    }

    return 0; // not found
}

bool bf_hashbucket::append_if_not_exists (uint64_t key, bf_idx value) {
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
        _chunk.next_chunk = new bf_hashbucket_chunk_linked (HASHBUCKET_INITIAL_CHUNK_SIZE * HASHBUCKET_INITIAL_EXPANSION);
    }

    uint32_t cur_count = HASHBUCKET_INITIAL_CHUNK_SIZE;
    for (bf_hashbucket_chunk_linked* cur_chunk = _chunk.next_chunk;; cur_chunk = cur_chunk->next_chunk) {
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
            cur_chunk->next_chunk = new bf_hashbucket_chunk_linked (cur_chunk->size * HASHBUCKET_SUBSEQUENT_EXPANSION);
        }
    }
    // shouldn't reach here
    w_assert0(false);
    return false;
}

bool bf_hashbucket::remove (uint64_t key) {
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
    for (bf_hashbucket_chunk_linked* cur_chunk = _chunk.next_chunk; cur_count < _used_count; cur_chunk = cur_chunk->next_chunk) {
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

bf_hashtable::bf_hashtable(uint32_t size) : _size(size) {
    _table = reinterpret_cast<bf_hashbucket*>(new char[sizeof(bf_hashbucket) * size]);
    ::memset (_table, 0, sizeof(bf_hashbucket) * size);
}

bf_hashtable::~bf_hashtable() {
    if (_table != NULL) {
        for (uint32_t i = 0; i < _size; ++i) {
            _table[i]._chunk.delete_chain();
        }
        delete[] reinterpret_cast<char*>(_table);
    }
} 

bool bf_hashtable::insert_if_not_exists(uint64_t key, bf_idx value) {
    uint32_t hash = bf_hash(key);
    return _table[hash % _size].append_if_not_exists(key, value);
}

bf_idx bf_hashtable::lookup(uint64_t key) const {
    uint32_t hash = bf_hash(key);
    return _table[hash % _size].find(key);
}

bool bf_hashtable::remove(uint64_t key) {
    uint32_t hash = bf_hash(key);
    return _table[hash % _size].remove(key);
}
