/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
#include "w_defines.h"

#define SM_SOURCE

#include "block_alloc.h"

#include "sm_base.h"
#include "lock_s.h"
#include "lock_x.h"
#include "lock_core.h"
#include "tls.h"
#include "lock_compt.h"
#include "lock_bucket.h"
#include "w_okvl.h"
#include "w_okvl_inl.h"

DECLARE_TLS(block_alloc<xct_lock_entry_t>, xctLockEntryPool);

xct_lock_info_t::xct_lock_info_t()
    : _head (NULL), _tail (NULL), _permission_to_violate (false) {
    // init_wait_map(g_me());
}

// allows reuse rather than free/malloc of the structure
xct_lock_info_t* xct_lock_info_t::reset_for_reuse() {
    // make sure the lock lists are empty
    w_assert1(_head == NULL);
    w_assert1(_tail == NULL);
    new (this) xct_lock_info_t;
    return this;
}

xct_lock_info_t::~xct_lock_info_t() {
    w_assert1(_head == NULL);
    w_assert1(_tail == NULL);
}

xct_lock_entry_t* xct_lock_info_t::link_to_new_request (lock_queue_t *queue,
                                                        lock_queue_entry_t *entry) {
    xct_lock_entry_t* link = new (*xctLockEntryPool) xct_lock_entry_t();
    link->queue = queue;
    link->entry = entry;
    if (_head == NULL) {
        _head = link;
        _tail = link;
    } else {
        _tail->next = link;
        link->prev = _tail;
        _tail = link;
    }
    _hashmap.push_front(link); // also add to private hashmap
    return link;
}
void xct_lock_info_t::remove_request (xct_lock_entry_t *entry) {
#if W_DEBUG_LEVEL>=3
    bool found = false;
    for (xct_lock_entry_t *p = _head; p != NULL; p = p->next) {
        if (p == entry) {
            found = true;
            break;
        }
    }
    w_assert3(found);
#endif //W_DEBUG_LEVEL>=3
    if (entry->prev == NULL) {
        // then it should be current head
        w_assert1(_head == entry);
        _head = entry->next;
        if (_head != NULL) {
            _head->prev = NULL;
        }
    } else {
        w_assert1(entry->prev->next == entry);
        entry->prev->next = entry->next;
    }

    if (entry->next == NULL) {
        // then it should be current tail
        w_assert1(_tail == entry);
        _tail = entry->prev;
        if (_tail != NULL) {
            _tail->next = NULL;
        }
    } else {
        w_assert1(entry->next == _head || entry->next->prev == entry);
        entry->next->prev = entry->prev;
    }

    _hashmap.remove(entry); //removes from private hashmap, too

    xctLockEntryPool->destroy_object(entry);
}

XctLockHashMap::XctLockHashMap() {
    reset();
}
XctLockHashMap::~XctLockHashMap() {
}
void XctLockHashMap::reset() {
    ::memset(_buckets, 0, sizeof(xct_lock_entry_t*) * XCT_LOCK_HASHMAP_SIZE);
}

const okvl_mode& XctLockHashMap::get_granted_mode(uint32_t lock_id) const {
    uint32_t bid = _bucket_id(lock_id);
    // we don't take any latch here. See the comment of XctLockHashMap
    // for why this is safe.
    for (const xct_lock_entry_t *current = _buckets[bid]; current != NULL;
         current = current->private_hashmap_next) {
        if (current->queue->hash() == lock_id) {
            return current->entry->get_granted_mode();
        }
    }
    return ALL_N_GAP_N;
}

void XctLockHashMap::push_front(xct_lock_entry_t* link) {
    // link becomes a new head of the bucket
    uint32_t bid = _bucket_id(link->queue->hash());
    link->private_hashmap_prev = NULL;
    link->private_hashmap_next = _buckets[bid];
    if (_buckets[bid] != NULL) {
        _buckets[bid]->private_hashmap_prev = link;
    }
    _buckets[bid] = link;
}
void XctLockHashMap::remove(xct_lock_entry_t* link) {
    if (link->private_hashmap_next != NULL) {
        link->private_hashmap_next->private_hashmap_prev = link->private_hashmap_prev;
    }
    if (link->private_hashmap_prev != NULL) {
        link->private_hashmap_prev->private_hashmap_next = link->private_hashmap_next;
    } else {
        // "link" was the head.
        uint32_t bid = _bucket_id(link->queue->hash());
        w_assert1(_buckets[bid] == link);
        _buckets[bid] = link->private_hashmap_next;
    }
}
