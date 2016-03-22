/*
 * (c) Copyright 2014, Hewlett-Packard Development Company, LP
 */
/*
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2010-2014
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

#include "w_defines.h"

#include "sm_base.h"
#include "log_carray.h"

void ConsolidationArray::wait_for_leader(CArraySlot* info) {
    long old_count;
    while( (old_count=info->vthis()->count) >= SLOT_FINISHED);
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
}

bool ConsolidationArray::wait_for_expose(CArraySlot* info) {
    w_assert1(SLOT_FINISHED == info->vthis()->count);
    w_assert1(CARRAY_RELEASE_DELEGATION);
    lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
    // If there is a predecessor which is still running,
    // let's try to delegate the work of releasing the buffer
    // to the slow predecessor. 1/32 to stop too long chains.
    const int TERMINATE_CHAIN_POSSIBILITY = 32;
    if(info->pred2 && (_indexof(info) % TERMINATE_CHAIN_POSSIBILITY) != 0) {
        lintel::atomic_thread_fence(lintel::memory_order_release);
        // Atomically change my status to DELEGATED.
        int64_t waiting_cas_tmp = QNODE_WAITING._combined;
        if(info->me2._status._combined == QNODE_WAITING._combined &&
            lintel::unsafe::atomic_compare_exchange_strong<int64_t>(
                &info->me2._status._combined, &waiting_cas_tmp, QNODE_DELEGATED._combined)) {
            return true; // delegate succeeded
        }
    }
    _expose_lock.__unsafe_end_acquire(&info->me2, info->pred2);
    return false;
}

ConsolidationArray::ConsolidationArray(int active_slot_count)
    : _slot_mark(0), _active_slot_count(active_slot_count) {
    // Zero-out all slots
    ::memset(_all_slots, 0, sizeof(CArraySlot) * ALL_SLOT_COUNT);
    typedef CArraySlot* CArraySlotPtr;
    _active_slots = new CArraySlotPtr[_active_slot_count];
    for (int i = 0; i < ALL_SLOT_COUNT; ++i) {
        _all_slots[i].count = SLOT_UNUSED;
        _all_slots[i].error = w_error_ok;
    }
    // Mark initially active slots
    for (int i = 0; i < _active_slot_count; ++i) {
        _active_slots[i] = _all_slots + i;
        _active_slots[i]->count = SLOT_AVAILABLE;
    }
}
ConsolidationArray::~ConsolidationArray() {
    delete[] _active_slots;
    // Check all slots are freed
    for (int i = 0; i < ALL_SLOT_COUNT; ++i) {
        w_assert0(_all_slots[i].count == SLOT_UNUSED
            || _all_slots[i].count == SLOT_AVAILABLE);
    }
}


CArraySlot* ConsolidationArray::join_slot(int32_t size, carray_status_t &old_count)
{
    w_assert1(size > 0);
    carray_slotid_t idx =  (carray_slotid_t) ::pthread_self();
    while (true) {
        // probe phase
        CArraySlot* info = NULL;
        while (true) {
            idx = (idx + 1) % _active_slot_count;
            info = _active_slots[idx];
            old_count = info->vthis()->count;
            if (old_count >= SLOT_AVAILABLE) {
                // this slot is available for join!
                break;
            }
        }

        // join phase
        while (true) {
            // set to 'available' and add our size to the slot
            carray_status_t new_count = join_carray_status(old_count, size);
            carray_status_t old_count_cas_tmp = old_count;
            if(lintel::unsafe::atomic_compare_exchange_strong<carray_status_t>(
                &info->count, &old_count_cas_tmp, new_count))
            {
                // CAS succeeded. All done.
                // The assertion below doesn't necessarily hold because of the
                // ABA problem -- someone else might have grabbed the same slot
                // and gone through a whole join-release cycle, so that info is
                // now on a different array position. In general, this second
                // while loop must not use idx at all.
                // w_assert1(old_count != 0 || _active_slots[idx] == info);
                return info;
            }
            else {
                // the status has been changed.
                w_assert1(old_count != old_count_cas_tmp);
                old_count = old_count_cas_tmp;
                if (old_count < SLOT_AVAILABLE) {
                    // it's no longer available. retry from probe
                    break;
                } else {
                    // someone else has joined, but still able to join.
                    continue;
                }
            }
        }
    }
}

void ConsolidationArray::join_expose(CArraySlot* info) {
    if (CARRAY_RELEASE_DELEGATION) {
        info->me2._status.individual._delegated = 0;
        info->pred2 = _expose_lock.__unsafe_begin_acquire(&info->me2);
    }
}

CArraySlot* ConsolidationArray::grab_delegated_expose(CArraySlot* info) {
    // Four cases to consider
    // 1. Delegated
    // 2. Delegating
    // 3. Spinning (can't delegate)
    // 4. Busy
    w_assert1(SLOT_FINISHED == info->vthis()->count);
    if (CARRAY_RELEASE_DELEGATION) {
        lintel::atomic_thread_fence(lintel::memory_order_release);
        // did next (predecessor in terms of logging) delegate to us?
        mcs_lock::qnode *next = info->me2.vthis()->_next;
        if (!next) {
            // the above fast check is not atomic if someone else is now connecting.
            // (if it's already connected, as 8-byte read is at least regular, safe)
            // So, additional atomic CAS to make sure we really don't have next.
            mcs_lock::qnode* me2_cas_tmp = &(info->me2);
            if (!lintel::unsafe::atomic_compare_exchange_strong<mcs_lock::qnode*>(
                &_expose_lock._tail, &me2_cas_tmp, (mcs_lock::qnode*) NULL)) {
                // CAS failed, so someone just connected to us.
                w_assert1(_expose_lock._tail != info->me2.vthis());
                w_assert1(info->me2.vthis()->_next != NULL);
                next = _expose_lock.spin_on_next(&info->me2);
            } else {
                // CAS succeeded, so we removed ourself from _expose_lock!
            }
        }

        if (next) {
            // This is safe because me2 is the first element
            CArraySlot* next_i = reinterpret_cast<CArraySlot*>(next);
            w_assert1(&next_i->me2 == next);

            // if the next says it's delegated, we take it over.
            int64_t status_cas_tmp = QNODE_WAITING._combined;
            lintel::unsafe::atomic_compare_exchange_strong<int64_t>(
                &(next_i->me2._status._combined), &status_cas_tmp, QNODE_IDLE._combined);
            if (status_cas_tmp == QNODE_DELEGATED._combined) {
                // they delegated... up to us to do their dirty work
                w_assert1(SLOT_FINISHED == next_i->vthis()->count);
                w_assert1(next_i->pred2 == &info->me2);
                lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
                info->vthis()->count = SLOT_UNUSED;
                info = next_i;
                return info;
            }
        }
    }

    // if I get here I hit NULL or non-delegate[ed|able] node, so we are done.
    lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
    info->vthis()->count = SLOT_UNUSED;
    return NULL;
}

void ConsolidationArray::replace_active_slot(CArraySlot* info)
{
    w_assert1(info->count > SLOT_AVAILABLE);
    while (SLOT_UNUSED != _all_slots[_slot_mark].count) {
        if(++_slot_mark == ALL_SLOT_COUNT) {
            _slot_mark = 0;
        }
    }
    _all_slots[_slot_mark].count = SLOT_AVAILABLE;

    // Look for pointer to the slot in the active array
    for (int i = 0; i < _active_slot_count; i++) {
        if (_active_slots[i] == info) {
            _active_slots[i] = _all_slots + _slot_mark;
        }
    }
}
