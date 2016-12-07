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
#ifndef LOG_CARRAY_H
#define LOG_CARRAY_H

/**
 * \defgroup CARRAY Consolidation Array
 * \ingroup SSMLOG
 * \brief \b Consolidation \b Array (\b C-Array).
 * \details
 * Logging functions/members to implement \e Consolidation-Array with \e Decoupled-Buffer-Fill
 * and \e Delegated-Buffer-Release invented at CMU/EPFL.
 * This technique dramatically reduces the contention in log buffer
 * accesses. For more details, read the Aether paper and its extended version on VLDB Journal.
 *
 * \section ACK Acknowledgement
 * The ideas, performance evaluations, and the initial implementation are solely done by the
 * EPFL team. We just took the implementation and refactored, keeping the main logics.
 * @see https://bitbucket.org/shoremt/shore-mt/commits/66e5c0aa2f6528cfdbda0907ad338066f14ec066
 *
 * \section DIFF Differences from Original Implementation
 * A few minor details have been changed.
 *   \li Separated the core C-Array logics to the classes in this file rather than
 * further bloating log_core.
 *   \li CArraySlot (insert_info in original code) places me2 at first so that we can avoid
 * the very tricky (or dubious in portability) offset calculation by union-ing int and pointer.
 * It's simply a reinterpret_cast in our code.
 *   \li qnode itself has the status as union so that we don't need hacked_qnode thingy.
 *   \li We use Lintel's atomic operations, which have slightly different method signatures.
 * We made according changes to incorporate that.
 *   \li Lots of comments added.
 *
 * \section PERF Performance Improvements
 * As of 20140220, with and without this improvement, TPCC on 4-socket machine is as
 * follows (12 worker threads, 3 active slots, CARRAY_RELEASE_DELEGATION=false).
 *   \li LOCK-ON: BEFORE=13800 TPS, AFTER=15400 TPS.
 *   \li LOCK-OFF: BEFORE=17900 TPS, AFTER=30900 TPS.
 *   \li i.e. LOCK-OFF, LOG-OFF: 63000 TPS.
 * So, big improvements in log contention, but not completely removing it.
 * Now lock manager is bigger bottleneck.
 *
 * \section CON Considerations
 * Among the three techniques, \e Delegated-Buffer-Release was a bit dubious to be added
 * because, as shown in the Aether paper, it has little benefits in "usual" workload yet
 * adds 10% overheads because of a few more atomic operations.
 * It might be worth the extra 10% for stability in case of highly skewed log sizes, so we
 * should keep an eye. CARRAY_RELEASE_DELEGATION option turns on/off this feature.
 *
 * Also, consider adjusting the number of active slots depending on the number of worker
 * threads as suggested in the paper. The startup option \b sm_carray_slots does it.
 * The default value for this option is ConsolidationArray#DEFAULT_ACTIVE_SLOT_COUNT
 *
 * \section REF Reference
 * \li Ryan Johnson, Ippokratis Pandis, Radu Stoica, Manos Athanassoulis, and Anastasia
 * Ailamaki. "Aether: a scalable approach to logging."
 * Proceedings of the VLDB Endowment 3, no. 1-2 (2010): 681-692.
 * \li Ryan Johnson, Ippokratis Pandis, Radu Stoica, Manos Athanassoulis, and Anastasia
 * Ailamaki. "Scalability of write-ahead logging on multicore and multisocket hardware."
 * The VLDB Journal 21, no. 2 (2012): 239-263.
 */

#include <stdint.h>
#include <boost/static_assert.hpp>
#include "w_base.h"
#include "w_error.h"
#include "mcs_lock.h"
#include "lsn.h"

/**
 * Whether to enable \e Delegated-Buffer-Release.
 * \ingroup CARRAY
 *
 * CS: in plog approach (Atomic Commit Protocol), delegated buffer releases
 * become crucial, since the total log volume of each transactions is more
 * likely to vary considerably. In the old approach, the technique is only
 * important in individual log record size (and not the sum of all log
 * records of a transaction) varies too much.
 */
const bool CARRAY_RELEASE_DELEGATION = false;

/**
 * \brief An integer to represents the status of one C-Array slot.
 * \ingroup CARRAY
 * \details
 * The high 32 bits represent the number of threads joining the group.
 * The low 32 bits represent the total number of bytes of the logs in the group.
 * We combine the two information to one 64bit integer for efficient atomic operations.
 * A C-Array slot is available for new use only when this status value is
 * exactly 0 (SLOT_AVAILABLE). Negative values have special meanings. See the constants below.
 * @see CArraySlot
 * @see ConsolidationArray::Constants
 */
typedef int64_t carray_status_t;

/**
 * Index in ConsolidationArray's slot.
 * \ingroup CARRAY
 */
typedef uint32_t carray_slotid_t;

/**
 * \brief One slot in ConsolidationArray.
 * \ingroup CARRAY
 * \details
 * Each slot belongs to two mcs_lock queues, one for buffer acquisition (me/_insert_lock)
 * and another for buffer release (me2/_expose_lock).
 */
struct CArraySlot {
    /**
    * \brief The secondary queue lock used to delegate buffer-release.
    * Lock head is ConsolidationArray::_expose_lock.
    * This must be the first member as we reinterpret qnode as insert_info.
    * See Section A.3 of Aether paper.
    */
    mcs_lock::qnode me2;                // +16 -> 16

// Logging information. Also useful as padding for cacheline (64 byte).
    /** where will we end up on disk? */
    lsn_t lsn;                          // +8 -> 24
    /** end point of our predecessor. */
    int64_t old_end;                    // +8 -> 32
    /** start point for thread groups. */
    int64_t start_pos;                  // +8 -> 40
    /** how much of the allocation already claimed? */
    int64_t pos;                        // +8 -> 48
    /** eventually assigned to _cur_epoch. */
    int64_t new_end;                    // +8 -> 56
    /** positive if we started a new partition */
    int64_t new_base;                   // +8 -> 64

    /**
     * \brief The current status of this slot.
     * \details
     * This is the key variable used for every atomic operation of C-array slot.
     * @see carray_status_t
     */
    carray_status_t count;              // +8 -> 72

    /**
    * The main queue lock used to acquire log buffers.
    * Lock head is log_core::_insert_lock.
    * \NOTE This should not be in the same cache line as me2.
    */
    mcs_lock::qnode me;                 // +16 -> 88
    /**
    * Predecessor qnode of me2. Used to delegate buffer release.
    */
    mcs_lock::qnode* pred2;             // +8 -> 96
    /**
     * Set when inserting the log of this slot failed, so far only eOUTOFLOGSPACE possible.
     */
    w_error_codes error;                // +sizeof(w_error_codes)

    /** To make this multiply of cacheline size. */
    char            padding[32-sizeof(w_error_codes)]; // +32-sizeof(w_error_codes) -> 128

    /**
     * volatile accesses to make sure compiler isn't fooling us.
     * Most code anyway relies on atomic operations. These are not heavily used.
     */
    CArraySlot volatile* vthis() { return this; }
    /** const version. */
    const CArraySlot volatile* vthis() const { return this; }
};
// Doesn't compile in old Debian.
//BOOST_STATIC_ASSERT_MSG((sizeof(CArraySlot) % CACHELINE_SIZE) == 0,
//    "size of CArraySlot must be aligned to CACHELINE_SIZE for better performance");

/**
 * \brief The implementation class of \b Consolidation \b Array.
 * \ingroup CARRAY
 * \details
 * See Section A.2 and A.3 of Aether paper.
 */
class ConsolidationArray {
public:
    /** @param[in] active_slot_count Max number of slots that can be active at the same time */
    ConsolidationArray(int active_slot_count);
    ~ConsolidationArray();

    /** Constant numbers. */
    enum Constants {
        /** Total number of slots. */
        ALL_SLOT_COUNT      = 256,

        /** Default value for max number of slots that can be active at the same time. */
        DEFAULT_ACTIVE_SLOT_COUNT   = 3,

        /**
        * slots that are in active slots and up for grab have this carray_status_t.
        */
        SLOT_AVAILABLE      = 0,
        /**
        * slots that are in the pool but not in active slots
        * have this carray_status_t.
        */
        SLOT_UNUSED         = -1,
        /**
        * Once the first thread in the slot puts this as carray_status_t, other threads can no
        * longer join the slot.
        */
        SLOT_PENDING        = -2,
        /**
        * Once the first thread acquires a buffer space and LSN, it puts this \b MINUS
        * the combined log size as the carray_status_t. All threads in the slot atomically add
        * its log size to this, making the last one notice that it is exactly SLOT_FINISHED.
        */
        SLOT_FINISHED       = -4,
    };

    /**
     * Calculate a new CArray status after joining the given log size to the existing status.
     */
    static carray_status_t join_carray_status (carray_status_t current_status, int32_t size) {
        w_assert1(size >= 0);
        w_assert1(current_status >= 0);
        const carray_status_t THREAD_INCREMENT = 1L << 32;
        return current_status + size + THREAD_INCREMENT;
    }
    /**
     * Extract the current-total of log size in C-Array status.
     */
    static int32_t extract_carray_log_size(carray_status_t current_status) {
        w_assert1(current_status >= 0);
        // bug fixed: lower 32-bit is the size
        //const carray_status_t THREAD_MASK = 0xFFFF;
        const carray_status_t THREAD_MASK = 0xFFFFFFFF;
        return current_status & THREAD_MASK;
    }

    /**
     * Grabs some active slot and \b atomically joins the slot.
     * @param[in] size log size to add
     * @param[out] status \b atomically obtained status of the joined slot
     * @return the slot we have just joined
     */
    CArraySlot*         join_slot(int32_t size, carray_status_t &status);

    /**
     * join the memcpy-complete queue but don't spin yet.
     * This sets the CArraySlot#me2 and CArraySlot#pred2.
     */
    void                join_expose(CArraySlot* slot);

    /**
     * Atomically checks if the slot has a successor slot that delegated its release to this
     * slot, returning the "next" slot to expose. No matter whether there is "next",
     * this slot is atomically freed from the expose chain.
     * See Section A.3 of Aether paper.
     * @return NULL if no one delegated, a delegated slot otherwise.
     */
    CArraySlot*         grab_delegated_expose(CArraySlot* slot);

    /**
     * Spins until the leader of the given slot acquires log buffer.
     * @pre current thread is not the leader of the slot
     */
    void                wait_for_leader(CArraySlot* slot);

    /**
     * Tries to delegate the buffer release of this slot to slowly-moving predecessor
     * if there is.
     * @return true if we successfully delegated our dirty work to the poor predecessor.
     * @pre current thread is the leader of the slot
     */
    bool                wait_for_expose(CArraySlot* slot);

    /**
     * Retire the given slot from active slot, upgrading an unused thread to an active slot.
     * @param[slot] slot to replace
     * @pre current thread is the leader of the slot
     * @pre slot->count > SLOT_AVAILABLE, in other words thte slot is
     * already owned and no other thread can disturb this change.
     */
    void                replace_active_slot(CArraySlot* slot);

private:
    int                 _indexof(const CArraySlot* slot) const;

    /**
     * Clockhand of active slots. We use this to evenly distribute accesses to slots.
     * This value is not protected at all because we don't care even if it's not
     * perfectly even. We anyway atomically obtain the slot.
     */
    int32_t             _slot_mark;
    /** Max number of slots that can be active at the same time. */
    const int32_t       _active_slot_count;
    /** All slots, including available, currently used, or retired slots. */
    CArraySlot          _all_slots[ALL_SLOT_COUNT];
    /** Active slots that are (probably) up for grab or join. */
    CArraySlot**        _active_slots;

    // paddings to make sure mcs_lock are in different cacheline
    /** @cond */ char   _padding[CACHELINE_SIZE]; /** @endcond */
    /**
     * \brief Lock to protect threads releasing their log buffer.
     */
    mcs_lock            _expose_lock;
};

inline int ConsolidationArray::_indexof(const CArraySlot* info) const {
    return info - _all_slots;
}

#endif // LOG_CARRAY_H
