/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define LOCK_CORE_C
#define SM_SOURCE

#include "sm_base.h"
#include "lock_s.h"
#include "lock_core.h"
#include "lock_raw.h"

#include "w_strstream.h"
#include <sstream>
#include <AtomicCounter.hpp>

/** implementation of dump/output/other debug utility functions in lock_core,lock_m. */

void
lock_core_m::assert_empty() const
{
    int found_request=0;

    for (uint h = 0; h < _htabsz; h++)   {
        // empty queue is fine. just check leftover requests
        for (MarkablePointer<RawLock> lock = _htab[h].head.next;
             !lock.is_null(); lock = lock->next) {
            ++found_request;
            DBGOUT1("leftover lock request(h=" << h << "):" << *lock.get_pointer());
        }
    }
    w_assert1(found_request == 0);
}


void lock_core_m::dump(ostream &o) {
    o << " WARNING: Dumping lock table. This method is thread-unsafe!!" << std::endl;
    lintel::atomic_signal_fence(lintel::memory_order_acquire); // memory barrier
    for (uint h = 0; h < _htabsz; h++)  {
        // empty queue is fine. just check leftover requests
        for (MarkablePointer<RawLock> lock = _htab[h].head.next;
             !lock.is_null(); lock = lock->next) {
            o << "lock request(h=" << h << "):" << *lock.get_pointer()
            << std::endl;
        }
    }
    o << "--end of lock table--" << std::endl;
}

/*********************************************************************
 *
 *  operator<<(ostream, lockid)
 *
 *  Pretty print a lockid to "ostream".
 *
 *********************************************************************/
ostream&
operator<<(ostream& o, const lockid_t& i)
{
    o << "L(" << i.store() << ": key-hash=" << i.l[1];
    return o << ')';
}
