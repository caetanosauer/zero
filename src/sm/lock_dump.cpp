/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#define LOCK_CORE_C
#define SM_SOURCE

#include "sm_int_1.h"
#include "kvl_t.h"
#include "lock_s.h"
#include "lock_x.h"
#include "lock_core.h"
#include "lock_bucket.h"

#include "w_strstream.h"
#include <sstream>
#include <Lintel/AtomicCounter.hpp>

/** implementation of dump/output/other debug utility functions in lock_core,lock_m. */

// Obviously not mt-safe:
ostream &
xct_lock_info_t::dump_locks(ostream &out) const
{
    /*
    const lock_request_t         *req;
    const lock_head_t                 *lh;
    request_list_i   iter(my_req_list); // obviously not mt-safe
    while ((req = iter.next())) {
        w_assert9(req->xd == xct());
        lh = req->get_lock_head();
        out << "Lock: " << lh->name
            << " Mode: " << int(req->mode())
            << " State: " << int(req->status()) <<endl;
    }
    */
    return out;
}
ostream& operator<<(ostream &o, const xct_lock_info_t &) { return o; }

void
lock_core_m::assert_empty() const
{
    int found_request=0;

    for (uint h = 0; h < _htabsz; h++)   {
        // empty queue is fine. just check leftover requests
        for (lock_queue_t *queue = _htab[h]._queue; queue != NULL; queue = queue->next()) {
            for (lock_queue_entry_t *req = queue->_head; req != NULL; req = req->_next) {
                ++found_request;
                DBGOUT1("leftover lock request(hash=" << queue->hash() << "):" << *req);
            }
        }
    }
    w_assert1(found_request == 0);
}


void lock_core_m::dump(ostream &o) {
    o << " WARNING: Dumping lock table. This method is thread-unsafe!!" << std::endl;
    lintel::atomic_signal_fence(lintel::memory_order_acquire); // memory barrier
    for (uint h = 0; h < _htabsz; h++)  {
        // empty queue is fine. just check leftover requests
        for (lock_queue_t *queue = _htab[h]._queue; queue != NULL; queue = queue->next()) {
            for (lock_queue_entry_t *req = queue->_head; req != NULL; req = req->_next) {
                o << "lock request(hash=" << queue->hash() << "):" << *req
                << ", observed_release_version=" << req->get_observed_release_version()
                << ", queue's release version=" << queue->_release_version
                << std::endl;
            }
        }
    }
    o << "--end of lock table--" << std::endl;
}

ostream&
operator<<(ostream& o, const lock_queue_entry_t& r)
{
    o << "xct:" << r._li.tid()
      << " granted-mode:" << r._granted_mode
      << " req-mode:" << r._requested_mode
      << " thr:" << r._thr.get_fingerprint_map()
      << " wait-map:" << r._li.get_wait_map();

    return o;
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
    stid_t s;
    i.extract_stid(s);
    o << "L(" << s << ": key-hash=" << i.l[1];
    return o << ')';
}
