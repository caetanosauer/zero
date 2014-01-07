#ifndef QSXMUTEX_HPP
#define QSXMUTEX_HPP
#include <inttypes.h>
#include "Lintel/AtomicCounter.hpp"

class QSXMutex {
  typedef uint64_t rwcount_t;
private:
  const static rwcount_t minwriter = rwcount_t(1) << 31;
  const static rwcount_t wlockmask = 2*minwriter-1;
  const static rwcount_t rlockmask = minwriter-1;

  lintel::Atomic<rwcount_t> rwcount; // lower bits numreaders, upper bits - seqnum
  QSXMutex(const QSXMutex&);         // =delete (unimpemented)
  void operator=(const QSXMutex&);   // =delete (unimpemented)
public:
  typedef rwcount_t TicketQ;
  typedef rwcount_t TicketS;
  typedef rwcount_t TicketX;

  QSXMutex() : rwcount(2*minwriter) {} // start from 2. 0 is an invalid Ticket/Seqnum
  ~QSXMutex() {}

  TicketX acquireX() {
    rwcount_t seq0 = rwcount;
    while (seq0 & wlockmask || // no readers or writer
	   !rwcount.compare_exchange_strong(&seq0, seq0+minwriter)) {} //weak is ok
    return seq0+minwriter;
  }
  
  TicketX tryAcquireX() { 
    rwcount_t seq0 = rwcount;
    return 
      seq0 & wlockmask && // no readers or writer
      rwcount.compare_exchange_strong(&seq0, seq0+minwriter) //weak is ok
      ? seq0+minwriter : TicketX(false);
  }

  bool releaseX(const TicketX /*t*/) {
    return rwcount+=minwriter; // can be memory_order_relaxed. optimize "seq == seq0 + 2";
  }

  TicketS acquireS() {
    while (rwcount++ & minwriter) { // yes writer
      rwcount--; // TODO: maybe convert to CAS
    }
    return wlockmask & rwcount; // no writer
  }

  TicketS tryAcquireS() {
    if (rwcount++ & minwriter) { // yes writer
      rwcount--; // TODO: maybe convert to CAS
      return TicketS(false);
    } else {
      return wlockmask & rwcount; // no writer
    }
  }

  bool releaseS(const TicketS) {
    rwcount--;
    return true;
  }

  TicketQ acquireQ() {
    // optimistic. Don't wait for writers if any.
    return rwcount.load(/*lintel::memory_order_acquire*/);
  }

  TicketQ tryAcquireQ() {
    return acquireQ();
  }

  bool releaseQ(const TicketQ seq0) {
    //compiler barrier. memory_order_acquire is not a typo.
    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    return !(seq0 & minwriter) && 
      rwcount.load(/*lintel::memory_order_relaxed*/) == seq0; // TODO: ignore readers
  }

  // Upgrade to higher mode -- Functions in form of tryUpgradeAB.  Caller promises they have
  // rights in mode A, and they want to get rights in mode B.  If the return value is true, they
  // will have successfully relinquished the previous rights in exchange for the new without any
  // other threads having taken an incompatable latch mode to the starting mode inbetween (e.g.,
  // S to X guarantees no other thread gets X inbetween).  If false, the caller still has their
  // original rights; it is likely that subsequent calls will still fail.  Note that this can
  // return false spurriously for subtle memory-order related reasons.  Callers must deal with an
  // implementation which is simply "return false".
  //
  // This call may block, but if it blocks it will have the same deadlock properties as the
  // matching acquire call (e.g., acquireX will block forever is someone else calls acquireS
  // without a matching releaseS).
  //
  // Only one of the two tickets will be valid after return, and newTicket's validity is the same
  // as the return value.  (TBD -- maybe the old ticket could still be used for a reaquire?)
  //
  // Users must be able to deal with implementations which always return false.
  TicketS tryUpgradeQS(const TicketQ /*t*/) {
    return TicketS(false); //unimplemented
  }
    
  TicketX tryUpgradeQX(const TicketQ /*t*/) {
    return TicketX(false); //unimplemented
  }    
    
  TicketX tryUpgradeSX(const TicketS /*t*/) {
    rwcount_t seq0 = rwcount;
    if(!rwcount.compare_exchange_strong(&seq0, seq0+minwriter)) {
      // another try_rdunlock_and_lock() is ahead of us
      return TicketX(false);
    } else { //wait for other readers to release their rdlocks
      rwcount_t expected = seq0+minwriter; //no writer or readers, except us
      while(!rwcount.compare_exchange_strong(&expected, expected-1)) {
	expected=seq0+minwriter;
      }
      return seq0+minwriter;
    }
  }

  // Downgrade to lower mode -- Functions in form of tryDowngradeAB.  Caller promises that they
  // have rights in mode A, and they want to instead have rights in mode B.  If the return value
  // is true, they will have successfully relinquished the previous rights in exchange for the
  // new without any other threads having taken an incompatable latch mode to the starting mode
  // inbetween (e.g., X to S guarantees no other thread gets X inbetween, or S for that matter).
  // If false, the caller still has their original rights.  This may return false spurriously,
  // and callers must also deal with an implementation which is simply "return false".
  //
  // This call may block, and it has the same deadlock properties as aquiring the matching
  // acquire call.
  //
  // Only one of the two tickets will be valid after the return, and newTicket's validity is the
  // same as the return value.
  //
  // Users must be able to deal with implementations which always return false.
  TicketS tryDownGradeXS(const TicketX /*t*/) {
    return rwcount+=(minwriter+1);
  }

  TicketQ tryDownGradeXQ(const TicketX /*t*/) {
    return rwcount+=(minwriter+1);
  }

  TicketQ tryDownGradeSQ(const TicketS /*t*/) {
    return rwcount+=minwriter;    
  }

  // Reacquire rights which had been released -- If you give a previously valid ticket for a
  // particular mode, the internal implementation will try to figure out if any other threads had
  // acquired the latch in an incompatible mode since the ticket was issued.  Upon a return of
  // true, the caller will hold the latch in the indicated mode, and can treat it as if they had
  // held that mode continuously.
  //
  // Callers should not use these methods unless they have called release using that ticket.
  //
  // Users must be able to deal with implementations which always return false.
  bool reaquireQ(TicketQ t) {
    return t == acquireQ();
  }

  bool reaquireS(TicketS /*t*/) {
    return false;
  }    

  bool reaquireX(TicketX /*t*/) {
    return false;
  }
    
  // TODO:
  //
  // API to allow statically asserting if an implementation has a "useful" implementation of
  // any particular method or mode.
};
#endif
