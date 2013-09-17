#include <stdint.h>
#include <atomic>
#include <cassert>
#include <iostream>

class FairRWLatch {
  std::atomic<uint64_t> state;

  FairRWLatch(const FairRWLatch&); //=delete
  void operator = (const FairRWLatch&); //=delete
public:
  FairRWLatch() : state(0ull) {}
  
  enum LatchType { //internal state of the RW latch
    latch_invalid = 0,
    latch_none,
    latch_s, //incremented reader ctr. decrement in order to release
    latch_s_with_write, // latch_s and holding current ticket
    latch_u, //upgrade. s_w_write and u are currently identical
    latch_x, // curr ticket and readers disallowed and no readers 
    latch_x_skipped_upgrade};
  
  bool canRead(LatchType l) {
    switch (l) {
    case latch_s:
    case latch_s_with_write:
    case latch_u:
    case latch_x:
    case latch_x_skipped_upgrade:
      return true;
      break;
    case latch_none:
      return false;
    default:
      //ERROR
      return false;
    }
  }

  bool canWrite(LatchType l) {
  switch(l) {
  case latch_x:
  case latch_x_skipped_upgrade:
    return true;
    break;
  case latch_none:
  case latch_s:
  case latch_s_with_write:
  case latch_u:
    return false;
    break;
  default:
    //ERROR
    return false;
  }
  }


  // State is 
  // intyy_t writer_ticket, intzz_t extrawritestate, intyy_t writerturn,
  // bit readerinline, bit writeheld, intxx_t readercount, intqq_t extrabits

  // Ticket and turn are meant to be self explanitory.  
  //
  // extra state is for opportunistic concurrency control on the
  // reader side (opportunistic operator is safe if ticket==turn to
  // start, and turn and extra state stayed the same).
  //
  // readerinline is if a reader has attempted to grab the write lock
  // so as to prevent reader starvation
  //
  // writeheld does not mean that a write can actually go, but rather
  // that the writer is trying to block out further readers.
  // writeheld and readercount of 0 means that the writer whose turn
  // it is can go.
  //
  // This organization means that 
  // 1) ticket naturaly overflows properly,
  // 2) writerturn only has to special case after e_state gets rather large
  // 3) overflow on readercount will automatically fill writeheld, which
  //    makes additional readers back off.  This isn't perfect, but it slightly
  //    makes it "safer" to accidentally have too many readers.
  // (note: think about if flipping order of r_inline and w_held would help;
  // could make the reader backoff case test both)

  // For the sake of concreteness, presume 0 extra bits, 14 bit reader
  // count, 16 bit writer_ticket and writer_turn, and 16 bits
  // extrawriterstate (64 total if I add correctly)

  const uint64_t w_ticket_mask = 0xFFFF000000000000ull;
  const uint64_t e_state_mask  = 0x0000FFFF00000000ull;
  const uint64_t w_turn_mask   = 0x00000000FFFF0000ull;
  const uint64_t r_inline_mask = 0x0000000000008000ull;
  const uint64_t w_held_mask   = 0x0000000000004000ull;
  const uint64_t r_count_mask  = 0x0000000000003FFFull;

  const uint16_t w_ticket_shift = 48;
  const uint16_t e_state_shift  = 32;
  const uint16_t w_turn_shift   = 16;
  // r_inline and w_held are bools; they need no shift
  const uint16_t r_count_shift  = 0;


  // How things work uncontested
  // Read latch:
  //
  //   Uncontested, readers increment read count to latch, and
  //   decrement to release
  //
  // Write latch:
  //
  //   Uncontested, writers incr ticket, then use old ticket to wait
  //   for turn.  Once turn==their ticket, they have the latch.  They
  //   increment turn to release.
  //
  // Opportunistic reads:
  //
  //   They read the whole state variable, if ticket==turn and
  //   write_held is not set, they consider themselves good to go.  On
  //   release, they reread the state variable; if turn and extra
  //   state are the same, and write_held is not set, then they were
  //   successful.
  //
  // U latch:
  //
  //   The thread gets a read latch (if it doesn't have one already),
  //   and if and only if ticket==turn, increments ticket.  If
  //   ticket!=turn, then it is not possible to get a U.  On upgrade,
  //   the U latch decrements read count sets w_held and locks out
  //   further readers from entering the critical section.  Once there
  //   are no other readers, it has an X latch.
  //
  //
  // How things work under contention:
  //  Read latch: 
  //
  //   If write_held is set, then we are not allowed to acquire a read
  //   latch and must wait for the writer (who is in line) to finish.
  //   We try to have at most one reader "get in line" as a writer;
  //   that reader will get a write lock (and have turn set
  //   appropriately) but won't lock out readers.  Hopefully this
  //   means that worst case we take turns between batches of readers
  //   and writers; that is, while the designated reader has its
  //   latch, all readers can enter the critical section and writers
  //   will queue up.  Once that reader finishes, the readers that
  //   have already entered will be allowed to finish, but all writers
  //   will be able to go 1 by one (in order as per turn) before any
  //   other readers enter the critical section.
  //
  // Write latch:
  //
  //   Once it is the writers turn (as per ticket), they will lock out
  //   further readers from entering the critical section and wait for
  //   the count of readers to go to 0.

  // A few other assumptions and invariants:
  //
  //   Only threads who hold a ticket for which it is their turn may
  //   clear w_held.
  //
  //   The bug of overflowing r_count may occasionally be masked by
  //   overflow setting w_held; when that happens then the overflowing
  //   thread will try to clear w_held.  The resulting fault may be
  //   masked, we hope.  The fault of overflow of r_count clearing
  //   w_held we hope will also usually be masked; really, try not to
  //   have more readers than we have counter to track them.
  //
  //   Once w_held is set, no more readers may enter the critical section
  //
  //   Once you grab a ticket, you MUST spin until it is your turn (or
  //   arrange for another thread to do so on your behalf).  If you
  //   get a ticket, you must, once it is your turn, increment turn
  //   eventually.  Hence any abortable acquisition/upgrade to a write
  //   latch can't grab a ticket until it is assured that ticket==turn
  //   and there won't be any interfering readers.
  //
  //   Dealing with deadlock is a higher level concern; this code
  //   doesn't care.
  
  bool isWriteHeld(uint64_t var) {
    return w_held_mask & var;
  }
  
  bool isReaderInline(uint64_t var) {
    return r_inline_mask & var;
  }

  uint64_t readerCount(uint64_t var) {
    return (r_count_mask & var) >> r_count_shift;
  }
  
  uint64_t writerTicket(uint64_t var) {
    return (w_ticket_mask & var) >> w_ticket_shift;
  }
  
  uint64_t writerTurn(uint64_t var) {
    return (w_turn_mask & var) >> w_turn_shift;
  }

  uint64_t writersWaiting(uint64_t var) {
    return writerTicket(var) - writerTurn(var); // Could this be more efficient?
    // Also, could the boolean version be done, and be done more efficiently?
    // E.g. 1 mask, 1 shift, 1 xor, one last mask?
  }

  uint64_t getWriteTicket() {
    uint64_t to_add = 1ull << w_ticket_shift;
    //std::cout << "raw toadd: 0x" << std::hex << to_add << std::dec << std::endl;
    //   std::cout << "old ticket: " << writerTicket(state) << std::endl;
    //std::cout << "old turn: " << writerTurn(state) << std::endl;
    //std::cout << "raw state: 0x" << std::hex << state << std::dec << std::endl;
    uint64_t cached_state = (state += to_add); // state is atomic 
    //std::cout << "retval: " << writerTicket(cached_state-to_add) << std::endl;
    //std::cout << "new ticket: " << writerTicket(state) << std::endl;
    //std::cout << "new turn: " << writerTurn(state) << std::endl;
    //std::cout << "raw state: 0x" << std::hex << state << std::dec << std::endl;

    return writerTicket(cached_state-to_add); // Want old value
  }

  void spinForWrite(uint64_t ticket){
    uint64_t cached_state;
    do {
      cached_state = state; //state is atomic
      // Fancier spinning?  Yield? etc. etc.?
    } while(writerTurn(cached_state) != ticket);    
  }
  void spinForReadersClear(){
    uint64_t cached_state;
    do {
      cached_state = state; //state is atomic
      // Fancier spinning?  Yield? etc. etc.?
    } while(readerCount(cached_state) != 0);    
  }

  void release(LatchType l) {
    switch (l) {
    case latch_s:
      releaseReadLock();
      break;
    case latch_s_with_write:
      releaseReadWWrite();
      break;
    case latch_u:
      releaseReadWWrite();
      break;
    case latch_x:
      releaseWriteLock();
      break;
    case latch_x_skipped_upgrade:
      releaseReadLock();
      break;
    default:
      //Error?
      break;
    }
  }

  void releaseReadWWrite() {
    // Same as releaseWriteLock except we don't bother with w_held at all.
    uint64_t cached_state = state; //state is atomic
    uint64_t to_add = 1ull << w_turn_shift;
    
    if ((cached_state & (e_state_mask | w_turn_mask)) ==
	(e_state_mask | w_turn_mask)) {
      to_add -= e_state_mask;
    }
    
    state += to_add; // state is atomic
  }

  LatchType getWriteLock() {
    uint64_t ticket = getWriteTicket();
    do {
      uint64_t cached_state = state ; //state is atomic
      //std::cout << "writeTurn: " << writerTurn(cached_state)
      //	<< ", ticket:" << ticket << std::endl;
      if (writerTurn(cached_state) == ticket) {
	state |= w_held_mask; // state is atomic
	spinForReadersClear();
	return latch_x;
      } else {
	spinForWrite(ticket);
      }
    } while(1);
    return latch_none;
  }

  void releaseWriteLock() {
    uint64_t cached_state = state; // state is atomic
    uint64_t to_add = 1ull << w_turn_shift;

    // We want to give the next writer an uninturrupted chance if
    // there exists such a writer; hence, we clear w_held only if
    // there are not more writers waiting, and we do so via the atomic
    // add (so it is one step)
    if (! writersWaiting(cached_state)) {
      to_add -= w_held_mask;
    }
    
    //Guard for overflow
    if ((cached_state & (e_state_mask | w_turn_mask)) ==
	(e_state_mask | w_turn_mask)) {
      to_add -= e_state_mask;
    }

    state += to_add; // state is atomic
  }

  LatchType getReadLock() {
    do{
      uint64_t cached_state = state; // state is atomic
      
      if (isWriteHeld(cached_state)) {
	// Here is where one would get in line for the write lock as a
	// reader
	if (!isReaderInline(cached_state)){
	  if(getReadAsWriter()) {
	    return latch_s_with_write;
	  }
	}

	spinForRead();
	continue;
      }
      if (tryGetRead()) {
	return latch_s;
      }
      // if give up condition, break
    } while(1); // Can always go back to the beginning and try again.
    return latch_none;
  }
  
  bool getReadAsWriter() {
    uint64_t cached_state;
    { // Atomic (or and fetch)
      cached_state = state;   // state is atomic
      state |= r_inline_mask; // state is atomic
    }
    if(isReaderInline(cached_state)) {
      return false; // Somebody else got in line as a writer before us
    }
    uint64_t ticket = getWriteTicket();
    spinForWrite(ticket);
    //Yay! it's our turn!.

    // Atomically add ourselves as a reader, and clear the write_held
    // bit. We don't increment turn (we let readers add themselves
    // until we're done; this is a little unfair to writers, buts
    // seems a good balance)
    uint64_t to_add = 1ull << r_count_shift;
    to_add -= r_inline_mask;
    cached_state = state; // state is atomic
    if (isWriteHeld(cached_state)) {
      to_add -= w_held_mask;
    }
    
    state += to_add; // state is atomic
    return true;
  }

  void releaseReadLock() {
    uint64_t to_sub = 1ull << r_count_shift;
    state -= to_sub; // state is atomic
  }
  /*void releaseReadLock() {
    uint64_t to_sub = 1 << r_count_shift;
    state -= to_add; // TODO atomic
    }*/

  void spinForRead(){
    uint64_t cached_state;
    do {
      cached_state = state; // state is atomic
      // Fancier spinning?  Yield? etc. etc.?
    } while(isWriteHeld(cached_state));    
  }

  bool tryGetRead(){
    uint64_t to_add = 1ull << r_count_shift;
    uint64_t cached_state = (state += to_add); // state is atomic
    if(isWriteHeld(cached_state)){  
      state -= to_add; // state is atomic
      return false;
    } else {
      return true;
    }
  }

  LatchType tryUpgradeOrU(LatchType l_in) {
    uint64_t cached_state = state; // state is atomic
    switch(l_in) {
    case latch_x:
    case latch_x_skipped_upgrade:
      return l_in;
      break;
    }
   if(!writersWaiting(cached_state)) {
      // We have a hope of either getting a U or maybe even X directly!
     //oneShotGetImmediateTicket(); //FIXME
     assert(false);
    }

  }
  
};

