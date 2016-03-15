#include <iostream>
//#include "util.h"
#include <vector>
#include <thread>
#include <random>
#include <unistd.h>

#include <relacy/relacy_std.hpp>

class QSXMutex {
  typedef uint64_t rwcount_t;
private:
  const static rwcount_t minwriter = rwcount_t(1) << 1;
  const static rwcount_t wmask = 2ULL*minwriter-1ULL;
  const static rwcount_t rmask = ~(minwriter-1ULL);

  std::atomic<rwcount_t> rwcount; // lower bits numreaders, upper bits - seqnum
  QSXMutex(const QSXMutex&);         // =delete (unimpemented)
  void operator=(const QSXMutex&);   // =delete (unimpemented)
public:
  typedef rwcount_t TicketQ;
  typedef rwcount_t TicketS;
  typedef rwcount_t TicketX;

  QSXMutex() : rwcount(2*minwriter) {} // start from 2. 0 is an invalid Ticket/Seqnum
  ~QSXMutex() {}

  void print() {
    //std::clog << "(" << (rwcount>>31) << "," << (~rmask&rwcount) << ")" << std::endl;
  }

  TicketX acquireX() {
    rwcount_t seq0;
    while ( wmask & (seq0=rwcount.load(std::memory_order_acquire)) || // no readers or writer
	    !rwcount.compare_exchange_weak(seq0, seq0+minwriter, std::memory_order_acquire)) { //weak is ok
      //asm("PAUSE\n\t":::); // doesn't seem to help. Can't just stick into non-asm loop...
      //...because compiler generates loop, not recognized by the "PAUSE" convention
    }
    return seq0+minwriter;
  }
  
  TicketX tryAcquireX() { 
    rwcount_t seq0 = rwcount.load(std::memory_order_acquire);
    return 
      seq0 & wmask && // no readers or writer
      rwcount.compare_exchange_strong(seq0, seq0+minwriter, std::memory_order_release) //weak is ok
      ? seq0+minwriter : TicketX(false);
  }

  bool releaseX(const TicketX /*t*/) {
    return rwcount.fetch_add(minwriter, std::memory_order_acq_rel)+minwriter; // can be memory_order_relaxed. optimize "seq == seq0 + 2";
  }

  TicketS acquireS() {
    rwcount_t seq0;
    while ((seq0=rwcount.fetch_add(1, std::memory_order_seq_cst)) & minwriter) { // yes writer
      rwcount.fetch_sub(1, std::memory_order_seq_cst); // TODO: maybe convert to CAS
    }
    
    // while ( minwriter & (seq0=rwcount.load(/*lintel::memory_order_acquire*/)) || // no writer
    // 	    !rwcount.compare_exchange_strong(&seq0, seq0+1)) { //weak is ok
    //   //asm("PAUSE\n\t":::); // doesn't seem to help. Can't just stick into non-asm loop...
    //   //...because compiler generates loop, not recognized by the "PAUSE" convention
    // }
    
    return rmask & seq0;
  }

  TicketS tryAcquireS() {
    if (rwcount.fetch_add(1, std::memory_order_seq_cst) & minwriter) { // yes writer
      rwcount.fetch_sub(1, std::memory_order_seq_cst); // TODO: maybe convert to CAS
      return TicketS(false);
    } else {
      return rmask & rwcount.load(std::memory_order_acquire); // no writer
    }
  }

  bool releaseS(const TicketS) {
    rwcount.fetch_sub(1, std::memory_order_seq_cst);
    return true;
  }

  TicketQ acquireQ() {
    // optimistic. Don't wait for writers if any.
    //std::cout << "acquireQ:"<< (rmask & rwcount.load(std::memory_order_acquire)) << std::endl;
    return rmask & rwcount.load(std::memory_order_seq_cst);
  }

  TicketQ tryAcquireQ() {
    return acquireQ();
  }

  bool releaseQ(const TicketQ seq0) {
    //compiler barrier. memory_order_acquire is not a typo.
    std::atomic_thread_fence(std::memory_order_acquire);
    return !(seq0 & minwriter) && 
      (rwcount.load(std::memory_order_relaxed) - seq0 < minwriter); //ignore readers
  }

};

struct race_test_XS : rl::test_suite<race_test_XS, 4>
{
  QSXMutex m;
  rl::var<int> d=0;

  void before()
  {
  }

  void thread(unsigned index)
  {
    if (index==0 || index==1) {
      QSXMutex::TicketX tX = m.acquireX();
      d($)=0;
      m.releaseX(tX);
    } else if (index==2 || index==3) {
      QSXMutex::TicketS tS = m.acquireS();
      int i = d($);
      m.releaseS(tS);
    }
  }
};


struct race_test_XQ : rl::test_suite<race_test_XQ, 2>
{
  QSXMutex m;
  std::atomic<int> d;

  void before()
  {
    d.store(0, std::memory_order_relaxed);
  }

  void thread(unsigned index)
  {
    if (index==0 /*|| index==1*/) {
      QSXMutex::TicketX tX = m.acquireX();
      d.store(1, std::memory_order_relaxed);
      m.releaseX(tX);
    } else if (index==1 /*|| index==3*/) {
      QSXMutex::TicketS tQ = m.acquireQ();
      int i = d.load(std::memory_order_relaxed);
      int j = d.load(std::memory_order_relaxed);
      if(m.releaseQ(tQ)) {
	RL_ASSERT(i==j);
      }
    }
  }
};


int main()
{
    rl::test_params params;
    params.context_bound = 2;
    params.iteration_count = 10000;

    rl::simulate<race_test_XS>(params);
    std::cout << "RW count: " << params.stop_iteration << std::endl;

    rl::simulate<race_test_XQ>(params);
    std::cout << "RW count: " << params.stop_iteration << std::endl;
}
