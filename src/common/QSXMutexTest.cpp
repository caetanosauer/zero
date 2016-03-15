#include <iostream>
#include "util.h"
#include <vector>
#include <thread>
#include <random>

#include "QSXMutex.hpp"

const int num_writers = 4;
const int num_readers = 2;
const int num_threads = num_writers+num_readers;
const int dim = num_threads*8;
std::vector<unsigned> values(dim, 2); const unsigned total=2*dim;
std::vector<QSXMutex> m(dim);

std::atomic<int> active_writers(num_writers);
std::atomic<int> read_locks(0);

void writer(int cpu)
{
  set_cpu_affinity(cpu);

  std::mt19937 rng;
  rng.seed(0);
  std::uniform_int_distribution<int> uint_dist(0,dim-1); // range [0,dim-1]
  std::vector<QSXMutex::TicketX> tX(dim);

  for(int count=0; count<10*1000000/2/num_writers; count++) {
    int i = uint_dist(rng);
    int j = uint_dist(rng);
    if(i==j) { j=(j+1)%dim; }
    int min = std::min(i,j);
    int max = std::max(i,j);
    const int amt = uint_dist(rng);
    tX[min] = m[min].acquireX(); assert(tX[min]);
    tX[max] = m[max].acquireX(); assert(tX[max]);
    values[min]-=amt;
    values[max]+=amt;
    m[min].releaseX(tX[min]); // assert here causes ICE with g++4.[78]
    m[max].releaseX(tX[max]); // assert here causes ICE with g++4.[78]
  }
  active_writers--;
  std::clog << cpu << "w:done" << std::endl;
}

void reader(int cpu)
{
  set_cpu_affinity(cpu);
  std::vector<QSXMutex::TicketS> tS(dim);

  int count = 0;
  do {
    count++;

    for(int i=0; i<dim; ++i) {
      tS[i] = m[i].acquireS(); assert(tS[i]);
    }

    unsigned sum=0;
    for(int i=0; i<dim; ++i) {
      sum+=values[i];
    }
    assert(sum==total);

    for(int i=0; i<dim; ++i) {
      assert(m[i].releaseS(tS[i]));
    }
  } while(active_writers);
  read_locks+=count*dim;

  std::clog << cpu << "r:done " << count*dim/1000000.0 << std::endl;
}

int main() {
  std::vector<std::thread> threads(num_threads);
  
  for (int i = 0; i < num_threads; ++i) {
    threads[i] = std::thread( (i<num_writers ? writer:reader), i);
  }
  
  //Join the threads with the main thread
  for (int i = 0; i < num_threads; ++i) {
    threads[i].join();
  }

  std::clog << "read_locks:" << read_locks/1000000.0 << std::endl;  
  return 0;
}

#if 0
int main()
{
  set_cpu_affinity(6);

  QSXMutex m;

  for(unsigned long long i=0; i<2000*1000*1000; ++i) {
    asm("":::"memory"); //prevent compiler from optimising loop away
    QSXMutex::TicketQ tQ = m.acquireQ();
    if(!m.releaseQ(tQ)) { return 1; }
  }
  return 0;
}
#endif
