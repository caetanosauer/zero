#include "w_defines.h"
#include "w.h"
// definition of sthread_t needed before including w_hashing.h
struct sthread_t
{
    static uint64_t rand();
};
typedef unsigned short uint16_t;

#include "w_hashing.h"
#include "w_findprime.h"
#include "rand48.h"
#include <iostream>
#include "gtest/gtest.h"
#include <errlog_s.h>

// MUST be initialized before the hashes below.
static __thread rand48 tls_rng = RAND48_INITIALIZER;

uint64_t sthread_t::rand() { return tls_rng.rand(); }

int tries(50);
enum {H0, H1, H2, H3, H4, H5, H6, H7, H8, H9, H10, HASH_COUNT};
uint32_t        _hash_seeds[HASH_COUNT];
int hash_count(H2);

#if W_DEBUG_LEVEL > 3
bool debug(true);
bool prnt(true);
#else // W_DEBUG_LEVEL
bool debug(false);
bool prnt(false);
#endif // W_DEBUG_LEVEL

int64_t     _size(1024);
int64_t     _prime_replacement(1024);
bool use_prime(true);

struct bfpid_t
{
    uint16_t _vol;
    uint   _store;
    uint   page;

    bfpid_t (uint16_t v, uint s, uint p) :
                _vol(v), _store(s), page(p) {}

    uint16_t vol() const { return _vol; }
    uint store() const { return _store; }
};
ostream &
operator << (ostream &o, const bfpid_t &p)
{
            o << p._vol << "." << p._store << "." << p.page ;
            return o;
}

unsigned 
hash(int h, bfpid_t const &pid) 
{
    if(debug) cout << "h=" << h << " pid=" << pid << endl;
    EXPECT_GE(h, 0);
    EXPECT_LT(h, (int) HASH_COUNT); // this cast is required to avoid "an unnamed type..." error in suncc
    unsigned x = pid.vol();
    if(debug) cout << " x= " << x << endl;
    x ^= pid.page; // XOR doesn't do much, since
                   // most of the time the volume is the same for all pages
    if(debug) cout << " x= " << x << endl;
    EXPECT_LT(h, (int) HASH_COUNT);

    unsigned retval = w_hashing::uhash::hash64(_hash_seeds[h], x);

    if(debug) cout << " retval= " << retval << endl;
    retval %= unsigned(_size);
    if(debug) cout << " retval= " << retval << endl;
    return retval;
}

#define DUMP2(i) \
    if(prnt) cout << endl \
        << "\t" \
        << ::hex \
            << _hash_seeds[i] \
            << "=_hash2["<<::dec<<i<<::hex<<"].a.a" \
        << endl

void dump()
{
    if(prnt)
    {
        cout << "HASH 1  tables"  << endl;
    }
    for(int i=0; i < HASH_COUNT; i++) {
        DUMP2(i);
    }
}
    

void testit() {
    ::srand (4344); // for repeatability
    for (int i = 0; i < HASH_COUNT; ++i) {
        _hash_seeds[i] = ((uint32_t) ::rand() << 16) + ::rand();
    }
    
    
    
    cout << "tries=" << tries << endl;

    dump();

#define page_sz 8192ull

    int64_t nbufpages=(_size * 1024 - 1)/page_sz + 1; // sm.cpp passes to
            // bf_m constructor
    int64_t nbuckets=(16*nbufpages+8)/9; // bf_core_m constructor

    int64_t _prime = w_findprime(int64_t(nbuckets));
    if(!use_prime) { // override
        _prime = _prime_replacement;
    }

    cout << "For requested bp size "<< _size << ": "
    << endl
    << "\t nbufpages=" << nbufpages << " of page size " << page_sz << ","
    << endl
    << "\t nbuckets (from bf_core_m constructor)="  << nbuckets << "," 
    << endl;
    if(use_prime) {
        cout << "\t use prime " ;
    }
    cout 
        << " _size=" << _prime 
    << endl;

    _size = _prime;

    int *buckets = new int[_size];
    for(int i=0; i < _size; i++)
    { buckets[i]=0; }

    for(int i=0; i < tries; i++)
    {
#define START 0
        int j= START + i;

        bfpid_t p(1,5,j);
        for (int k=0; k < hash_count; k++)
        {
            unsigned h=::hash(k,p);
            if(prnt) {
                cout << ::hex << "0x" << h  << ::dec
                <<  "  \t "  << p
                <<  " hash # " << k ;
                if(buckets[int(h)] > 0) cout << " XXXXXXXXXXXXXXXXXXXX ";
                cout << endl;
            }

            buckets[int(h)]++;
        }
                    
    }

    int worst=0;
    int ttl=0;
    for(int i=0; i < _size; i++)
    {
        int collisions=buckets[i]-1;
        if(collisions > 0) {
            ttl += collisions;
            if(collisions > worst) worst=collisions;
        }
    }
    cout << "SUMMARY: buckets: " << _size
        << endl
        << " tries: " << tries << "(x " << hash_count
        << " hashes=" << tries*hash_count << ");  "
        << endl
        << " collisions: " << ttl
        << " ( " << 100*float(ttl)/float(tries*hash_count) << " %) "
        << endl
        << " worst case (max bkt len): " << worst
        << endl;

    delete[] buckets;

}


TEST (CuckooTest, Test1) {
    tries = 1000;
    _size = 1024;
    testit();
}
TEST (CuckooTest, Test2) {
    tries = 200;
    _size = 2048;
    testit();
}
TEST (CuckooTest, Test3) {
    tries = 100;
    _size = 512;
    testit();
}
