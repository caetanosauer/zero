#include "w_defines.h"

#include "w.h"
#include "atomic_container.h"
#include <iostream>
#include "gtest/gtest.h"

#if W_DEBUG_LEVEL > 3
bool     verbose(true);
#else // W_DEBUG_LEVEL
bool     verbose(false);
#endif // W_DEBUG_LEVEL

int tries(100);

typedef unsigned long u_type;

struct  THING {
// offset 0
        THING *_me;
// offset 8/4
        char  dummy[43]; // odd number
// offset 52/48
        int    _i;
// offset 56/??
        THING * _next;
// offset 64/??
        char  dummy2[10]; // odd number again
// offset 64/??
        char  dummy3;    // not aligned 
        int  dummy4;    // aligned 
public:
        THING(int i) : _i(i) { _me = this; }

        int check(int i)  const {  
                if(i != _i) {
                    cerr << "int: expected " << ::dec << i << " found " << _i << endl;
                    return 1;
                }
                if(_me != this) {
                    union {  const THING *p; u_type x; }u; u.p  =  this;
                    union {  const THING *p; u_type x; }m; m.p= _me;

                    cerr << "ptr: expected " << ::hex << u.x
                      << " found " << m.x
                      << ::dec
                      << endl;
                    return 1; 

                 }
                 return 0; 
            }
        int check()  const {  
                if(_me != this) {
                    union {  const THING *p; u_type x; }u; u.p  =  this;
                    union {  const THING *p; u_type x; }m; m.p= _me;

                    cerr << "ptr: expected " << ::hex << u.x
                      << " found " << m.x
                      << ::dec
                      << endl;

                    return 1; 
                 }
                 return 0; 
        }
        int  i() const { return _i; }
};
atomic_container C(w_offsetof(THING, _next));

int* pushed, *popped;

int push1(int i)
{
    THING *v;
    v = new THING(i);
    EXPECT_TRUE(v != NULL);
    EXPECT_TRUE(v->check(i) == 0);
    union {
        void *v;
        u_type u;
    } pun = {v};
    if (verbose) {
        cout << " pushing " << ::hex << pun.u << ::dec << endl;
    }
    C.push(v);
    pushed[i]++;
    return 0;
}

int pop1(int i)
{
    THING *v;
    v = (THING *)C.pop();
    union {
        void *v;
        u_type u;
    } pun = {v};

    if (verbose) {
        cout << " popping " << ::hex << pun.u << ::dec << endl;
    }
    EXPECT_TRUE(v != NULL);
    if(v->check(i)) {
                delete v;
            return 1;
    }  else {
            popped[v->i()]--;
    }
        delete v;
    return 0;
}
int pop1()
{
    THING *v;
// The atomic container should do a slow pop (i.e., pop from the
// backup list) if need be, but should return NULL if  empty
    v = (THING *)C.pop();
    union {
        void *v;
        u_type u;
    } pun = {v};
    if (verbose) {
        cout << " popping " << ::hex << pun.u << ::dec << endl;
    }

    if(!v) return -1;
    if(v->check()) {
                delete v;
       return 1;
    }  else {
                popped[v->i()]--;
    }
        delete v;
    return 0;
}

// push/pop pairs
int test1(int tries)
{
    int e(0);
    for(int i=0; i < tries; i++)
    {
        e += push1(i);
        e += pop1(i);
    }
    cout << "test 1 e=" << e << endl;
    return e;
}

// (push2, /pop1) then pop all
int test2(int tries)
{
    int e(0);

    for(int i=0; i < tries; i++)
    {
        e += push1(i*2);
        e += push1(i*3);
        e += pop1();
    }
    cout << "test 2 e= " << e << endl;
    return e;
}

// pop all.
int  test3(int /*tries*/)
{
    int e(0);
    int f(0);

    // pop1() returns -1 if empty
    // The atomic container should do a slow pop (i.e., pop from the
    // backup list) if need be, but should return NULL if  empty
    while( (f = pop1()) >=0 ) 
    {
            e += f;
    }
    cout << "test 3 e=" << e << endl;
    return e;
}

TEST(ContainerTest, All) {
    cout << "OFFSET IS " << C.offset() << endl;
    cout 
        << "w_offsetof(THING, _me) " 
        << w_offsetof(THING, _me)  << endl;
    cout 
        << "w_offsetof(THING, dummy) " 
        << w_offsetof(THING, dummy)  << endl;
    cout 
        << "w_offsetof(THING, _i) " 
        << w_offsetof(THING, _i)  << endl;
    cout 
        << "w_offsetof(THING, _next) " 
        << w_offsetof(THING, _next)  << endl;
    cout 
        << "w_offsetof(THING, dummy2) " 
        << w_offsetof(THING, dummy2)  << endl;
    cout 
        << "w_offsetof(THING, dummy3) " 
        << w_offsetof(THING, dummy3)  << endl;
    cout 
        << "w_offsetof(THING, dummy4) " 
        << w_offsetof(THING, dummy4)  << endl;
    cout << "SIZEOF THING  IS " << sizeof(THING) << endl;

    cout << "tries " << tries << endl;

    pushed = new int[tries*3];
    popped = new int[tries*3];
    for(int i=0; i < tries*3; i++) pushed[i] = popped[i] = 0;
    EXPECT_EQ(test1(tries), 0);
    EXPECT_EQ(test2(tries), 0);
    EXPECT_EQ(test3(tries), 0);
    delete[] pushed;
    delete[] popped;
}
