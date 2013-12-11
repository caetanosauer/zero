#include "w_defines.h"
/*
 * If possible compile this _without_ optimization; so the 
 * register variables are really put in registers.
 */

#include <cassert>
#include "w.h"
#include "sthread.h"

#include <iostream>
#include <sstream>
#include <iomanip>
#include "gtest/gtest.h"

__thread stringstream *_safe_io(NULL);
void safe_io_init() 
{ 
        if (_safe_io==NULL) _safe_io = new stringstream; 
}

#define SAFE_IO(XXXX) { safe_io_init(); \
        *_safe_io <<  XXXX; \
        fprintf(stdout, "%s", _safe_io->str().c_str()); }

class float_thread_t : public sthread_t {
public:
        float_thread_t(int fid);
protected:
        virtual void run();
private:
        int id;
};

class int_thread_t : public sthread_t {
public:
        int_thread_t(int fid);
protected:
        virtual void run();
private:
        int id;
};

float_thread_t::float_thread_t(int fid)
: id(fid)
{
        w_ostrstream_buf s(40);         // XXX magic number
        s << "float[" << id << "]" << ends;
        rename(s.c_str());
}
    

int_thread_t::int_thread_t(int fid)
: id(fid)
{
        w_ostrstream_buf s(40);         // XXX magic number
        s << "int[" << id << "]" << ends;
        rename(s.c_str());
}


int     NumFloatThreads = 4;
int     NumIntThreads = 4;
bool    mix_it_up = false;
bool    verbose = false;

int     *ack;
sthread_t **worker;

void harvest(int threads)
{
        int     i;

        for(i=0; i < threads; ++i){
                EXPECT_FALSE( worker[i]->join().is_error() );
                if (verbose) {
                        SAFE_IO( "Finished: " << endl << *worker[i] << endl);
                }

                EXPECT_TRUE(ack[i] != 0);
                delete worker[i];
        }
}

TEST (ThreadTest3, All) {
    int i;
    int     threads;

    if (mix_it_up)
            threads = NumFloatThreads + NumIntThreads;
    else
            threads = NumFloatThreads > NumIntThreads ?
                        NumFloatThreads : NumIntThreads;

    ack = new int[threads];
    EXPECT_TRUE (ack != NULL);
    worker = new sthread_t *[threads];
    EXPECT_TRUE (worker != NULL);

    for (i=0; i<NumIntThreads; ++i) {
            ack[i] = 0;
            worker[i] = new int_thread_t(i);
            EXPECT_TRUE (worker[i] != NULL);
            EXPECT_FALSE( worker[i]->fork().is_error() );
    }

    if (!mix_it_up)
            harvest(NumIntThreads);

    int     base = mix_it_up ? NumIntThreads : 0;
    
    for(i=base ; i < base + NumFloatThreads; ++i){
            ack[i] = 0;
            worker[i] = new float_thread_t(i);
            EXPECT_TRUE (worker[i] != NULL);
            EXPECT_FALSE( worker[i]->fork().is_error());
    }
    harvest(mix_it_up ? threads : NumFloatThreads);

    delete [] worker;
    delete [] ack;
}

void float_thread_t::run()
{
    float result;
    register float r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13,
        r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25,
        r26, r27, r28, r29, r30, r31;

    r1 = (float) id;
    r2 = (float) id;
    r3 = (float) id;
    r4 = (float) id;
    r5 = (float) id;
    r6 = (float) id;
    r7 = (float) id;
    r8 = (float) id;
    r9 = (float) id;
    r10 = (float) id;
    r11 = (float) id;
    r12 = (float) id;
    r13 = (float) id;
    r14 = (float) id;
    r15= (float) id;
    r16= (float) id;
    r17= (float) id;
    r18= (float) id;
    r19= (float) id;
    r20 = (float) id;
    r21 = (float) id;
    r22 = (float) id;
    r23 = (float) id;
    r24 = (float) id;
    r25 = (float) id;
    r26 = (float) id;
    r27 = (float) id;
    r28 = (float) id;
    r29 = (float) id;
    r30 = (float) id;
    r31 = (float) id;

    result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
        r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
        r26+ r27+ r28+ r29+ r30+ r31;

    SAFE_IO ( "Hello, world from " << id 
         << ", result = " 
         << setprecision(6) << setiosflags(ios::fixed) 
         << result << resetiosflags(ios::fixed)
         << ", check = " << 31 * id << endl
    )
    sthread_t::yield();

    result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
        r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
        r26+ r27+ r28+ r29+ r30+ r31;

    SAFE_IO( "Hello, world from " << id 
         << ", result = " << setprecision(6) << setiosflags(ios::fixed) 
         << result << resetiosflags(ios::fixed)
         << ", check = " << 31 * id << endl;)

    sthread_t::yield();
    ack[id] = 1;
}

void int_thread_t::run()
{
    int result;
    register int r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13,
        r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25,
        r26, r27, r28, r29, r30, r31;

    r1 = id;
    r2 = id;
    r3 = id;
    r4 = id;
    r5 = id;
    r6 = id;
    r7 = id;
    r8 = id;
    r9 = id;
    r10 = id;
    r11 = id;
    r12 = id;
    r13 = id;
    r14 = id;
    r15= id;
    r16= id;
    r17= id;
    r18= id;
    r19= id;
    r20 = id;
    r21 = id;
    r22 = id;
    r23 = id;
    r24 = id;
    r25 = id;
    r26 = id;
    r27 = id;
    r28 = id;
    r29 = id;
    r30 = id;
    r31 = id;

    result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
        r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
        r26+ r27+ r28+ r29+ r30+ r31;

    SAFE_IO( "Hello, world from " << id 
         << ", result = " << result
         << ", check = " << 31 * id << endl;)
    sthread_t::yield();

    result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
        r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
        r26+ r27+ r28+ r29+ r30+ r31;
    SAFE_IO( "Hello, world from " << id 
         << ", result = " << result
         << ", check = " << 31 * id << endl;)
    sthread_t::yield();
    ack[id] = 1;
}

