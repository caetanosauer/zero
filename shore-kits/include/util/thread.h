/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
                       Copyright (c) 2007-2009
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

#ifndef __THREAD_H
#define __THREAD_H


// pthread.h should always be the first include!
#include <pthread.h>
#include <cstdio>

#include "k_defines.h"

#include <cerrno>
#include <cassert>
#include <functional>
#include <cstdarg>
#include <stdint.h>
#include <time.h>

#include "util/c_str.h"
#include "util/exception.h"
#include "util/randgen.h"


DEFINE_EXCEPTION(ThreadException);


#ifdef __spacrv9
// Macro that tries to bind a thread to a specific CPU
#define TRY_TO_BIND(cpu,boundflag)                                      \
    if (processor_bind(P_LWPID, P_MYID, cpu, NULL)) {                   \
       TRACE( TRACE_CPU_BINDING, "Cannot bind to processor (%d)\n", cpu);  \
       boundflag = false; }                                             \
    else {                                                              \
    TRACE( TRACE_CPU_BINDING, "Binded to processor (%d)\n", cpu);       \
    boundflag = true; }

#else

// No-op
#define TRY_TO_BIND(cpu,boundflag)              \
    TRACE( TRACE_DEBUG, "Should bind me to (%d)\n", _prs_id);
    
#endif

#ifndef __GCC
//using std::rand_r;
#endif

pthread_mutex_t thread_mutex_create(const pthread_mutexattr_t* attr=NULL);
void thread_mutex_lock(pthread_mutex_t &mutex);
void thread_mutex_unlock(pthread_mutex_t &mutex);
void thread_mutex_destroy(pthread_mutex_t &mutex);


pthread_cond_t thread_cond_create(const pthread_condattr_t* attr=NULL);
void thread_cond_destroy(pthread_cond_t &cond);
void thread_cond_signal(pthread_cond_t &cond);
void thread_cond_broadcast(pthread_cond_t &cond);
void thread_cond_wait(pthread_cond_t &cond, pthread_mutex_t &mutex);
bool thread_cond_wait(pthread_cond_t &cond, pthread_mutex_t &mutex,
                           struct timespec &timeout);
bool thread_cond_wait(pthread_cond_t &cond, pthread_mutex_t &mutex,
		      int timeout_ms);

template <class T>
T* thread_join(pthread_t tid) 
{
    // the union keeps gcc happy about the "type-punned" pointer
    // access going on here. Otherwise, -O3 could break the code.
    union {
        void *p;
        T *v;
    } u;

    if(pthread_join(tid, &u.p))
        THROW(ThreadException);

    return u.v;
}


/***********************************************************************
 *
 *  @struct thread_pool
 * 
 *  @brief  Structure that represents a pool of (worker) threads
 *
 *  @note   A request of a new thread is either granted immediately or 
 *          the requestor has to block until notified (by the conditional
 *          variable).
 *
 ***********************************************************************/


struct thread_pool 
{
    pthread_mutex_t _lock;
    pthread_cond_t _cond;
    int _max_active;		// how many can be active at a time?
    int _active;		// how many are actually active?

    thread_pool(int max_active)
	: _lock(thread_mutex_create()),
	  _cond(thread_cond_create()),
	  _max_active(max_active), _active(0)
    {
    }

    void start();
    void stop();

}; // EOF: thread_pool


/** define USE_SMTHREAD_AS_BASE if there is need the base thread class 
 *  to derive from the smthread_t class (Shore threads)
 */
#define USE_SMTHREAD_AS_BASE

#ifdef USE_SMTHREAD_AS_BASE
#include "sm_vas.h"
#endif


/***********************************************************************
 *
 *  @class thread_t
 * 
 *  @brief shore-mt-client thread base class. Basically a thin wrapper around an
 *         internal method and a thread name.
 *
 *  @note  if USE_SMTHREAD_AS_BASE is defined it uses the smthread_t class
 *         as base class (for Shore code execution) 
 *
 ***********************************************************************/

#ifdef USE_SMTHREAD_AS_BASE
class thread_t : public smthread_t 
#else
class thread_t
#endif
{
private:
    c_str        _thread_name;
    randgen_t    _randgen;

#ifdef USE_SMTHREAD_AS_BASE
    void run(); /** smthread_t::fork() is going to call run() */
    thread_pool* _ppool;
    void setuppool(thread_pool* apool) { _ppool = apool; }
    void setupthr();
#endif    

protected:
    bool _delete_me;

public:

    /** The previously used run() is already used by smthread core.
     *  Thus, run() now does the thread_t specific setup and
     *  calls work(). That is, work() is the new entry function for
     *  thread_t instead of run().
     */
    virtual void work()=0; 
    
    bool delete_me() { return _delete_me; }
    
    c_str thread_name() {
        return _thread_name;
    }


    /**
     *  @brief Returns a pseudo-random integer between 0 and RAND_MAX.
     */
    void reset_rand();


    /**
     *  @brief Returns pointer to thread_t's randgen_t object.
     */
    randgen_t* randgen() {
        return &_randgen;
    }


    /**
     *  @brief Returns a pseudo-random integer between 0 and RAND_MAX.
     */
    int rand() {
        return _randgen.rand();
    }


    /**
     * Returns a pseudorandom, uniformly distributed int value between
     * 0 (inclusive) and the specified value (exclusive).
     *
     * Source http://java.sun.com/j2se/1.5.0/docs/api/java/util/Random.html#nextInt(int)
     */
    int rand(int n) {
        return _randgen.rand(n);
    }

    virtual ~thread_t() { }
    

protected:    
    thread_t(const c_str &name);
       
}; // EOF: thread_t


#ifdef USE_SMTHREAD_AS_BASE
void wait_for_sthread_clients(sthread_t** threads, int num_thread_ids);
#endif


/**
 *  @brief wraps up a class instance and a member function to look
 *  like a thread_t. Use the convenience function below to
 *  instantiate.
 */

template <class Class, class Functor>
class member_func_thread_t : public thread_t {
    Class *_instance;
    Functor _func;
public:
    member_func_thread_t(Class *instance, Functor func, const c_str &thread_name)
        : thread_t(thread_name),
          _instance(instance),
          _func(func)
    {
    }
    
    virtual void work() {
        _func(_instance);
    }
};



/**
 *  @brief Helper function for running class member functions in a
 *  pthread. The function must take no arguments and return a type
 *  compatible with (void).
 */

template <class Return, class Class>
thread_t* member_func_thread(Class* instance,
                             Return (Class::*mem_func)(),
                             const c_str& thread_name)
{
    typedef std::mem_fun_t<Return, Class> Functor;
    return new member_func_thread_t<Class, Functor>(instance,
                                                    Functor(mem_func),
                                                    thread_name);
}



// exported functions

void      thread_init(void);
thread_t* thread_get_self(void);
pthread_t thread_create(thread_t* t, thread_pool* p=NULL);

#if 0 // superceded by thread_local.h
template<typename T>
struct thread_local {
    pthread_key_t _key;
    
    // NOTE:: the template is here because there because I am not
    // aware of a way to pass 'extern "C" function pointers to a C++
    // class member function. Fortunately the compiler can still
    // instantiate a template taking 'extern "C"' stuff... However,
    // this makes a default constructor argument of NULL ambiguous, so
    // we have to have two constructors instead. Yuck.
    template<class Destructor>
    thread_local(Destructor d) {
	pthread_key_create(&_key, d);
    }
    thread_local() {
	pthread_key_create(&_key, NULL);
    }
    ~thread_local() {
	pthread_key_delete(_key);
    }

    T* get() {
	return reinterpret_cast<T*>(pthread_getspecific(_key));
    }
    void set(T* val) {
	int result = pthread_setspecific(_key, val);
	THROW_IF(ThreadException, result);
    }
    

    thread_local &operator=(T* val) {
	set(val);
	return *this;
    }
    operator T*() {
	return get();
    }
};
#endif
extern __thread thread_pool* THREAD_POOL;


#endif  /* __THREAD_H */
