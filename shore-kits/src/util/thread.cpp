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

#include "util/thread.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h> /* Added this for Enceladus. Maybe not work on lomond. */
#include <fcntl.h>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "k_defines.h"

#include <unistd.h>


#undef DISABLE_THREAD_POOL

/* internal datatypes */


struct thread_args {
    thread_t* t;
    thread_pool* p;
    thread_args(thread_t* thread, thread_pool* pool)
	: t(thread), p(pool)
    {
    }
};

   
class root_thread_t : public thread_t 
{
public:
    
    /**
     *  @brief 
     */
    root_thread_t(const c_str &name)
        : thread_t(name)
    {
    }

    virtual void work() {
        // must be overwridden, but never called for the root thread
        assert(false);
    }
};



/* internal helper functions */

extern "C" void thread_destroy(void* thread_object);
extern "C" void* start_thread(void *);
static void setup_thread(thread_args* args);



/* internal data structures */

static __thread thread_t* THREAD_KEY_SELF;
__thread thread_pool* THREAD_POOL;
// the default thread pool effectively has no limit.
static thread_pool default_thread_pool(1<<30);



/* method definitions */

/***********************************************************************
 *
 *  @class thread_t constructor
 * 
 *  @brief thread_t base class constructor. Every subclass constructor
 *  goes through this. Subclass should invoke the thread_t static
 *  initialization functions (init_thread_name() or
 *  init_thread_name_v()) to set up a new thread object's name.
 *
 ***********************************************************************/

thread_t::thread_t(const c_str &name)
#ifdef USE_SMTHREAD_AS_BASE
    : smthread_t(t_regular, name.data()), 
      _thread_name(name), _ppool(NULL), _delete_me(true)
#else
      : _thread_name(name), _delete_me(true)
#endif
{
    // do nothing...
}


void thread_t::reset_rand() {

    /* generate new seed */
    unsigned int new_seed;
    int fd = ::open("/dev/urandom", O_RDONLY);
    assert(fd != -1);
    int read_size = ::read(fd, &new_seed, sizeof(new_seed));
    assert(read_size == sizeof(new_seed));
    ::close(fd);

    /* reset _randgen using new seed */
    _randgen.reset(new_seed);
}


#ifdef USE_SMTHREAD_AS_BASE


/*********************************************************************
 *
 *  @fn:    run
 *  
 *  @brief: Setups the context of a (smthread_t derived) thread_t
 *
 *  @note:  This function is wrapped with the initialization and destroy
 *          of a sthread. 
 *
 *********************************************************************/

void thread_t::run()
{ 
    setupthr(); // Setups the pool and the TLS variables
    
    thread_t* thread  = THREAD_KEY_SELF;
    thread_pool* pool = THREAD_POOL;
    assert (pool);
    pool->start();
    thread->work();    
    pool->stop();
}


void thread_t::setupthr()
{
    if(!_ppool)
	_ppool = &default_thread_pool;

    // past this point the thread should belong to a pool
    assert (_ppool);

    // Register local data. Should not fail since we only need two
    // pieces of thread-specific storage.
    THREAD_KEY_SELF = this;
    THREAD_POOL     = _ppool;
    reset_rand();    
}
#endif


/**
 *  @brief Initialize thread module.
 *
 *  @return void
 */

void thread_init(void)
{
    thread_args args(new root_thread_t(c_str("root-thread")), NULL);
    setup_thread(&args);
}
 


thread_t* thread_get_self(void)
{
    // It would be nice to verify that the name returned is not
    // NULL. However, the name of the root thread can be NULL if we have
    // not yet completely initialized it.
    return THREAD_KEY_SELF;
}


/*********************************************************************
 *
 *  @brief:  Creates a new thread and starts it.
 *
 *  @param:  thread - A pointer to a pthread_t that will store the ID of
 *           the newly created thread.
 *
 *  @param:  t - A thread that contains the function to run.
 *
 *  @return: tid - The thread id of the newly created thread. Needed, 
 *           for the caller to destroy (join) the thread.
 *
 *********************************************************************/

pthread_t thread_create(thread_t* t, thread_pool* pool)
{
    pthread_t tid;
    int err;
    pthread_attr_t pattr;

    // create a new kernel schedulable thread
    err = pthread_attr_init( &pattr );
    THROW_IF(ThreadException, err);
  
    err = pthread_attr_setscope( &pattr, PTHREAD_SCOPE_SYSTEM );
    THROW_IF(ThreadException, err);
  
    err = pthread_create(&tid, &pattr, start_thread, new thread_args(t, pool));
    THROW_IF(ThreadException, err);

    return (tid);
}



pthread_mutex_t thread_mutex_create(const pthread_mutexattr_t* attr)
{
    pthread_mutexattr_t        mutex_attr;
    const pthread_mutexattr_t* ptr_mutex_attr;
    int err;

    if (attr == NULL)
    {
#if 0
        err = pthread_mutexattr_init(&mutex_attr);
        THROW_IF(ThreadException, err);

        err = pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_ERRORCHECK);
        THROW_IF(ThreadException, err);
    
        err = pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_PRIVATE);
        THROW_IF(ThreadException, err);

        ptr_mutex_attr = &mutex_attr;
#else
        (void) mutex_attr; // Keep gcc happy
	ptr_mutex_attr = attr;
#endif
    }
    else {
        ptr_mutex_attr = attr;
    }

    pthread_mutex_t mutex;
    err = pthread_mutex_init(&mutex, ptr_mutex_attr);
    THROW_IF(ThreadException, err);
    return mutex;
}



void thread_mutex_lock(pthread_mutex_t &mutex)
{
    for(int i=0; i < 3; i++) {
	int err = pthread_mutex_trylock(&mutex);
	if(!err)
	    return;
	if(err != EBUSY)
	    THROW_IF(ThreadException, err);
    }

    thread_pool* pool = THREAD_POOL;
    assert (pool);
    pool->stop();
    int err = pthread_mutex_lock(&mutex);
    pool->start();
    THROW_IF(ThreadException, err);
}



void thread_mutex_unlock(pthread_mutex_t &mutex)
{
    int err = pthread_mutex_unlock(&mutex);
    THROW_IF(ThreadException, err);
}



void thread_mutex_destroy(pthread_mutex_t &mutex)
{
    int err = pthread_mutex_destroy(&mutex);
    THROW_IF(ThreadException, err);
}



pthread_cond_t thread_cond_create(const pthread_condattr_t* attr)
{
    pthread_cond_t cond;
    int err = pthread_cond_init(&cond, attr);
    THROW_IF(ThreadException, err);
    return cond;
}



void thread_cond_destroy(pthread_cond_t &cond)
{
    int err = pthread_cond_destroy(&cond);
    THROW_IF(ThreadException, err);
}



void thread_cond_signal(pthread_cond_t &cond)
{
    int err = pthread_cond_signal(&cond);
    THROW_IF(ThreadException, err);
}



void thread_cond_broadcast(pthread_cond_t &cond)
{
    int err = pthread_cond_broadcast(&cond);
    THROW_IF(ThreadException, err);
}



void thread_cond_wait(pthread_cond_t &cond, pthread_mutex_t &mutex)
{
    thread_pool* pool = THREAD_POOL;
    assert (pool);
    pool->stop();
    int err = pthread_cond_wait(&cond, &mutex);
    pool->start();
    THROW_IF(ThreadException, err);
}

bool thread_cond_wait(pthread_cond_t &cond, pthread_mutex_t &mutex,
                           struct timespec &timeout)
{
    thread_pool* pool = THREAD_POOL;
    assert (pool);
    pool->stop();
    int err = pthread_cond_timedwait(&cond, &mutex, &timeout);
    pool->start();
    
    switch(err) {
    case 0: return true;
    case ETIMEDOUT: return false;
    default: THROW_IF(ThreadException, err);
    }

    unreachable();
}

bool thread_cond_wait(pthread_cond_t &cond, pthread_mutex_t &mutex,
                           int timeout_ms)
{
    if(timeout_ms > 0) {
    struct timespec timeout;
    struct timeval now;
    gettimeofday(&now, NULL);
    if(timeout_ms > 1000) {
	timeout.tv_sec = timeout_ms / 1000;
	timeout.tv_nsec = (timeout_ms - timeout.tv_sec*1000)*1000;
    }
    else {
	timeout.tv_sec = 0;
	timeout.tv_nsec = timeout_ms*1000;
    }
    
    return thread_cond_wait(cond, mutex, timeout);
    }
    thread_cond_wait(cond, mutex);
    return true;
}



/*********************************************************************
 *
 *  @fn:     start_thread
 *
 *  @brief:  thread_main function for newly created threads. Receives a
 *           thread_t object as its argument and it calls its run() function.
 *
 *  @param:  thread_object A thread_t*.
 *
 *  @return: The value returned by thread_object->run().
 *
 *********************************************************************/

void* start_thread(void* thread_object)
{
    thread_args* args = (thread_args*)thread_object;
    setup_thread(args);
    delete args;
    
    thread_t* thread  = THREAD_KEY_SELF;
    thread_pool* pool = THREAD_POOL;
    assert (pool);

    pool->start();
    thread->work();    
    pool->stop();
    
    if(thread->delete_me())
	delete thread;
    return (NULL);
}




// NOTE: we can't call thread_xxx methods because they call us
void thread_pool::start() 
{
#ifndef DISABLE_THREAD_POOL
    int err = pthread_mutex_lock(&_lock);
    THROW_IF(ThreadException, err);
    while(_active == _max_active) {
	err = pthread_cond_wait(&_cond, &_lock);
	THROW_IF(ThreadException, err);
    }
    assert(_active < _max_active);
    _active++;
    err = pthread_mutex_unlock(&_lock);
    THROW_IF(ThreadException, err);
#endif
}

void thread_pool::stop() 
{
#ifndef DISABLE_THREAD_POOL
    int err = pthread_mutex_lock(&_lock);
    THROW_IF(ThreadException, err);
    _active--;
    thread_cond_signal(_cond);
    err = pthread_mutex_unlock(&_lock);
    THROW_IF(ThreadException, err);
#endif
}



static void setup_thread(thread_args* args) 
{
    thread_t* thread  = args->t;
    thread_pool* pool = args->p;

    if (!pool) {
	pool = &default_thread_pool;
    }

    // past this point the thread should belong to a pool
    assert (pool);

    // Register local data. Should not fail since we only need two
    // pieces of thread-specific storage.
    THREAD_KEY_SELF = thread;
    THREAD_POOL     = pool;
    thread->reset_rand();
}



#ifdef USE_SMTHREAD_AS_BASE

/*********************************************************************
 *  
 *  @fn:    wait_for_sthread_clients
 *
 *  @brief: Wait for an array of created clients (sthread-derived) threads
 *           to exit. 
 *
 *  @param: threads - An array of sthreads.
 *
 *  @param: num_thread_ids - The number of valid thread IDs in the
 *  thread_ids array.
 *
 *********************************************************************/

void wait_for_sthread_clients(sthread_t** threads, int num_thread_ids) 
{
    // wait for client threads to receive error message
    for (int i = 0; i < num_thread_ids; i++) {
        // join should not really fail unless we are doing
        // something seriously wrong...
        threads[i]->join();
        
        //int join_ret = pthread_join(thread_ids[i], NULL);
        //assert(join_ret == 0);
    }
}

#endif /** USE_SMTHREAD_AS_BASE */
