#ifndef BLOCK_ALLOC_H
#define BLOCK_ALLOC_H

/**\cond skip */
#include "dynarray.h"
#include "mem_block.h"
//#include "tatas.h"

// for placement new support, which users need
#include <new>
#include <w.h>
#include <stdlib.h>
#include <deque>

/* Forward decls so we can do proper friend declarations later
 */
template<class T, size_t MaxBytes>
class block_alloc;

template<class T, size_t MaxBytes>
inline
void* operator new(size_t nbytes, block_alloc<T, MaxBytes> &alloc);

template<class T, size_t MaxBytes>
inline
void operator delete(void* ptr, block_alloc<T, MaxBytes> &alloc);

// a basic block_pool backed by a dynarray
struct dynpool : public memory_block::block_pool {
    typedef memory_block::block mblock;
    pthread_mutex_t    _lock;
    //tatas_lock         _lock;
    dynarray           _arr;
    std::deque<mblock*> _free_list;
    size_t        _chip_size;
    size_t        _chip_count;
    size_t        _log2_block_size;
    size_t        _arr_end;

    NORET         dynpool(size_t chip_size, size_t chip_count,
                size_t log2_block_size, size_t max_bytes);
    
    virtual
    NORET        ~dynpool();
    
    virtual
    bool         validate_pointer(void* ptr);

protected:

    size_t       _size() const;

    mblock*      _at(size_t i);
    
    virtual
    mblock*      _acquire_block();

    virtual
    void         _release_block(mblock* b);
    
};

/** \brief A factory for speedier allocation from the heap.
 *
 * This allocator is intended for use in a multithreaded environment
 * where many short-lived objects are created and released.
 *
 * Allocations are not thread safe, but deallocations are. This allows
 * each thread to allocate objects cheaply, without worrying about who
 * will eventually deallocate them (they must still be deallocated, of
 * course).
 * To use: give each thread its own allocator: that provides the thread-safety.
 *
 * The factory is backed by a global dynarray which manages
 * block-level allocation; each block provides N chips to hand out to
 * clients. The allocator maintains a cache of blocks just large
 * enough that it can expect to recycle the oldest cached block as
 * each active block is consumed; the cache can both grow and shrink
 * to match demand.
 *
 * PROS:
 * - most allocations are extremely cheap -- no malloc(), no atomic ops
 * - deallocations are also cheap -- one atomic op
 * - completely immune to the ABA problem 
 * - memory can be redistributed among threads between bursts

 * CONS:
 *
 * - each thread must have its own allocator, which means managing
 *   thread-local storage (if compilers ever support non-POD __thread
 *   objects, this problem would go away).
 *
 * - though threads attempt to keep their caches reasonably-sized,
 *   they will only do so at allocation or thread destruction time,
 *   leading to potential hoarding
 *
 * - memory leaks (or unexpectedly long-lived objects) are extremly
 *   expensive because they keep a whole block from being freed
 *   instead of just one object. However, the remaining chips of each
 *   block are at least available for reuse. 
 */

template<class T, class Pool=dynpool, size_t MaxBytes=0>
struct block_pool 
{
    typedef memory_block::meta_block_size<sizeof(T)> BlockSize;

    // use functions because dbx can't see enums very well
    static size_t block_size() { return BlockSize::BYTES; }
    static size_t chip_count() { return BlockSize::COUNT; }
    static size_t chip_size()  { return sizeof(T); }

    // gets old typing this over and over...
#define TEMPLATE_ARGS chip_size(), chip_count(), block_size()

    static Pool* get_static_pool() {
        static Pool p(chip_size(), chip_count(), BlockSize::LOG2,
              MaxBytes? MaxBytes : 1024*1024*1024);
        return &p;
    }
  
    block_pool()
    : _blist(get_static_pool(), TEMPLATE_ARGS)
    {
    }

    /* Acquire one object from the pool.
     */
    void* acquire() {
        return _blist.acquire(TEMPLATE_ARGS);
    }
    
    /* Verify that we own the object then find its block and report it
       as released. If \e destruct is \e true then call the object's
       desctructor also.
     */
    static
    void release(void* ptr) {
        w_assert0(get_static_pool()->validate_pointer(ptr));
        memory_block::block::release_chip(ptr, TEMPLATE_ARGS);
    }
    
private:
    memory_block::block_list _blist;

};

template<class T, size_t MaxBytes=0>
struct block_alloc {
    
    typedef block_pool<T, dynpool, MaxBytes> Pool;

    static
    void destroy_object(T* ptr) {
        if(ptr == NULL) return;

        ptr->~T();
        Pool::release(ptr);  // static method
        // that releases the ptr into a static pool
        // (member of block_pool) (of type dynpool)
    }
    
private:
    Pool _pool;
    
    // let operator new/delete access alloc()
    friend void* operator new<>(size_t, block_alloc<T,MaxBytes> &);
    friend void  operator delete<>(void*, block_alloc<T,MaxBytes> &);
};

template<class T, size_t MaxBytes>
inline
void* operator new(size_t nbytes, block_alloc<T,MaxBytes> &alloc) 
{
    (void) nbytes; // keep gcc happy
    w_assert1(nbytes == sizeof(T));
    return alloc._pool.acquire();
}

/* No, this isn't a way to do "placement delete" (if only the language
   allowed that symmetry)... this operator is only called -- by the
   compiler -- if T's constructor throws
 */
template<class T, size_t MaxBytes>
inline
void operator delete(void* ptr, block_alloc<T,MaxBytes> & /*alloc*/) 
{
    if(ptr == NULL) return;
    block_alloc<T,MaxBytes>::Pool::release(ptr);
    w_assert2(0); // let a debug version catch this.
}

inline
size_t dynpool::_size() const {
    return _arr_end >> _log2_block_size;
}

inline
dynpool::mblock* dynpool::_at(size_t i) {
    size_t offset = i << _log2_block_size;
    union { char* c; mblock* b; } u = {_arr+offset};
    return u.b;
}



#undef TEMPLATE_ARGS

// prototype for the object cache TFactory...
template<class T>
struct object_cache_default_factory {
    // these first three are required... the template args are optional
    static T*
    construct(void* ptr) { return new (ptr) T; }

    static void
    reset(T* t) { /* do nothing */ }

    static T*
    init(T* t) { /* do nothing */ return t; }
};

template<class T>
struct object_cache_initializing_factory {
    // these first three are required... the template args are optional
    static T*
    construct(void* ptr) { return new (ptr) T; }
    
    static void
    reset(T* t) { t->reset(); }

    static T*
    init(T* t) { t->init(); return t; }

    // matched by object_cache::acquire below, but with the extra first T* arg...
    template<class Arg1>
    static T* init(T* t, Arg1 arg1) { t->init(arg1); return t; }
    template<class Arg1, class Arg2>
    static T* init(T* t, Arg1 arg1, Arg2 arg2) { t->init(arg1, arg2); return t; }    
    template<class Arg1, class Arg2, class Arg3>
    static T* init(T* t, Arg1 arg1, Arg2 arg2, Arg3 arg3) { t->init(arg1, arg2, arg3); return t; }
};

template <class T, class TFactory=object_cache_default_factory<T>, size_t MaxBytes=0>
struct object_cache {
    
    // for convenience... make sure to extend the object_cache_default_factory to match!!!
    T* acquire() {
        return TFactory::init(_acquire());
    }
    template<class Arg1>
    T* acquire(Arg1 arg1) {
        return TFactory::init(_acquire(), arg1);
    }
    template<class Arg1, class Arg2>
    T* acquire(Arg1 arg1, Arg2 arg2) {
        return TFactory::init(_acquire(), arg1, arg2);
    }    
    template<class Arg1, class Arg2, class Arg3>
    T* acquire(Arg1 arg1, Arg2 arg2, Arg3 arg3) {
        return TFactory::init(_acquire(), arg1, arg2, arg3);
    }
    
    T* _acquire() {
    // constructed when its block was allocated...
        union { void* v; T* t; } u = {_pool.acquire()};
        return u.t;
    }

    static
    void release(T* obj) {
        TFactory::reset(obj);
        Pool::release(obj);
    }

private:
    
    struct cache_pool : public dynpool {

        // just a pass-thru...
        NORET cache_pool(size_t cs, size_t cc, size_t l2bs, size_t mb)
            : dynpool(cs, cc, l2bs, mb)
        {
        }
    
        virtual void _release_block(mblock* b);
        virtual mblock* _acquire_block();
        virtual NORET ~cache_pool();
    };

    typedef block_pool<T, cache_pool, MaxBytes> Pool;
    
    Pool _pool;

};

template <class T, class TF, size_t M>
inline
void object_cache<T,TF,M>::cache_pool::_release_block(mblock* b) {
    union { cache_pool* cp; memory_block::block_list* bl; } u={this};
    b->_owner = u.bl;
    dynpool::_release_block(b);
}
    
/* Intercept untagged (= newly-allocated) blocks in order to
   construct the objects they contain.
*/
template <class T, class TF, size_t M>
inline
dynpool::mblock* object_cache<T,TF,M>::cache_pool::_acquire_block() {
    dynpool::mblock* b = dynpool::_acquire_block();
    void* me = this;
    if(me != b->_owner) {
        // new block -- initialize its objects
        for(size_t j=0; j < Pool::chip_count(); j++) 
            TF::construct(b->_get(j, Pool::chip_size()));
        b->_owner = 0;
    }
    return b;
}

/* Destruct all cached objects before going down
 */
template <class T, class TF, size_t M>
inline
NORET object_cache<T,TF,M>::cache_pool::~cache_pool() {
    size_t size = _size();
    for(size_t i=0; i < size; i++) {
        mblock* b = _at(i);
        for(size_t j=0; j < Pool::chip_count(); j++) {
            union { char* c; T* t; } u = {b->_get(j, Pool::chip_size())};
            u.t->~T();
        }
    }
}

/* A pool for holding blobs of bytes whose size is fixed but
   determined at runtime. This class must only be instantiated once
   per Tag.
 */
struct blob_pool
{
    typedef dynpool Pool;
    typedef memory_block::block_list BlockList;

    struct helper;
    blob_pool(size_t size);

    size_t nbytes() const { return _chip_size; }
    
    void* acquire();
    // really release, but for backward compat w/ ats...
    void destroy(void* ptr);
    
private:
    size_t _chip_size;
    size_t _block_size;
    size_t _chip_count;
    Pool _pool;
    
    // no copying allowed
    blob_pool(blob_pool&);
    void operator=(blob_pool&);
};

inline
void* operator new(size_t nbytes, blob_pool &alloc) 
{
    w_assert1(nbytes <= alloc.nbytes());
    return alloc.acquire();
}

/* No, this isn't a way to do "placement delete" (if only the language
   allowed that symmetry)... this operator is only called -- by the
   compiler -- if T's constructor throws
 */
inline
void operator delete(void* ptr, blob_pool &alloc) 
{
    alloc.destroy(ptr);
}



/**\endcond skip */
#endif
