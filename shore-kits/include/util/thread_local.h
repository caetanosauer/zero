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

#ifndef __THREAD_LOCAL_H
#define __THREAD_LOCAL_H

#if 0
struct thread_local_base {
    typedef void (*deleter)(void*);

private:    
    static void register_object(void* ptr, deleter d);
    
};

template<class T>
class thread_local : thread_local_base {
public:
    /* these classes and constructors allow us to bind 0-3 arguments
       that will be passed to T's constructor each time a thread
       requests its thread-local value the first time.
     */
    struct constructor {
	virtual T* construct()=0;
	virtual ~constructor() { }
    };

    struct constructor0 : constructor {
	virtual T* construct() { return new T; }
    };
    
    template<class T1>
    struct constructor1 : constructor {
	T1 _p1;
	constructor1(T1 p1) : _p1(p1) { }
	virtual T* construct() { return new T(_p1); }
    };
    template<class T1, class T2>
    struct constructor2 : constructor {
	T1 _p1;
	T2 _p2;
	constructor2(T1 p1, T2 p2) : _p1(p1), _p2(p2) { }
	virtual T* construct() { return new T(_p1, _p2); }
    };
    template<class T1, class T2, class T3>
    struct constructor3 : constructor {
	T1 _p1;
	T2 _p2;
	T3 _p3;
	constructor3(T1 p1, T2 p2, T3 p3) : _p1(p1), _p2(p2), _p3(p3) { }
	virtual T* construct() { return new T(_p1, _p2, _p3); }
    };
        
    // when a thread exits, all its thread-local variables must be deleted
    static void delete_ptr(void* ptr) { delete (T*) ptr; }
    
    constructor* _c;

    // no-nos
    void operator=(thread_local &other);
    thread_local(thread_local &other);
    
public:
    /*
      NOTE 1: All memory referenced by constructor parameters must
      remain valid for the life of the program unless the parameters
      have appropriate (deep) copy constructors.
      
       NOTE 2: These constructors all pass their arguments by
       value. This shouldn't matter because the copying only happens
       when a tls is declared, not when a instance is created for a
       thread.

       If T's constructor takes a reference to some object that must
       not be copied, the parameter can be replaced with an assignable
       and copy-constructible wrapper struct having an operator A&()
       -- C++ type conversions will kick in automagically from
       there.

       Or, better yet, just change the constructor to take a pointer ;)
       
	struct wrapper {
	    A const* _ref;
	    wrapper(A* ref) : _ref(ref) { }
	    operator A const&() { return *_ref; }
	}
     */
    thread_local() : _c(new constructor0<T>) { }
    
    template<class T1>
    thread_local(T1 p1) : _c(new constructor1<T, T1>(p1)) { }
    
    template<class T1, class T2>
    thread_local(T1 p1, T2 p2) : _c(new constructor2<T, T1, T2>(p1, p2)) { }

    template<class T1, class T2, class T3>
    thread_local(T1 p1, T2 p2, T3 p3) : _c(new constructor3<T, T1, T2, T3>(p1, p2, p3)) { }

    /* no operator-> because TLS is not (necessarily) free and we want
       the programmer to make a local copy of the pointer
     */
    //     operator T*() { return get(); }
    T* get() {
	
	static __thread T* _tls = NULL;
	if(_tls)
	    return _tls;

	T* ptr = _c->construct();
	register_object(ptr, &delete_ptr);
	return _tls = ptr;
    }
    T &operator=(T const &other) { return *get() = other; }
    operator T&() { return *get(); }
};

#endif
#endif
