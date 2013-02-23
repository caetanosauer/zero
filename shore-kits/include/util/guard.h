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

#ifndef __UTIL_GUARD_H
#define __UTIL_GUARD_H

#include <cstdio>
#include <cassert>
#include <cstddef>
#include <unistd.h>
#include "util/trace.h"
#include "util/exception.h"

// see http://src.opensolaris.org/source/xref/sfw/usr/src/cmd/gcc/gcc-3.4.3/gcc/testsuite/g++.old-deja/g++.abi/align.C
// Origin: Alex Samuel <samuel (at) codesourcery.com>
#define alignmentof(type) (alignment_of<type>())
template<typename T>
inline size_t alignment_of() {
    struct S {
	char c;
	T t;
    };
    return offsetof(S, t);
}

inline void test_alignment(void const* ptr, int align) {
    union {
	void const* p;
	size_t i;
    } u;
    u.p = ptr;
    
    // make sure the requested alignment is a power of 2
    assert((align & -align) == align);
    
    // make sure the pointer is properly aligned
    assert(((align-1) & u.i) == 0);
}

template<class T>
inline T* aligned_cast(void const* ptr) {
    union {
	void const* p;
	T* t;
	size_t i;
    } u;
    u.p = ptr;

    // make sure the pointer is properly aligned
    assert(((alignmentof(T)-1) & u.i) == 0);
    return u.t;
}

/**
 * @brief A generic RAII guard class.
 *
 * This class ensures that the object it encloses will be properly
 * disposed of when it goes out of scope, or with an explicit call to
 * done(), whichever comes first.
 *
 * This class is much like the auto_ptr class, other than allowing
 * actions besides delete upon destruct. In particular it is *NOT
 * SAFE* to use it in STL containers because it does not fulfill the
 * Assignable concept.
 *
 * TODO: make generic and configurable (ie, use templates to determine
 * what "null" and "action" are)
 *
 */
template <typename T>
class guard {
private:
    T* _ptr;

private:
    // Specialize this function as necessary

    void action(T* ptr) {
        delete ptr;
    }

    
    /* possible change 
    void action(T* ptr) {
       if (ptr != NULL)
          delete ptr;
    }
    */

public:
    
    guard(T* ptr=NULL)
        : _ptr(ptr)
    {
    }
    guard(guard &other)
        : _ptr(other.release())
    {
    }

    /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
     * WARNING:
     *
     * The copy constructor must be declared public in order for
     * initializations of the form "guard<foo> f = ..." to compile
     * (even though the function never actually gets called).
     *
     * That said, this function's body cannot be safely defined
     * because there is no way for a const guard to release() its
     * pointer. By declaring -- but not defining -- the copy
     * constructor and its matching assignment operator we prevent
     * multiple guards from accidentally fighting over the same
     * pointer. The only remaining case is where the user
     * independently creates two guards for the same pointer (not much
     * we can do about that).
     */
    guard(guard const &);
    guard &operator =(const guard &);
    /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
    
    guard &operator =(guard &other) {
        assign(other.release());
        return *this;
    }
    guard &operator =(T* ptr) {
        assign(ptr);
        return *this;
    }

    operator T*() {
        return get();
    }
    operator T const*() const {
        return get();
    }

    T &operator *() {
        return *get();
    }
    T const &operator *() const {
        return *get();
    }
    
    T* operator ->() {
        return get();
    }
    T const* operator ->() const {
        return get();
    }
    
    T* get() {
        return _ptr;
    }
    T const* get() const {
        return _ptr;
    }
    
    /**
     * @brief Notifies this guard that its services are no longer
     * needed because some other entity has assumed ownership of the
     * pointer.
     *
     * NOTE: this function is marked const so that the assignment
     * operator and copy constructor can work properly. 
     */
    T* release() {
        T* ptr = _ptr;
        _ptr = NULL;
        return ptr;
    }

    /**
     * @brief Notifies this guard that its action should be performed
     * now rather than at destruct time.
     */
    void done() {
        assign(NULL);
    }

    ~guard() {
        done();
    }
    
private:
    void assign(T* ptr) {
        if(_ptr && _ptr != ptr)
            action(_ptr);
        _ptr = ptr;
    }
    
};

template<>
inline void guard<FILE>::action(FILE* ptr) {
    if(fclose(ptr))
        TRACE(TRACE_ALWAYS, "fclose failed");
}


/**
 * @brief A generic pointer guard base class. An action will be
 * performed exactly once on the stored pointer, either with an
 * explicit call to done() or when the guard goes out of scope,
 * whichever comes first.
 *
 * This class is much like the auto_ptr class, other than allowing
 * actions besides delete upon destruct. In particular it is *NOT
 * SAFE* to use it in STL containers because it does not fulfill the
 * Assignable concept.
 *
 * This class *DOES NOT* support being cast as "const" in the
 * traditional sense. Other than disallowing use of the assignment
 * operator '=' it always allows modification of its contents so that
 * it do its job properly. This should not be a problem in practice
 * because a "const T" (with T = <pointer to some type>, eg int*) does
 * not actually protect a pointer's contents from modification
 * anyway. Use T = <pointer to some const type> for that, eg const
 * int*.
 *
 */
template <class T, class Impl>
class guard_base_t {
protected:
    mutable T _obj;

public:
    typedef guard_base_t<T, Impl> Base;

    // The value that represents "null" for typename T. Impl usually
    // descends from this class so this makes a handy default
    // implementation
    static T null_value() {
        return NULL;
    }
    static bool different(const T &a, const T &b) {
        return a != b;
    }
    
    guard_base_t(T obj)
        : _obj(obj)
    {
    }
    guard_base_t(const guard_base_t &other)
        : _obj(other.release())
    {
    }
    guard_base_t &operator =(const guard_base_t &other) {
        assign(other.release());
        return *this;
    }
    
    operator T() const {
        return get();
    }

    T get() const {
        return _obj;
    }
    
    /**
     * @brief Notifies this guard that its services are no longer
     * needed because some other entity has assumed ownership of the
     * pointer.
     *
     * NOTE: this function is marked const so that the assignment
     * operator and copy constructor can work properly. 
     */
    T release() const {
        T obj = _obj;
        _obj = Impl::null_value();
        return obj;
    }

    /**
     * @brief Notifies this guard that its action should be performed
     * now rather than at destruct time.
     */
    void done() const {
        assign(Impl::null_value());
    }

    ~guard_base_t() {
        done();
    }
    
protected:
    void assign(T obj) const {
        if(Impl::different(_obj, obj)) {
            // free the old object and set it NULL
            Impl::guard_action(_obj);
            _obj = obj;
        }
    }
    
};

template <typename T, typename Impl>
struct pointer_guard_base_t : guard_base_t<T*, Impl> {
    typedef guard_base_t<T*, Impl> Base;
    pointer_guard_base_t(T* ptr)
        : Base(ptr)
    {
    }
    T &operator *() const {
        return *Base::get();
    }
    operator T*() const {
        return Base::get();
    }
    T* operator ->() const {
        return Base::get();
    }
};


/**
 * @brief Ensures that a pointer allocated with a call to operator
 * new() gets deleted
 */
template <class T>
struct pointer_guard_t : pointer_guard_base_t<T, pointer_guard_t<T> > {
    pointer_guard_t(T* ptr=NULL)
        : pointer_guard_base_t<T, pointer_guard_t<T> >(ptr)
    {
    }
    static void guard_action(T* ptr) {
        delete ptr;
    }
};



/**
 * @brief Ensures that an array allocated with a call to operator
 * new() gets deleted
 */
template <class T>
struct array_guard_t : pointer_guard_base_t<T, array_guard_t<T> > {
    array_guard_t(T *ptr=NULL)
        : pointer_guard_base_t<T, array_guard_t<T> >(ptr)
    {
    }
    static void guard_action(T *ptr) {
        delete [] ptr;
    }
};



/**
 * @brief Ensures that a file gets closed
 */
struct file_guard_t : pointer_guard_base_t<FILE, file_guard_t> {
    file_guard_t(FILE *ptr=NULL)
        : pointer_guard_base_t<FILE, file_guard_t>(ptr)
    {
    }
    static void guard_action(FILE *ptr) {
        if(ptr && fclose(ptr)) {
            TRACE(TRACE_ALWAYS, 
                  "fclose failed in guard destructor");
        }
    }
};

/**
 * @brief Ensures that a file descriptor gets closed
 */
struct fd_guard_t : guard_base_t<int, fd_guard_t> {
    fd_guard_t(int fd)
        : guard_base_t<int, fd_guard_t>(fd)
    {
    }
    static void guard_action(int fd) {
        if(fd >= 0 && close(fd)) 
            TRACE(TRACE_ALWAYS, "close() failed in guard destructor");
    }
    static int null_value() {
        return -1;
    }
};


#endif // __UTIL_GUARD_H

