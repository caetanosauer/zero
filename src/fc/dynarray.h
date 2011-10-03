/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
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
/*<std-header orig-src='shore' incl-file-exclusion='DYNARRAY_H'>

 $Id: dynarray.h,v 1.2 2010/06/23 23:42:57 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef __DYNARRAY_H
#define __DYNARRAY_H

/**\cond skip */

#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <algorithm>

/* A memory-mapped array which exploits the capabilities provided by
   mmap in order to grow dynamically without moving existing data or
   wasting memory.

   Ideal for situations where you don't know the final size of the
   array, the potential maximum is known but very large, and a
   threaded environment makes it unsafe to resize by reallocating.

   NOTE: the array only supports growing, under the assumption that
   any array which can shrink safely at all can shrink safely to size
   zero (with the data copied to a new, smaller dynarray)

   This approach makes the most sense in a 64-bit environment where
   address space is cheap.

   Note that most systems cannot reserve more than 2-8TB of
   contiguous address space (32-128TB total), most likely because most
   machines don't have that much swap space anyway.

 */
struct dynarray {
    
    /* Attempts to initialize the array with a capacity of /max_size/ bytes
       of address space and /size()/ zero.

       If /align/ is a non-zero power of two, the resulting memory
       will be aligned as requested.

       @return 0 on success, appropriate errno on failure
     */
    int init(size_t max_size, size_t align=0);

    /* Attempts to make a deep copy of /to_copy/, setting my capacity
       to the larger of /to_copy.capacity()/ and /max_size/

       @return 0 on success, appropriate errno on failure
     */
    int init(dynarray const &to_copy, size_t max_size=0);

    /* Destroys the existing mapping, if any, and returns the object
       to its uninitialized state
     */
    int fini();

    /* The reserved size of this mapping. The limit is set at
       initialization and cannot change later.
     */
    size_t capacity() const { return _capacity; }

    /* Maps in memory to bring the total to /new_size/ bytes. 

       @return 0, or an appropriate errno on failure
       EINVAL - new_size < size()
     */
    int resize(size_t new_size);

    /* Ensures that at least /new_size/ bytes are ready to use.

       In order to ensure array management is O(1) work per insertion,
       this function will always at least double the size of the array
       if /new_size > size()/.

       Unlike /resize/ this function accepts any value of /new_size/
       (doing nothing if the array is already big enough).

       @return 0 or an errno.
     */
    int ensure_capacity(size_t min_size);

    /* The currently available size. Assuming sufficient memory is
       available the array can grow to /capacity()/ bytes -- using
       calls to /resize()/.
     */
    size_t size() const { return _size; }
    
    operator char*() { return _base; }
    operator char const*() const { return _base; }

    dynarray() : _base(0), _size(0), _capacity(0) { }
    
private:
    // only safe if we're willing to throw exceptions (use init() and memcpy() instead)
    dynarray(dynarray const &other);
    dynarray &operator=(dynarray const &other);
    
    char* _base;
    size_t _size;
    size_t _capacity;
};



/* Think std::vector except backed by a dynarray.

 */
template<typename T>
struct dynvector {
    
    /* Initialize an empty dynvector with /limit() == max_count/

       @return 0 on success or an appropriate errno
     */
    int init(size_t max_count) {
	return _arr.init(count2bytes(max_count));
    }

    /* Destroy all contained objects and deallocate memory, returning
       the object to its uninitialized state.

       @return 0 on success or an appropriate errno
     */
    int fini() {
	for(size_t i=0; i < _size; i++)
	    (*this)[i].~T();

	_size = 0;
	return _arr.fini();
    }

    /* The largest number of elements the underlying dynarray instance
       can accommodate
     */
    size_t limit() const {
	return bytes2count(_arr.capacity());
    }

    /* The current capacity of this dynvector (= elements worth of
       allocated memory)
     */
    size_t capacity() const {
	return bytes2count(_arr.size());
    }

    /* The current logical size of this dynvector (= elements pushed
       so far)
     */
    size_t size() const {
	return _size;
    }

    /* Ensure space for the requested number of elements.

       Spare capacity is managed automatically, but this method can be
       useful if the caller knows the structure will grow rapidly or
       needs to ensure early on that all the needed capacity is
       available (e.g. Linux... overcommit.. OoM killer).

       @return 0 on success or an appropriate errno
    */
    int reserve(size_t new_capacity) {
	return _arr.ensure_capacity(count2bytes(new_capacity));
    }

    /* Default-construct objects at-end (if needed) to make /size() == new_size/

       @return 0 on success or an appropriate errno
     */
    int resize(size_t new_size) {
	if(int err=reserve(new_size))
	    return err;

	for(size_t i=size(); i < new_size; i++)
	    new (_at(i).c) T;
	
	_size = new_size;
	return 0;
    }

    /* Add /obj/ at-end, incrementing /size()/ by one

       @return 0 on success or an appropriate errno
     */
    int push_back(T const &obj) {
	size_t new_size = _size+1;
	if(int err=reserve(new_size))
	    return err;

	new (_at(_size).c) T(obj);
	_size = new_size;
	return 0;
    }

    T &back() { return this->operator[](size()-1); }
    T const &back() const { return this->operator[](size()-1); }

    /* Returns the ith element of the array; it is the caller's
       responsibility to ensure the index is in bounds.
     */
    T &operator[](size_t i) { return *_at(i).t; }
    T const &operator[](size_t i) const { return *_at(i).tc; }

    dynvector() : _size(0), _align_offset(0) { }
    
private:
    union ptr { char const* cc; T const* tc; char* c; T* t; };
    static size_t count2bytes(size_t count) { return count*sizeof(T); }
    static size_t bytes2count(size_t bytes) { return bytes/sizeof(T); }

    ptr _at(size_t idx) const {
	ptr rval = {_arr + count2bytes(idx)};
	return rval;
    }
    
    dynarray _arr;
    size_t _size; // element count, not bytes!!
    size_t _align_offset;
};


/**\endcond skip */
#endif
