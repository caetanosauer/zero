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

/** @file:   stl_pool.cpp
 * 
 *  @brief:  Pool for the Pool allocator
 * 
 *  @author: Ippokratis Pandis, Nov 2008
 *
 *  @note:   Taken from: http://www.sjbrown.co.uk/2004/05/01/pooled-allocators-for-the-stl/
 */

#ifndef __UTIL_STL_POOL
#define __UTIL_STL_POOL

#include <iostream>
#include "util/guard.h"

//! Simple pool class.
class Pool
{
 public:
  //! Allocates a pool with size elements each of granularity bytes.
  Pool( size_t granularity, size_t size );

  //! Checks for emptiness before destructing.
  ~Pool();

  //! Gets the pool granularity.
  size_t GetGranularity() const { return m_granularity; }

  //! Gets the pool size in elements.
  size_t GetSize() const { return m_size; }

  //! Gets the number of elements allocated from pooled storage.
  size_t GetUsed() const { return m_used; }

  //! Gets the number of elements allocated from non-pooled storage.
  size_t GetOverflow() const { return m_overflow; }

	
  //! Allocates memory from the pool without construction.
  void* Allocate();

  //! Deallocates memory from the pool without destruction.
  void Deallocate( void* block );


  //! Constructs an object from the pool.	
  template<typename T>
    T* Construct()
    {
      assert( sizeof( T ) <= m_granularity );
      T* block = reinterpret_cast<T*>( Allocate() );
      return new( block ) T;
    }

  //! Destructs an object back into the pool.
  template<typename T>
    void Destroy( T* instance )
    {
      assert( sizeof( T ) <= m_granularity );
      instance->~T();
      Deallocate( instance );
    }

 private:
  //! Returns true if the given instance is from pooled storage.
  bool IsFromPool( void const* instance ) const
  {
    char const* block = reinterpret_cast<char const*>( instance );
    return m_storage.get() <= block && block < ( m_storage.get() + m_size*m_granularity );
  }

  size_t m_granularity;	//!< The size of each element in the pool in bytes.
  size_t m_size;			//!< The number of elements in pooled storage.
  size_t volatile m_used;			//!< The number of pooled allocations.
  size_t volatile m_overflow;		//!< The number of non-pooled allocations.

  array_guard_t<char> m_storage;	//!< The pool storage.
  array_guard_t<void*> m_slots;		//!< The free list.
};


#endif /** __UTIL_STL_POOL */

