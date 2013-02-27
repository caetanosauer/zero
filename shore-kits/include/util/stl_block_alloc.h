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

#ifndef __STL_BLOCK_ALLOC
#define __STL_BLOCK_ALLOC

#include "block_alloc.h"

//! A standards-compliant pooled allocator.
template<typename T, class Pool=dynpool, size_t MaxBytes=0>
class stl_block_alloc
{
public:
    // we're just a wrapper for this underlying block_pool
    typedef block_pool<T,Pool,MaxBytes> pool_type;
    
    typedef size_t		size_type;			//!< A type that can represent the size of the largest object in the allocation model.
    typedef ptrdiff_t		difference_type;	//!< A type that can represent the difference between any two pointers in the allocation model.
	
    typedef T			value_type;			//!< Identical to T.
    typedef T*			pointer;			//!< Pointer to T;
    typedef T const*		const_pointer;		//!< Pointer to const T.
    typedef T&			reference;			//!< Reference to T.
    typedef T const&		const_reference;	//!< Reference to const T.
  
    //! A struct to construct an allocator for a different type.
    template<typename U> 
    struct rebind { typedef stl_block_alloc<U,Pool,MaxBytes> other; };

    stl_block_alloc() { }
    
    template<typename U, typename P, size_t M>
    stl_block_alloc(stl_block_alloc<U,P,M> const& /*arg*/ ) { }


    //! The largest value that can meaningfully passed to allocate.
    size_type max_size() const { return 1; }

    pointer allocate( size_type /*count*/, std::allocator<void>::const_pointer /*hint*/ = 0 )
    {
	return reinterpret_cast<pointer>(_pool.acquire());
    }

    void deallocate( pointer block, size_type /*count*/ ) throw()
    {
	_pool.release(block);
    }

    //! Constructs an element of \c T at the given pointer.
    /*! Effect: new( element ) T( arg )
     */
    void construct( pointer element, T const& arg )
    {
	new( element ) T( arg );
    }

    //! Destroys an element of \c T at the given pointer.
    /*! Effect: element->~T()
     */
    void destroy( pointer element )
    {
	element->~T();
    }

    //! Returns the address of the given reference.
    pointer address( reference element ) const
    {
	return &element;
    }

    //! Returns the address of the given reference.
    const_pointer address( const_reference element ) const
    {
	return &element;
    }

    //! The pool for this allocator.
    pool_type _pool;
};

#include <map>
template<typename Key, typename Value, class Traits=std::less<Key> >
struct map__block_alloc { 
    typedef std::map<Key, Value, Traits, stl_block_alloc<std::pair<Key, Value> > > Type; 
};
template<typename Key, typename Value, class Traits=std::less<Key> >
struct multimap__block_alloc { 
    typedef std::multimap<Key, Value, Traits, stl_block_alloc<std::pair<Key, Value> > > Type; 
};

#include <list>
template<typename Value>
struct list__block_alloc { 
    typedef std::list<Value, stl_block_alloc<Value> > Type; 
};

#include <set>
template<typename Value, typename Traits = std::less<Value> >
struct set__block_alloc { 
    typedef std::set<Value, Traits, stl_block_alloc<Value> > Type; 
};

#include <vector>
template<typename Value>
struct vector__block_alloc { 
    typedef std::vector<Value, stl_block_alloc<Value> > Type; 
};

#endif
