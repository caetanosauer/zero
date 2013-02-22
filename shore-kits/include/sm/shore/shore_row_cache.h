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

/** @file:   shore_row_cache.h
 *
 *  @brief:  Cache for tuples (row_impl<>) used in Shore
 *
 *  @note:   row_cache_t           - class for tuple caching
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __SHORE_ROW_CACHE_H
#define __SHORE_ROW_CACHE_H

#include "k_defines.h"
#include "block_alloc.h"
#include "sm/shore/shore_row.h"

ENTER_NAMESPACE(shore);

template <class TableDesc>
class row_cache_t 
{
public:    
    typedef table_row_t table_tuple;

    struct tuple_factory {
	// WARNING: manually assign non-NULL before using the cache... Or Else
	static TableDesc* &ptable() {
	    static TableDesc* _ptable;
	    return _ptable;
	}
	static table_tuple* construct(void* ptr) {
	    return new(ptr) table_tuple(ptable());
	}
	static void destroy(table_tuple* t) { t->~table_tuple(); }

	static void reset(table_tuple* t) {
	    assert (t->_ptable == ptable());
	    t->reset();
	}
	
	// TODO: figure out how to build in the areprow stuff?
        // IP: The areprow stuff should use another block allocator for char*[some-max-size]
	static table_tuple* init(table_tuple* t) { return t; }
    };
    
private:
    typedef object_cache<table_tuple, tuple_factory> Cache;
    Cache	  _cache;

public:

    /* Return an unused object, if cache empty allocate and return a new one
     */
    table_tuple* borrow() { return _cache.acquire(); }

    /* Returns an object to the cache. The object is reset and put on the
     * free list.
     */
    static
    void giveback(table_tuple* ptn) 
    {        
        assert (ptn);
	Cache::release(ptn);
    }
    
}; // EOF: row_cache_t


EXIT_NAMESPACE(shore);


#endif /* __SHORE_ROW_CACHE_H */
