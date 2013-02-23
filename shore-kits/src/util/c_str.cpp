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

#include <stdarg.h>

#include "util/c_str.h"
#include "util/exception.h"
#include "util/sync.h"
#include "util/guard.h"
#include <stdio.h>

#include "k_defines.h"

#include "util/pool_alloc.h"

#undef TRACK_LEAKS

#ifdef TRACK_LEAKS
#include <map>
#include <set>
#include <vector>
typedef std::vector<void*> address_list;
typedef std::map<c_str::c_str_data*, address_list> owner_map;
static pthread_mutex_t owner_mutex = PTHREAD_MUTEX_INITIALIZER;
static owner_map* owners;

#ifdef __SUNPRO_CC

/*
  This declaration is actually inlined asm, not a function. To use it, (on sparc)
  create a file with a .il extension containing the following:

.inline get_caller,0
	mov %i7, %o0
.end

  Manually add that file onto the end of whatever command 'make' uses
  to compile this file, rebuild libutil.ca, and relink. If you
  get errors about discarded symbols, compile c_str.cpp without debug
  info (sorry, I don't know what the real problem is).
 */
extern "C" void* get_caller();
#else
// sorry, out of luck on the caller
void* get_caller() { return NULL; }
#endif
#endif

/**
   HACK: This is "Part B" of the "Swatchz Counter." Allocate a
   owner_map on the heap and assign it to the variable. Because the
   pointer defaults to a compile time constant (NULL), my changes are
   in no danger of getting clobbered when this file's static
   initializers run. For the same reason, swatchz_count will already
   be initialized to 0 before I get here.
 */
static int swatchz_count = 0;
initialize_allocator::initialize_allocator() {
    if(!swatchz_count++) {
#ifdef TRACK_LEAKS
	owners = new owner_map;
#endif
    }
}



// Ignore the warning: printf("") is valid!
const c_str c_str::EMPTY_STRING("%s", "");

struct c_str::c_str_data {
    mutable unsigned long _count;
    char _data[0];
    char* _str() { return _data; }
    DECLARE_POOL_ALLOC_POOL(c_str_data);
};


#ifdef TRACK_LEAKS
void print_live_strings() {
    owner_map::iterator it = owners->begin();
    std::set<void*> uniques;
    for(; it != owners->end(); ++it) {
	address_list::iterator it2 = it->second.begin();
	printf("\"%s\"", it->first->_str());
	uniques.clear();
	for(; it2 != it->second.end(); ++it2) {
	    if(uniques.find(*it2) != uniques.end())
		continue;
	    printf(" @0x%p", *it2);
	    uniques.insert(*it2);
	}
	printf("\n");
    }
	
}
#endif

c_str::c_str(const char* str, ...)
    : _data(NULL)
{

    static int const i = 1024;
    char tmp[i];
    char const* src;
    int count;

    // do we really have to do the va_args thing?
    if(strchr(str, '%')) {
	src = tmp;
	va_list args;
	va_start(args, str);
	count = vsnprintf(tmp, i, str, args);
	va_end(args);
        
	if((count < 0) /* glibc 2.0 */ || (count >= i) /* glibc 2.1 */)
	    THROW(BadAlloc); // oops!
    }
    else {
	src = str;
	count = strlen(str);
    }

    _data = (c_str_data*) c_str_data::pool()->alloc(sizeof(c_str_data) + count + 1);

    _data->_count = 1;
    memcpy(_data->_str(), src, count+1);
    
#if defined(TRACK_LEAKS)
    pthread_mutex_lock(&owner_mutex);
    (*owners)[_data].push_back(get_caller());
    pthread_mutex_unlock(&owner_mutex);
#endif
    
}



const char* c_str::data() const {
    // Return the actual string... whatever uses this string must
    // copy it since it does not own it.
    return _data->_str();
}



void c_str::assign(const c_str &other) {

#if defined(TRACK_LEAKS)
    pthread_mutex_lock(&owner_mutex);
    (*owners)[other._data].push_back(get_caller());
    pthread_mutex_unlock(&owner_mutex);
#endif
    
    // other._data won't disappear because it can't be destroyed before we return.
    _data = other._data;

#ifdef __sparcv9
    membar_enter();
    atomic_inc_64(&_data->_count);
    membar_exit();
#else
    __sync_fetch_and_add(&_data->_count, 1);
#endif
}



void c_str::release() {    
    int count;
#ifdef __sparcv9
    membar_producer();
    count = atomic_dec_64_nv(&_data->_count);
    membar_consumer();
#else
    count = __sync_add_and_fetch(&_data->_count, -1);
#endif    
    if(count == 0) {
        // we were the last reference, so nobody else could
        // possibly modify the data struct any more
            
        if(DEBUG_C_STR)
            printf("Freeing %s\n", _data->_str());
	
#ifdef TRACK_LEAKS
	pthread_mutex_lock(&owner_mutex);
	owners->erase(_data);
	pthread_mutex_unlock(&owner_mutex);
#endif
	c_str_data::pool()->free(_data);
    }
}
