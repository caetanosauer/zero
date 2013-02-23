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

#ifndef __C_STR_H
#define __C_STR_H


#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "k_defines.h"
#include "util/namespace.h"

#include "compat.h"

#define DEBUG_C_STR 0

using namespace std;

/**
   HACK: This is "Part A" of a "Swatchz Counter." It's job is to make
   sure the compiler initializes c_str's internal allocator before
   trying to initialize any statically allocated c_str objects.

   Because we must include the header to use c_str, we guarantee that
   our little abomination will initialize before any static c_str that
   might occur in the .cpp file. The linker will then pick some random
   .o file to initialize first, and the corresponding version of
   swatchz will initialize the allocator. Later .o files will see a
   non-zero counter and return immediately.

   We don't have to make this thing thread safe because all static
   initialization occurs before main(), so we're guaranteed
   single-threaded.
 */
struct initialize_allocator {
    initialize_allocator();
};
static initialize_allocator c_str_swatchz;

class c_str {

    struct c_str_data;
    c_str_data* _data;


    /**
     *  Decrement reference count, freeing memory if count hits 0.
     */
    void release();    
    
    /**
     *  @brief Just increment reference count of other. We are
     *  guaranteed that other will remain allocated until we return
     *  since other contributes exactly 1 to the total reference
     *  count.
     */
    void assign(const c_str &other);    

public:

    static const c_str EMPTY_STRING;


    c_str (const c_str &other=EMPTY_STRING) {
        if (DEBUG_C_STR)
            printf("copy constructor with other = %s\n", other.data());
        assign(other);
    }
    

    // start counting params at 2 instead of 1 -- non-static member function
    c_str(const char* str, ...) ATTRIBUTE(format(printf, 2, 3));
    

    operator const char*() const {
        return data();
    }


    const char* data() const;
    
    
    ~c_str() {
        if (DEBUG_C_STR)
            printf("in c_str destructor for %s\n", data());
        release();
    }
    
    
    c_str &operator=(const c_str &other) {
        if (DEBUG_C_STR)
            printf("in c_str = operator, this = %s, other = %s\n", data(), other.data());
        if(_data != other._data) {
            release();
            assign(other);
        }
        return *this;
    }
    

    bool operator<(const c_str &other) const {
        return strcmp(data(), other.data()) < 0;
    }


    bool operator>(const c_str &other) const {
        return strcmp(data(), other.data()) > 0;
    }

    
    bool operator==(const c_str &other) const {
        return strcmp(data(), other.data()) == 0;
    }
};



#endif
