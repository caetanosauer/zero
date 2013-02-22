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

#include <cstdio>

#include "qpipe/core/tuple.h"
#include "util.h"



ENTER_NAMESPACE(qpipe);



malloc_page_pool malloc_page_pool::_instance;



static size_t default_page_size = 8192;
static bool initialized = false;

void set_default_page_size(size_t page_size) {
    assert(!initialized);
    initialized = true;
    default_page_size = page_size;
}

size_t get_default_page_size() { return default_page_size; }



bool page::read_full_page(int fd) {
    
    /* create an aligned array of bytes we can read into */
    void* aligned_base;
    guard<char> ptr =
        (char*)aligned_alloc(page_size(), 512, &aligned_base);
    assert(ptr != NULL);


    /* read bytes over 'this' */
    /* read system call may return short counts */
    ssize_t size_read = rio_readn(fd, aligned_base, page_size());


    /* check for error */
    if (size_read == -1)
        THROW2(FileException, "::read failed %s", strerror(errno));

    
    /* check for end of file */
    if (size_read == 0)
        return false;


    /* rio_readn ensures we read the proper number of bytes */
    /* save page attributes that we'll be overwriting */
    page_pool* pool  = _pool;
    memcpy(this, aligned_base, size_read);
    _pool = pool;

    
    /* more error checking */
    if ( (page_size() != (size_t)size_read) ) {
        /* The page we read does not have the same size as the
           page object we overwrote. Luckily, we used the object
           size when reading, so we didn't overflow our internal
           buffer. */
        TRACE(TRACE_ALWAYS,
              "Read %zd byte-page with internal page size of %zd bytes. "
              "Sizes should all match.\n",
              size_read,
              page_size());
        THROW1(FileException, "::read read wrong size page");
    }
    
    return true;
}



void page::write_full_page(int fd) {

    /* create an aligned copy of ourselves */
    void* aligned_base;
    guard<char> ptr =
        (char*)aligned_alloc(page_size(), 512, &aligned_base);
    assert(ptr != NULL);
    memcpy(aligned_base, this, page_size());


    ssize_t write_count = rio_writen(fd, aligned_base, page_size());
    if ((size_t)write_count != page_size()) {
        TRACE(TRACE_ALWAYS, "::write(%d, %p, %zd) returned %zd: %s\n",
              fd,
              aligned_base,
              page_size(),
              write_count,
              strerror(errno));
        THROW2(FileException, "::write() failed %s", strerror(errno));
    }
}



bool page::fread_full_page(FILE* file) {

    // save page attributes that we'll be overwriting
    size_t size = page_size();
    TRACE(0&TRACE_ALWAYS, "Computed page size as %d\n", (int)size);
    page_pool* pool = _pool;

    // write over this page
    size_t size_read = ::fread(this, 1, size, file);
    _pool = pool;
    
    // Check for error
    if ( (size_read == 0) && !feof(file) )
        THROW2(FileException, "::fread failed %s", strerror(errno));
    
    // We expect to read either a whole page or no data at
    // all. Anything else is an error.
    if ( (size_read == 0) && feof(file) )
        // done with file
        return false;

    // check sizes match
    if ( (size != size_read) || (size != page_size()) ) {
        // The page we read does not have the same size as the
        // page object we overwrote. Luckily, we used the object
        // size when reading, so we didn't overflow our internal
        // buffer.
        TRACE(TRACE_ALWAYS,
              "Read %zd byte-page with internal page size of %zd bytes into a buffer of %zd bytes. "
              "Sizes should all match.\n",
              size_read,
              page_size(),
              size);
        THROW1(FileException, "::fread read wrong size page");
    }

    return true;
}



void page::fwrite_full_page(FILE *file) {
    size_t write_count = ::fwrite(this, 1, page_size(), file);
    if ( write_count != page_size() ) {
        TRACE(TRACE_ALWAYS, "::fwrite() wrote %zd/%zd page bytes %s\n",
              write_count,
              page_size(),
              strerror(errno));
        THROW2(FileException, "::fwrite() failed %s", strerror(errno));
    }
}



EXIT_NAMESPACE(qpipe);
