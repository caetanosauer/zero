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

/** @file file.h
 *
 *  @brief RAII class for C file system calls
 *
 *  @recourse http://en.wikipedia.org/wiki/Resource_Acquisition_Is_Initialization
 */

#ifndef __RAII_FILE_H
#define __RAII_FILE_H


#include <cstdlib> 
//#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <sys/types.h>


class file {
private:
    std::FILE* _file_handle ;
 
    // copy and assignment not implemented; prevent their use by
    // declaring them private.
    file( const file & ) ;
    file & operator=( const file & ) ;

public:
    file( const char* filename, const char* mode = "r+" ) : 
        _file_handle(std::fopen(filename, mode)) 
    {
        if( !_file_handle )
            throw std::runtime_error("file open failure") ;
    }

    ~file() {
        if( std::fclose(_file_handle) != 0 ) {
          // TODO: deal with filesystem errors, fclose() may fail when flushing latest changes
        }
    }
 
    void write(const char* str) {
      if( std::fputs(str, _file_handle) == EOF )
        throw std::runtime_error("file write failure") ;
    } 

    char* read(char* dest, size_t length) {
      return (std::fgets(dest, length, _file_handle));
      // TODO: deal with filesystem errors, fclose() may fail when flushing latest changes
    }

    void flush() {
        std::cout << "flushing" << std::endl;
        std::fflush(_file_handle);
    }
          
}; // EOF: file

 
/* // This RAII class can then be used as follows: */
 
/* void example_usage() { */
/*    // open file (acquire resource) */
/*     file logfile("logfile.txt") ; */
 
/*     logfile.write("hello logfile!") ; */
 
/*     // continue using logfile ...    */
/*     // throw exceptions or return without worrying about closing the log; */
/*     // it is closed automatically when logfile goes out of scope. */
/* } */


#endif
