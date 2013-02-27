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

#ifndef __TUPLE_FIFO_DIRECTORY_H
#define __TUPLE_FIFO_DIRECTORY_H

#include "util.h"


ENTER_NAMESPACE(qpipe);


DEFINE_EXCEPTION(TupleFifoDirectoryException);


class tuple_fifo_directory_t {

private:
    
    enum dir_state_t {
        TUPLE_FIFO_DIRECTORY_FIRST,
        TUPLE_FIFO_DIRECTORY_OPEN,
        TUPLE_FIFO_DIRECTORY_CLOSED
    };

    static c_str _dir_path;

    static pthread_mutex_t _dir_mutex;
    
    static dir_state_t _dir_state;
    
public:

    static const c_str& dir_path();
    static void open_once();
    static void close();
    static c_str generate_filepath(int id);
    
private:

    static bool filename_filter(const char* path);
    static void clean_dir();

};


EXIT_NAMESPACE(qpipe);


#endif
