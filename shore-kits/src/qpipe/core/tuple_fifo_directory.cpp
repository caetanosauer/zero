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

#include "util/thread.h"
#include "util/sync.h"
#include "util/fileops.h"
#include "qpipe/core/tuple_fifo_directory.h"

#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>



ENTER_NAMESPACE(qpipe);


pthread_mutex_t 
tuple_fifo_directory_t::_dir_mutex = PTHREAD_MUTEX_INITIALIZER;

tuple_fifo_directory_t::dir_state_t
tuple_fifo_directory_t::_dir_state = TUPLE_FIFO_DIRECTORY_FIRST;

c_str
tuple_fifo_directory_t::_dir_path = c_str("temp");



const c_str& tuple_fifo_directory_t::dir_path() {
    /* TODO Make this a configuration variable */
    return _dir_path;
}



void tuple_fifo_directory_t::open_once() 
{
    critical_section_t cs(_dir_mutex);
    if (_dir_state != TUPLE_FIFO_DIRECTORY_FIRST)
        return;

    const c_str& path = dir_path();
    if (fileops_check_file_directory(path.data()))
        THROW2(TupleFifoDirectoryException,
               "Tuple fifo directory %s does not exist",
               path.data());
    
    if (fileops_check_directory_accessible(path.data()))
        THROW2(TupleFifoDirectoryException,
               "Tuple fifo directory %s not writeable",
               path.data());

    clean_dir();

    _dir_state = TUPLE_FIFO_DIRECTORY_OPEN;
}



void tuple_fifo_directory_t::close() {

    critical_section_t cs(_dir_mutex);
    if (_dir_state != TUPLE_FIFO_DIRECTORY_OPEN)
        return;

    clean_dir();

    _dir_state = TUPLE_FIFO_DIRECTORY_CLOSED;
}



c_str tuple_fifo_directory_t::generate_filepath(int id) {
    return c_str("%s/tuple_fifo_%d", dir_path().data(), id);
}



bool tuple_fifo_directory_t::filename_filter(const char* path) {
    int id;
    return sscanf(path, "tuple_fifo_%d", &id) == 1;
}



void tuple_fifo_directory_t::clean_dir() {

    DIR* dir = opendir(dir_path().data());
    if (dir == NULL)
        THROW2(TupleFifoDirectoryException,
               "opendir(%s) failed", dir_path().data());

    while (1) {

        struct dirent* dinfo = readdir(dir);
        if (dinfo == NULL)
            /* done reading */
            break;
 
        const char* filename = dinfo->d_name;
        if (!filename_filter(filename))
            /* not a tuple_fifo file */
            continue;

        /* If we are here, we have found a tuple_fifo file. Delete
           it. */
        c_str filepath("%s/%s", dir_path().data(), filename);
        if (unlink(filepath.data()))
            THROW2(TupleFifoDirectoryException,
                   "unlink(%s) failed", filepath.data());

        /* debugging */
        TRACE(TRACE_ALWAYS, "Deleting old tuple_fifo file %s\n",
              filepath.data());
    }

    if (closedir(dir))
        THROW2(TupleFifoDirectoryException,
               "closedir(%s) failed", dir_path().data());
}



EXIT_NAMESPACE(qpipe);
