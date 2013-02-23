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

/** @file fileops.h
 *
 *  @brief Exports common file checking operations.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @bug See fileops.c.
 */

#ifndef __UTIL_FILEOPS_H
#define __UTIL_FILEOPS_H


/* exported constants */

#define FILEOPS_MAX_PATH_SIZE            1024
#define FILEOPS_ERROR_NOT_FOUND         -1
#define FILEOPS_ERROR_NOT_DIRECTORY     -2
#define FILEOPS_ERROR_PATH_TOO_LONG     -3
#define FILEOPS_ERROR_PERMISSION_DENIED -4


/* exported functions */

int fileops_check_file_exists    (const char* path);
int fileops_check_file_directory (const char* path);
int fileops_check_file_readable  (const char* path);
int fileops_check_file_writeable (const char* path);
int fileops_check_file_executable(const char* path);

int fileops_check_directory_accessible(const char* path);
int fileops_check_file_creatable      (const char* path);

int fileops_parse_parent_directory(char* dst, int dst_size, const char* src);


#endif
