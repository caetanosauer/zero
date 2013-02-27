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

/** @file store_string.cpp
 *
 *  @brief Implementation of string manipulation functions
 *
 *  @author Ippokratis Pandis (ipandis)
 */

#include "util/store_string.h"

#include "k_defines.h"

/* definitions of exported helper functions */


/** @fn store_string
 *  @brief Copies a string to another
 */

void store_string(char* dest, char* src) {
    int len = strlen(src);
    strncpy(dest, src, len);
    dest[len] = '\0';
}



/** @fn store_string
 *  @brief Copies a const string to another
 */


void store_string(char* dest, const char* src) {
    int len = strlen(src);
    strncpy(dest, src, len);
    dest[len] = '\0';
}

