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

/** @file progress.cpp
 *
 *  @brief Implementation of progress helper functions
 *
 *  @author Ippokratis Pandis (ipandis)
 */

#include "util/progress.h"

#include "k_defines.h"

/* definitions of exported helper functions */


/** @fn progress_reset
 *  @brief Set the indicator to 0
 */

void progress_reset(unsigned long* indicator) {
    *indicator = 0;
}



/** @fn progress_update
 *  @brief Inceases progress by 1, if PROGRESS_INTERVAL outputs a dot
 */

void progress_update(unsigned long* indicator) {
  
    if ( (++*indicator % PROGRESS_INTERVAL) == 0 ) {
        printf(".");
        fflush(stdout);
        *indicator = 0; // prevent overflow
    }
}



/** @fn progress_done
 *  @brief Outputs a done message
 */

void progress_done(const char* tablename) {
    
    printf("\nDone loading (%s)...\n", tablename);
    fflush(stdout);
}

