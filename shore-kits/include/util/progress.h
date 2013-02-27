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

/** @file progress.h
 *
 *  @brief Declaration of progress helper functions
 *
 *  @author Ippokratis Pandis (ipandis)
 */

#ifndef __UTIL_PROGRESS_H
#define __UTIL_PROGRESS_H


#define PROGRESS_INTERVAL 100000

#define MAX_LINE_LENGTH 1024


/** exported helper functions */

void progress_reset(unsigned long* indicator);
void progress_update(unsigned long* indicator);
void progress_done(const char* tablename);

#endif

