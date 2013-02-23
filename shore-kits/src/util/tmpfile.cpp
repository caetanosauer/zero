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

/** @file:   tmpfile.cpp
 *
 *  @brief:  Opens a temporary file with a unique name
 *
 *  @author: Naju Mancheril (ngm)
 *
 */

#include <stdlib.h>
#include "util/tmpfile.h"
#include "util/exception.h"
#include "util/trace.h"
#include "util/guard.h"
#include <stdio.h>


/******************************************************************** 
 *
 *  @fn:     create_tmp_file
 *
 *  @brief:  Opens a temporary file with a unique name
 *
 *  @param:  Name string to store the new file's name in
 *
 *  @return: The file or NULL on error
 *
 ********************************************************************/

FILE* create_tmp_file(c_str& name, const c_str& prefix) 
{
    // TODO: use a configurable temp dir
    char meta_template[] = "tmp/%s.XXXXXX";
    int len = sizeof(meta_template) - 2 + strlen(prefix);
    array_guard_t<char> name_template = new char[len + 1];
    sprintf(name_template, meta_template, prefix.data());
    int fd = mkstemp(name_template);
    if(fd < 0) {
        THROW3(FileException,
                        "Caught %s while opening temp file %s",
                        errno_to_str().data(), (char*) name_template);
    }

    TRACE(TRACE_TEMP_FILE, "Created temp file %s\n", (char*) name_template);

    // open a stream on the file
    FILE *file = fdopen(fd, "w");
    if(!file) {
        THROW3(FileException,
                        "Caught %s while opening a stream on %s",
                        errno_to_str().data(), (char*) name_template);
    }

    name = (char*) name_template;
    return file;
}


