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

#ifndef __UTIL_CMD_PRINTER_H
#define __UTIL_CMD_PRINTER_H


#include "util/command/command_handler.h"


class printer_t : public command_handler_t 
{
public:
    printer_t() { }
    virtual ~printer_t() { }

    virtual void handle_command(const char* command);

    void init() { assert(0); }
    void close() { assert(0); }
    int handle(const char* /* cmd */) { return (SHELL_NEXT_CONTINUE); }
    void setaliases() { assert(0); }
    void usage() { assert(0); }
    string desc() const { return (string("")); }               

}; // EOF: printer_t

#endif /** __UTIL_CMD_PRINTER_H */
