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

#ifndef __UTIL_CMD_TRACE_H
#define __UTIL_CMD_TRACE_H


#include <map>
#include "util/command/command_handler.h"
#include "util.h"

using std::map;


class trace_cmd_t : public command_handler_t 
{
    map<c_str, int> _known_types;
    
public:

    trace_cmd_t() { init(); }
    ~trace_cmd_t() { }

    void init();
    void close() { }

    int handle(const char* cmd);

    void setaliases();
    void usage();
    string desc() const { return (string("Manipulates tracing level")); }               

private:
    
    void enable(const char* type);
    void disable(const char* type);
    void print_enabled_types();
    void print_known_types();

}; // EOF: trace_cmd_t


#endif /** __UTIL_CMD_TRACE_H */


