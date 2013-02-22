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

#include "util.h"
#include "util/config.h"
#include "util/command/tracer.h"

#include "k_defines.h"

/* definitions of exported methods */


void trace_cmd_t::setaliases()
{
    _name = string("trace");
    _aliases.push_back("trace");
    _aliases.push_back("t");
    _aliases.push_back("tracer");
}


void trace_cmd_t::init()
{

#define ADD_TYPE(x) _known_types[c_str("%s", #x)] = x;
    
    ADD_TYPE(TRACE_ALWAYS);
    ADD_TYPE(TRACE_TUPLE_FLOW);
    ADD_TYPE(TRACE_PACKET_FLOW);
    ADD_TYPE(TRACE_SYNC_COND);
    ADD_TYPE(TRACE_SYNC_LOCK);
    ADD_TYPE(TRACE_THREAD_LIFE_CYCLE);
    ADD_TYPE(TRACE_TEMP_FILE);
    ADD_TYPE(TRACE_CPU_BINDING);
    ADD_TYPE(TRACE_QUERY_RESULTS);
    ADD_TYPE(TRACE_QUERY_PROGRESS);
    ADD_TYPE(TRACE_STATISTICS);
    ADD_TYPE(TRACE_NETWORK);
    ADD_TYPE(TRACE_RESPONSE_TIME);
    ADD_TYPE(TRACE_WORK_SHARING);
    ADD_TYPE(TRACE_TRX_FLOW);
    ADD_TYPE(TRACE_KEY_COMP);
    ADD_TYPE(TRACE_RECORD_FLOW);

    ADD_TYPE(TRACE_DEBUG);
};



int trace_cmd_t::handle(const char* cmd)
{
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    char tag[SERVER_COMMAND_BUFFER_SIZE];

    // parse cmd tag (should be something like "tracer")
    if ( sscanf(cmd, "%s", cmd_tag) < 1 ) {
        TRACE(TRACE_ALWAYS, "Unable to parse cmd tag!\n");
        usage();
        return (SHELL_NEXT_CONTINUE);
    }
  
    // we can now use cmd tag for all messages...


    // parse tag
    if ( sscanf(cmd, "%*s %s", tag) < 1 ) {
        usage();
        return (SHELL_NEXT_CONTINUE);
    }

    if (!strcasecmp(tag, "known")) {
        print_known_types();
        return (SHELL_NEXT_CONTINUE);
    }

    if (!strcasecmp(tag, "list")) {
        print_enabled_types();
        return (SHELL_NEXT_CONTINUE);
    }

    if (!strcasecmp(tag, "enable")) {
        char trace_type[SERVER_COMMAND_BUFFER_SIZE];
        if ( sscanf(cmd, "%*s %*s %s", trace_type) < 1 ) {
            usage();
            return (SHELL_NEXT_CONTINUE);
        }
        enable(trace_type);
        return (SHELL_NEXT_CONTINUE);
    }
    
    if (!strcasecmp(tag, "disable")) {
        char trace_type[SERVER_COMMAND_BUFFER_SIZE];
        if ( sscanf(cmd, "%*s %*s %s", trace_type) < 1 ) {
            usage();
            return (SHELL_NEXT_CONTINUE);
        }
        disable(trace_type);
        return (SHELL_NEXT_CONTINUE);
    }

    TRACE(TRACE_ALWAYS, "Unrecognized tag %s\n", tag);
    usage();
    return (SHELL_NEXT_CONTINUE);
}



void trace_cmd_t::enable(const char* type)
{
    map<c_str, int>::iterator it;
    for (it = _known_types.begin(); it != _known_types.end(); ++it) {
        if (!strcasecmp(it->first.data(), type)) {
            /* found it! */
            int mask = it->second;
            TRACE_SET(TRACE_GET() | mask);
            TRACE(TRACE_ALWAYS, "Enabled %s\n", it->first.data());
            return;
        }
    }

    TRACE(TRACE_ALWAYS, "Unknown type %s\n", type);
}



void trace_cmd_t::disable(const char* type)
{
    map<c_str, int>::iterator it;
    for (it = _known_types.begin(); it != _known_types.end(); ++it) {
        if (!strcasecmp(it->first.data(), type)) {
            /* found it! */
            int mask = it->second;
            TRACE_SET(TRACE_GET() & (~mask));
            TRACE(TRACE_ALWAYS, "Disabled %s\n", it->first.data());
            return;
        }
    }

    TRACE(TRACE_ALWAYS, "Unknown type %s\n", type);
}



void trace_cmd_t::print_known_types() {

    map<c_str, int>::iterator it;
    for (it = _known_types.begin(); it != _known_types.end(); ++it) {
        TRACE(TRACE_ALWAYS,
              "Registered trace type %s\n", it->first.data());
    }
}



void trace_cmd_t::print_enabled_types() {

    map<c_str, int>::iterator it;
    for (it = _known_types.begin(); it != _known_types.end(); ++it) {
        int mask = it->second;
        if ( TRACE_GET() & mask )
            TRACE(TRACE_ALWAYS, "Enabled type %s\n", it->first.data());
    }
}



void trace_cmd_t::usage() {
    TRACE(TRACE_ALWAYS, "trace known|list|enable <type>|disable <type>\n");
}
