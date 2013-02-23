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

#ifndef __UTIL_SERVER_CONFIG_H
#define __UTIL_SERVER_CONFIG_H

#define SERVER_COMMAND_BUFFER_SIZE 1024

// Configurable values
#define SCLIENT_VERSION "v2.1"
#define SCLIENT_PROMPT  "(sparta) "

// Until we get CC to recognize our libreadline install...
#define USE_READLINE 1

// Default configuration values
#define SCLIENT_DIRECTORY_NAME ".shorekits"
#define SCLIENT_HISTORY_FILE   "history"

#define SCLIENT_NET_MODE_LISTEN_PORT 10000


#endif // __UTIL_SERVER_CONFIG_H
