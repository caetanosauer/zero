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

/** @file:   command_handler.h
 *
 *  @brief:  Interface each shell command should implement 
 *
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Naju Mancheril (ngm)
 */

#ifndef __CMD_HANDLER_H
#define __CMD_HANDLER_H

#include "k_defines.h"

#include <vector>

#include "util.h"

using namespace std;



// constants
const int SHELL_COMMAND_BUFFER_SIZE = 64;
const int SHELL_NEXT_CONTINUE       = 1;
const int SHELL_NEXT_DISCONNECT     = 2; // used in network mode
const int SHELL_NEXT_QUIT           = 3;


class command_handler_t 
{
public:
    typedef vector<string> aliasList;
    typedef aliasList::iterator aliasIt;

protected:
    aliasList _aliases;
    string _name;

public:

    command_handler_t() { }
    virtual ~command_handler_t() { }

    // COMMAND INTERFACE


    // init/close
    virtual void init() { /* default do nothing */ };
    virtual void close() { /* default do nothing */ };

    // by default should return SHELL_NEXT_CONTINUE
    virtual int handle(const char* cmd)=0; 

    // should push_back() a set of aliases
    // the first one is the basic command name    
    virtual void setaliases()=0;
    string name() const { return (_name); }
    aliasList* aliases() { return (&_aliases); }

    // should print usage
    virtual void usage() { /* default do nothing */ };

    // should return short description
    virtual string desc() const=0;

}; // EOF: command_handler_t



#endif /** __CMD_HANDLER_H **/
