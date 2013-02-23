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

/** @file:   shell.h
 *
 *  @brief:  Abstract shell class for the test cases
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#ifndef __UTIL_SHELL_H
#define __UTIL_SHELL_H

#include "k_defines.h"

#include <map>


#include <assert.h>
#include <signal.h>
#include <errno.h>

// For reading from std::in
#include <readline/readline.h>
#include <readline/history.h>

// For reading from network
#include <sys/types.h>  // for socket
#include <sys/socket.h> 
#include <netdb.h>      // for clientaddr structures

// For reading from FILE
#include <iostream>
#include <fstream>
#include <string>

#include "util/command/command_handler.h"
#include "util/command/tracer.h"

#include "util.h"

using namespace std;



extern "C" void sig_handler_fwd(int sig);


typedef map<string,command_handler_t*> cmdMap;
typedef cmdMap::iterator cmdMapIt;



/*********************************************************************
 *
 *  @brief: Few basic commands
 *
 *********************************************************************/

class envVar;



#define REGISTER_CMD(cmdtype,cmdname)                   \
    cmdname = new cmdtype();                            \
    cmdname->setaliases();                              \
    add_cmd(cmdname.get())

#define REGISTER_CMD_PARAM(cmdtype,cmdname,paramname)   \
    cmdname = new cmdtype(paramname);                   \
    cmdname->setaliases();                              \
    add_cmd(cmdname.get())



struct quit_cmd_t : public command_handler_t {
    void setaliases();
    int handle(const char* /* cmd */) { return (SHELL_NEXT_QUIT); }
    string desc() const { return (string("Quit")); }               
};


struct disconnect_cmd_t : public command_handler_t 
{
    void setaliases();
    int handle(const char* /* cmd */) { return (SHELL_NEXT_DISCONNECT); }
    string desc() const { return (string("Disconnect client")); }
};


struct help_cmd_t : public command_handler_t 
{
    cmdMap* _pcmds; // pointer to the supported commands
    help_cmd_t(cmdMap* pcmds) : _pcmds(pcmds) { assert(pcmds); }
    ~help_cmd_t() { }
    void setaliases();
    int handle(const char* cmd);
    void usage() { 
        TRACE( TRACE_ALWAYS, "HELP       - prints usage\n"); 
        TRACE( TRACE_ALWAYS, "HELP <cmd> - prints detailed help for <cmd>\n"); 
    }
    string desc() const { return (string("Help - Use 'help <cmd>' for usage of specific cmd")); }
    void list_cmds();
}; 


struct set_cmd_t : public command_handler_t 
{
    envVar* ev;
    void init();
    void setaliases();
    int handle(const char* cmd);
    void usage();
    string desc() const { return (string("Sets env vars")); }               
};


struct env_cmd_t : public command_handler_t 
{
    envVar* ev;
    void init();
    void setaliases();
    int handle(const char* cmd);
    void usage();
    string desc() const { return (string("Prints env vars")); }               
};


struct conf_cmd_t : public command_handler_t 
{
    envVar* ev;
    void init();
    void setaliases();
    int handle(const char* cmd);
    void usage();
    string desc() const { return (string("Rereads env vars")); }               
};


struct echo_cmd_t : public command_handler_t {
    void setaliases() { _name = string("echo"); _aliases.push_back("echo"); }
    int handle(const char* cmd) {
        printf("%s\n", cmd+strlen("echo "));
        return (SHELL_NEXT_CONTINUE);
    }
    void usage() { TRACE( TRACE_ALWAYS, "usage: echo <string>\n"); }
    string desc() const { return string("Echoes its input arguments to the screen"); }
};


struct break_cmd_t : public command_handler_t {
    void setaliases() { _name = string("break"); _aliases.push_back("break"); }
    int handle(const char* /* cmd */) {
        raise(SIGINT);
        return (SHELL_NEXT_CONTINUE);
    }
    void usage() { TRACE( TRACE_ALWAYS, "usage: break\n"); }
    string desc() const { return string("Breaks into a debugger by raising ^C " \
                                  "(terminates program if no debugger is active)"); }
};


struct zipf_cmd_t : public command_handler_t {
    double _s;
    bool _is_enabled;
    zipf_cmd_t() : _s(0.0), _is_enabled(false) { };
    ~zipf_cmd_t() { };
    void setaliases();
    int handle(const char* cmd);
    void usage();
    string desc() const;
};




/*********************************************************************
 *
 *  @abstract class: shell_t
 *
 *  @brief:          Base class for shells.
 *
 *  @usage:          - Inherit from this class
 *                   - Implement the process_command() function
 *                   - Call the start() function
 *
 *********************************************************************/



class shell_t 
{
protected:

    array_guard_t<char> _cmd_prompt;
    int   _cmd_counter;

    bool  _save_history;
    int   _state;

    mcs_lock _lock;
    cmdMap   _cmds;
    cmdMap   _aliases;
    bool     _processing_command;

    // For network mode
    bool   _net_mode;
    uint_t _net_port;

    // For input file mode
    bool   _inputfile_mode;
    string _inputfile;
    ifstream _inputfilestream;

    file_guard_t _in_stream;
    int _listen_fd;

    struct sockaddr_in _client_addr;
    int _client_len;
    int _conn_fd;

    int _net_conn_cnt;


    // cmds
    guard<quit_cmd_t> _quiter;
    guard<disconnect_cmd_t> _disconnecter;
    guard<help_cmd_t> _helper;
    guard<set_cmd_t>  _seter;
    guard<env_cmd_t>  _enver;
    guard<conf_cmd_t> _confer;
    guard<trace_cmd_t>   _tracer;

    guard<echo_cmd_t> _echoer;
    guard<break_cmd_t> _breaker;
    guard<zipf_cmd_t> _zipfer;

    int _register_commands();    

public:

    shell_t(const char* prompt = SCLIENT_PROMPT, 
            const bool save_history = true,
            const bool net_mode = false,
            const uint_t net_port = SCLIENT_NET_MODE_LISTEN_PORT,
            const bool inputfile_mode = false,
            const string inputfile = "");

    virtual ~shell_t();

    static shell_t* &instance() { 
        static shell_t* _instance; return _instance; 
    }

    int get_command_cnt() { 
        return (_cmd_counter); 
    }

    static void sig_handler(int sig) {
	assert(sig == SIGINT && instance());	
	if( int rval=instance()->SIGINT_handler() )
	    exit(rval);
    }

    // should register own commands
    virtual int register_commands()=0;
    int add_cmd(command_handler_t* acmd);
    int init_cmds();
    int close_cmds();

    // basic shell functionality    
    virtual int SIGINT_handler() { return (ECANCELED); /* exit immediately */ }     
    int start();

    // Executes a command read from std::in
    int process_readline();

    // Executes a command read from the network socket
    int process_network();

    // Executes a command read from the input file
    int process_fileline();

    // Executes a specific command
    int process_one(const char* cmd);

}; // EOF: shell_t



#endif /* __UTIL_SHELL_H */

