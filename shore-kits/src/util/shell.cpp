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

/** @file:   shell.cpp
 *
 *  @brief:  Implementation of an abstract shell class for the test cases
 *
 *  @author: Ippokratis Pandis (ipandis)
 */


#include "util/shell.h"
#include "util/chomp.h"
#include "util/tcp.h"
#include "util/envvar.h"


void sig_handler_fwd(int sig)
{
    shell_t::sig_handler(sig);
}



/*********************************************************************
 *
 *  @fn:    constructor
 *  
 *  @brief: Initializes variables and sets prompt
 *
 *********************************************************************/

shell_t::shell_t(const char* prompt, 
                 const bool save_history,
                 const bool net_mode,
                 const uint_t net_port,
                 const bool inputfile_mode,
                 const string inputfile
) 
        : _cmd_counter(0), 
          _save_history(save_history), 
          _state(SHELL_NEXT_CONTINUE), 
          _processing_command(false),
          _net_mode(net_mode),
          _net_port(net_port),
          _inputfile_mode(inputfile_mode),
          _inputfile(inputfile),
          _listen_fd(-1), _conn_fd(-1), _net_conn_cnt(0)
{
    // 1. Set prompt
    _cmd_prompt = new char[SHELL_COMMAND_BUFFER_SIZE];
    memset(_cmd_prompt,0,SHELL_COMMAND_BUFFER_SIZE);
    if (prompt) strncpy(_cmd_prompt, prompt, strlen(prompt));
    _register_commands();
}



/*********************************************************************
 *
 *  @fn:    destructor
 *  
 *  @brief: Closes any open resources
 *
 *********************************************************************/

shell_t::~shell_t() 
{
    // 2. If in inputfile mode, close file
    if (_inputfile_mode)
    {
        _inputfilestream.close();
    }
}




/*********************************************************************
 *
 *  @fn:    start
 *  
 *  @brief: Starts a loop of commands. 
 *          
 *  @note:  Exits only if the processed command returns 
 *          PROMPT_NEXT_QUIT. 
 *
 *********************************************************************/

int shell_t::start() 
{
    // 1. Install SIGINT handler
    instance() = this;
    struct sigaction sa;
    struct sigaction sa_old;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);

    sa.sa_handler = &sig_handler_fwd;

    if (sigaction(SIGINT, &sa, &sa_old) < 0) {
        TRACE( TRACE_ALWAYS, "Cannot install SIGINT handler\n");
        return (-1);
    }

    // 2. If in network mode, open listening socket    
    if (_net_mode) {
        // 2a. Open listening socket
        _listen_fd = open_listenfd(_net_port);
        if (_listen_fd == -1) {
            TRACE( TRACE_ALWAYS, "Could not open list port (%d):\n%s\n",
                   _net_port, strerror(errno));
            return (-2);
        }
    }
    
    // 3. If in input FILE mode, open file stream
    if (_inputfile_mode)
    {
        // 3a. Open file stream
        _inputfilestream.open(_inputfile.c_str());
        if ((!_inputfilestream.is_open()) || (!_inputfilestream.good()))
        {
            TRACE( TRACE_ALWAYS, "File (%s) empty or not found.\n",
                   _inputfile.c_str());
            return (-3);
        }
    }

    // 4. Init all commands
    init_cmds();

    // 5. Open saved command history (optional)
    if (_save_history) {
        _save_history = history_open();
    }
        
    // 6. Command loop
    _state = SHELL_NEXT_CONTINUE;
    while (_state == SHELL_NEXT_CONTINUE) 
    {
        if (_net_mode) 
        {
            _state = process_network();
            if (_state == SHELL_NEXT_DISCONNECT) 
            {
                _conn_fd = -1;
                _state = SHELL_NEXT_CONTINUE;
            }
        }
        else {
            if (_inputfile_mode) 
            {
                _state = process_fileline();
            }
            else
            {
                _state = process_readline();
            }
        }
    }    
        
    // 5. Save command history (optional)
    if (_save_history) {
        TRACE( TRACE_ALWAYS, "Saving history. (%d) commands...\n",
               _cmd_counter);
        history_close();
    }

    // 5. Close all commands
    close_cmds();

    // 6. Restore old signal handler (probably unnecessary)
    sigaction(SIGINT, &sa_old, 0);
	
    return (0);
}



/*********************************************************************
 *
 *  @fn:    process_readline
 *  
 *  @brief: Get input from readline and process it
 *
 *********************************************************************/

int shell_t::process_readline()
{
    assert (!_net_mode);
    assert (!_inputfile_mode);

    char *cmd = (char*)NULL;
        
    // Get a line from the user.
    cmd = readline(_cmd_prompt);
    if (cmd == NULL) {
        // EOF
        return (SHELL_NEXT_QUIT);
    }

    return (process_one(cmd));
}



/*********************************************************************
 *
 *  @fn:    process_network
 *  
 *  @brief: Get input from network and process it
 *
 *********************************************************************/

int shell_t::process_network()
{
    assert (_net_mode);
    assert (!_inputfile_mode);
    assert (_listen_fd>=0);

    // 1. Check if connection already opened
    if (_conn_fd<0) {
        // 2. Open client connection

        // 2a. Wait for client connection
        TRACE( TRACE_ALWAYS, 
               "Waiting for client connection (%d) at port (%d)\n",
               _net_conn_cnt, _net_port);
        _client_len = sizeof(_client_addr);
        
        _conn_fd = accept(_listen_fd, 
                          (struct sockaddr*)&_client_addr,
                          (socklen_t*)&_client_len);

        if (_conn_fd < 0) {
            TRACE( TRACE_ALWAYS, "Error accepting new connection\n");
            return (SHELL_NEXT_QUIT);
        }

        // 2b. Open connection descriptor
        _in_stream = fdopen(_conn_fd, "r");
        if (_in_stream.get() == NULL) {
            TRACE( TRACE_ALWAYS, 
                   "fdopen() failed on connection descriptor (%d)\n",
                   _conn_fd);
            close(_conn_fd);
            return (SHELL_NEXT_QUIT);
        }
                          
        TRACE( TRACE_ALWAYS, "Client connection opened...\n");
        ++_net_conn_cnt;
    }

    assert (_in_stream.get());
    
    char cmd[SERVER_COMMAND_BUFFER_SIZE];

    // 3. Read input from _in_stream (network)
    char* fgets_ret = fgets(cmd, sizeof(cmd), _in_stream);
    if (fgets_ret == NULL) {
        // EOF
        return (SHELL_NEXT_QUIT);
    }

    // 4. Chomp off trailing '\n' or '\r', if they exist
    chomp_newline(cmd);
    chomp_carriage_return(cmd);

    return (process_one(cmd));
}



/*********************************************************************
 *
 *  @fn:    process_fileline
 *  
 *  @brief: Get input from the input FILE and process it
 *
 *********************************************************************/

int shell_t::process_fileline()
{
    assert (!_net_mode);
    assert (_inputfile_mode);

    string cmd_read;
        
    // Read a line from the FILE
    if (_inputfilestream.eof())
    {
        return (SHELL_NEXT_QUIT);
    }
    getline(_inputfilestream,cmd_read);
    cout << "FILECMD: " << cmd_read;

    // char* cmd = cmd_read.c_str()

    // // 4. Chomp off trailing '\n' or '\r', if they exist
    // chomp_newline(cmd);
    // chomp_carriage_return(cmd);
        
    return (process_one(cmd_read.c_str()));
}



/*********************************************************************
 *
 *  @fn:    process_one
 *  
 *  @brief: Basic checks done by any shell
 *
 *********************************************************************/

int shell_t::process_one(const char* acmd) 
{
    assert (acmd != NULL);

    // 2. Lock shell
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    CRITICAL_SECTION(sh_cs,_lock);

    // 3. Get command tag
    if ( sscanf(acmd, "%s", cmd_tag) < 1) {
        _helper->list_cmds();
        return (SHELL_NEXT_CONTINUE);
    }
        
    // 4. History control
    if (*acmd) {
        // non-empty line...
        add_history(acmd);
    }

    // 5. Update stats
    ++_cmd_counter;

    // 6. Process command
    _processing_command = true;
    int rval=0;
    cmdMapIt cmdit = _aliases.find(cmd_tag);
    if (cmdit == _aliases.end()) {
        TRACE( TRACE_ALWAYS, "Unknown command (%s)\n", cmd_tag);
        rval = SHELL_NEXT_CONTINUE;
    }
    else {
        rval = cmdit->second->handle(acmd);
    }
    _processing_command = false;

    // 7. Return SHELL_NEXT_CONTINUE if everything went ok
    return (rval);
}


/*********************************************************************
 *
 *  @fn:    add_cmd()
 *  
 *  @brief: Registers a shell command    
 *
 *********************************************************************/

int shell_t::add_cmd(command_handler_t* acmd) 
{
    assert (acmd);
    assert (!acmd->name().empty());
    cmdMapIt cmdit;

    // register main name
    cmdit = _cmds.find(acmd->name());
    if (cmdit!=_cmds.end()) {
        TRACE( TRACE_ALWAYS, 
               "Cmd (%s) already registered\n", 
               acmd->name().c_str());
        return (0);
    }
    else {
        TRACE( TRACE_DEBUG, 
               "Registering cmd (%s)\n", 
               acmd->name().c_str());        
        _cmds[acmd->name()] = acmd;
    }

    // register aliases
    int regs=0; // counts aliases registered
    vector<string>* apl = acmd->aliases();
    assert (apl);
    for (vector<string>::iterator alit = apl->begin(); alit != apl->end(); ++alit) {
        cmdit = _aliases.find(*alit);
        if (cmdit!=_aliases.end()) {
            TRACE( TRACE_ALWAYS, 
                   "Alias (%s) already registered\n", 
                   (*alit).c_str());
        }
        else {
            TRACE( TRACE_DEBUG, 
                   "Registering alias (%s)\n", 
                   (*alit).c_str());
            _aliases[*alit]=acmd;
            ++regs;
        }
    }
    assert (regs); // at least one alias should be registered
    return (0);
}



/*********************************************************************
 *
 *  @fn:    init_cmds()
 *  
 *  @brief: Iterates over all the registered commands and calls their
 *          init() function.
 *
 *********************************************************************/
       
int shell_t::init_cmds()
{
    CRITICAL_SECTION(sh_cs,_lock);
    for (cmdMapIt it = _cmds.begin(); it != _cmds.end(); ++it) {
        it->second->init();
    }
    return (0);
}



/*********************************************************************
 *
 *  @fn:    close_cmds()
 *  
 *  @brief: Iterates over all the registered commands and calls their
 *          close() function.
 *
 *********************************************************************/

int shell_t::close_cmds()
{
    CRITICAL_SECTION(sh_cs,_lock);
    for (cmdMapIt it = _cmds.begin(); it != _cmds.end(); ++it) {
        it->second->close();
    }
    return (0);
}



/*********************************************************************
 *
 *  @fn:    _register_commands()
 *  
 *  @brief: Registers the basic set of functions for every shell (such
 *          as {trace,conf,env,set,quit,echo,break})
 *
 *********************************************************************/

int shell_t::_register_commands() 
{
    REGISTER_CMD(trace_cmd_t,_tracer);
    REGISTER_CMD(conf_cmd_t,_confer);
    REGISTER_CMD(env_cmd_t,_enver);
    REGISTER_CMD(set_cmd_t,_seter);
    REGISTER_CMD(quit_cmd_t,_quiter);
    REGISTER_CMD(echo_cmd_t,_echoer);
    REGISTER_CMD(break_cmd_t,_breaker);
    REGISTER_CMD(zipf_cmd_t,_zipfer);

    REGISTER_CMD_PARAM(help_cmd_t,_helper,&_cmds);

    return (0);
}



/*********************************************************************
 *
 *  Command-specific functionality
 *
 *********************************************************************/



/*********************************************************************
 *
 *  QUIT
 *
 *********************************************************************/

void quit_cmd_t::setaliases() 
{
    _name = string("quit");
    _aliases.push_back("quit");
    _aliases.push_back("q");
    _aliases.push_back("exit");
}


/*********************************************************************
 *
 *  DISCONNECT
 *
 *********************************************************************/

void 
disconnect_cmd_t::setaliases() 
{
    _name = string("disconnect");
    _aliases.push_back("disconnect");
    _aliases.push_back("d");
}


/*********************************************************************
 *
 *  HELP
 *
 *********************************************************************/

void help_cmd_t::setaliases() 
{
    _name = string("help");
    _aliases.push_back("help");
    _aliases.push_back("h");
}


void help_cmd_t::list_cmds()
{
    TRACE( TRACE_ALWAYS, 
           "Available commands (help <cmd>): \n\n");
    for (cmdMapIt it = _pcmds->begin(); it != _pcmds->end(); ++it) {
        TRACE( TRACE_ALWAYS, " %s - %s\n", 
               it->first.c_str(), it->second->desc().c_str());
    }
}


int help_cmd_t::handle(const char* cmd) 
{
    char help_tag[SERVER_COMMAND_BUFFER_SIZE];
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];    
    if ( sscanf(cmd, "%s %s", help_tag, cmd_tag) < 2) {
        // prints the list of commands
        list_cmds();
        return (SHELL_NEXT_CONTINUE);
    }
    // otherwise prints usage of a specific command
    cmdMapIt it = _pcmds->find(cmd_tag);
    if (it==_pcmds->end()) {
        TRACE( TRACE_ALWAYS,"Cmd (%s) not found\n", cmd_tag);
        return (SHELL_NEXT_CONTINUE);
    }
    it->second->usage();
    return (SHELL_NEXT_CONTINUE);
}


/*********************************************************************
 *
 *  SET
 *
 *********************************************************************/

void set_cmd_t::init() 
{ 
    ev = envVar::instance(); 
}

void set_cmd_t::setaliases() 
{    
    _name = string("set");
    _aliases.push_back("set");
    _aliases.push_back("s");
}


int set_cmd_t::handle(const char* cmd) 
{
    assert (ev);
    ev->parseSetReq(cmd);
    return (SHELL_NEXT_CONTINUE);
}


void set_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "SET Usage:\n\n"                               \
           "*** set [<PARAM_NAME=PARAM_VALUE>*]\n"                      \
           "\nParameters:\n"                                            \
           "<PARAM_NAME>  : The name of the environment variable to set\n" \
           "<PARAM_VALUE> : The new value of the env variable\n\n");
}


/*********************************************************************
 *
 *  ENV
 *
 *********************************************************************/

void env_cmd_t::init() 
{ 
    ev = envVar::instance(); 
}

void env_cmd_t::setaliases() 
{    
    _name = string("env");
    _aliases.push_back("env");
    _aliases.push_back("e");
}


int env_cmd_t::handle(const char* cmd)
{    
    assert (ev);
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    char env_tag[SERVER_COMMAND_BUFFER_SIZE];    
    if ( sscanf(cmd, "%s %s", cmd_tag, env_tag) < 2) {
        // prints all the env
        ev->printVars();    
        return (SHELL_NEXT_CONTINUE);
    }
    ev->checkVar(env_tag);
    return (SHELL_NEXT_CONTINUE);
}


void env_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, "ENV Usage:\n\n"                               \
           "*** env [PARAM]\n"                      \
           "\nParameters:\n"                                            \
           "env         - Print all the environment variables\n" \
           "env <PARAM> - Print the value of a specific env variable\n\n");
}


/*********************************************************************
 *
 *  CONF
 *
 *********************************************************************/

void conf_cmd_t::init() 
{ 
    ev = envVar::instance(); 
}

void conf_cmd_t::setaliases() 
{    
    _name = string("conf");
    _aliases.push_back("conf");
    _aliases.push_back("c");
}


int conf_cmd_t::handle(const char* /* cmd */)
{    
    ev->refreshVars();
    return (SHELL_NEXT_CONTINUE);
}


void conf_cmd_t::usage(void)
{
    TRACE( TRACE_ALWAYS, 
           "CONF - Tries to reread all the set env vars from the config file\n");
}



/*********************************************************************
 *
 *  ZIPF
 *
 *********************************************************************/

void zipf_cmd_t::setaliases() 
{ 
    _name = string("zipf"); 
    _aliases.push_back("zipf"); 
    _aliases.push_back("z"); 
}

int zipf_cmd_t::handle(const char* cmd)
{
    char cmd_tag[SERVER_COMMAND_BUFFER_SIZE];
    char s_tag[SERVER_COMMAND_BUFFER_SIZE];
    if ( sscanf(cmd, "%s %s", cmd_tag, s_tag) < 2) {
        _is_enabled = false;
    }
    else {
        _s = atof(s_tag);
        _is_enabled = true;
    }

    TRACE( TRACE_ALWAYS, "Setting Zipf. Enabled=%d. S=%.2f\n", _is_enabled, _s);
    setZipf(_is_enabled,_s);
    return (SHELL_NEXT_CONTINUE);
}


void zipf_cmd_t::usage()
{
    TRACE( TRACE_ALWAYS,
           "zipf [<s>] - Without arguments, disables zipf input generation.\n" \
           "           - With arguments, enabled zipf and sets \"s\"\n");
}

string zipf_cmd_t::desc() const
{
    return string("Enables/Disables zipfian input generation");
}
