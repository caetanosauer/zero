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

/** @file:   envvar.h
 *
 *  @brief:  "environment variables" singleton class
 *
 *  @author: Ippokratis Pandis (ipandis)
 */

#ifndef __UTIL_ENVVAR_H
#define __UTIL_ENVVAR_H

#include "k_defines.h"

#include <iostream>

#include <map>

#include <readline/readline.h>
#include <readline/history.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>

#include "sm_vas.h"

#include "util/trace.h"
#include "util/guard.h"
#include "util/confparser.h"

#include "util.h"

using namespace std;



/*********************************************************************
 *
 *  @class: envVar
 *
 *  @brief: Encapsulates the "environment" variables functionality.
 *          It does two things. First, it has the functions that parse
 *          a config file. Second, it stores all the parsed params to
 *          a map of <string,string>. The params may be either read from
 *          the config file or set at runtime.
 *
 *  @note:  Singleton
 *
 *  @usage: - Get instance
 *          - Call setVar()/getVar() for setting/getting a specific variable.
 *          - Call readConfVar() to parse the conf file for a specific variable.
 *            The read value will be stored at the map.
 *          - Call parseSetReq() for parsing and setting a set of params
 *
 *********************************************************************/

const string ENVCONFFILE = "shore.conf";
const string DEFAULTCONFIG = "tm1-1";

class envVar 
{
private:

    typedef map<string,string>        envVarMap;
    typedef envVarMap::iterator       envVarIt;
    typedef envVarMap::const_iterator envVarConstIt;

    envVarMap _evm;
    string _cfname;
    mcs_lock _lock;
    guard<ConfigFile> _pfparser;

    envVar(const string sConfFile=ENVCONFFILE) 
        : _cfname(sConfFile)
    { 
        assert (!_cfname.empty());
        _pfparser = new ConfigFile(_cfname);
        assert (_pfparser);
    }
    ~envVar() { }

    // Helpers

    template <class T>
    string _toString(const T& arg)
    {
        ostringstream out;
        out << arg;
        return (out.str());
    }

    // reads the conf file for a specific param
    // !!! the caller should have the lock !!!
    string _readConfVar(const string& sParam, const string& sDefValue); 
    
public:

    static envVar* instance() { static envVar _instance; return (&_instance); }

    // refreshes all the env vars from the conf file
    int refreshVars(void);

    // sets a new parameter
    int setVar(const string& sParam, const string& sValue);
    int setVarInt(const string& sParam, const int& iValue);

    // retrieves a specific param from the map. if not found, searches the conf file
    // and updates the map
    // @note: after this call the map will have an entry about sParam
    string getVar(const string& sParam, const string& sDefValue);  
    int    getVarInt(const string& sParam, const int& iDefValue);  
    double getVarDouble(const string& sParam, const double& iDefValue);

    // checks if a specific param is set at the map, or, if not at the map, at the conf file
    // @note: this call does not update the map 
    void checkVar(const string& sParam);      

    // sets as input another conf file
    void setConfFile(const string& sConfFile);
    string getConfFile() const;

    // prints all the env vars
    void printVars(void);

    // parses a SET request
    int parseOneSetReq(const string& in);
    
    // parses a string of SET requests
    int parseSetReq(const string& in);

    // gets db-config and then <db-config>-attribute
    int setConfiguration(string sConfiguration);
    string getSysName();
    int    setSysName(const string& sysName);
    string getSysDesign();
    int    setSysDesign(const string& sysDesign);

    string getSysVar(string sParam);
    int    getSysVarInt(string sParam);
    double getSysVarDouble(string sParam);

}; // EOF: envVar



#endif /* __UTIL_ENVVAR_H */

