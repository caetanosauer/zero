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

/** @file:   envvar.cpp
 *
 *  @brief:  "environment variables" singleton class
 *
 *  @author: Ippokratis Pandis (ipandis)
 */


#include "util/envvar.h"


/*********************************************************************
 *
 *  @fn:    setVar/getVar/readConfVar
 *  
 *  @brief: Environment variables manipulation
 *          
 *********************************************************************/


// helper: reads the conf file for a specific param
// !!! the caller should have the lock !!!
string envVar::_readConfVar(const string& sParam, const string& sDefValue)
{
    if (sParam.empty()||sDefValue.empty()) {
        TRACE( TRACE_ALWAYS, "Invalid Param or Value input\n");
        return ("");
    }
    assert (_pfparser);
    string tmp;
    // probe config file
    _pfparser->readInto(tmp,sParam,sDefValue); 
    // set entry in the env map
    _evm[sParam] = tmp;
    TRACE( TRACE_DEBUG, "(%s) (%s)\n", sParam.c_str(), tmp.c_str());
    return (tmp);
}



// sets a new parameter
int envVar::setVar(const string& sParam, const string& sValue)
{
    if ((!sParam.empty())&&(!sValue.empty())) {
        TRACE( TRACE_DEBUG, "(%s) (%s)\n", sParam.c_str(), sValue.c_str());
        CRITICAL_SECTION(evm_cs,_lock);
        _evm[sParam] = sValue;
        return (_evm.size());
    }
    return (0);
}


int envVar::setVarInt(const string& sParam, const int& iValue)
{    
    return (setVar(sParam,_toString(iValue)));
}



// refreshes all the env vars from the conf file
int envVar::refreshVars(void)
{
    TRACE( TRACE_DEBUG, "Refreshing environment variables\n");
    CRITICAL_SECTION(evm_cs,_lock);
    for (envVarIt it= _evm.begin(); it != _evm.end(); ++it)
        _readConfVar(it->first,it->second);    
    return (0);
}



// checks the map for a specific param
// if it doesn't find it checks also the config file
string envVar::getVar(const string& sParam, const string& sDefValue)
{
    if (sParam.empty()) {
        TRACE( TRACE_ALWAYS, "Invalid Param input\n");
        return ("");
    }

    CRITICAL_SECTION(evm_cs,_lock);
    envVarIt it = _evm.find(sParam);
    if (it==_evm.end()) {        
        //TRACE( TRACE_DEBUG, "(%s) param not set. Searching conf\n", sParam.c_str()); 
        return (_readConfVar(sParam,sDefValue));
    }
    return (it->second);
}

int envVar::getVarInt(const string& sParam, const int& iDefValue)
{
    return (atoi(getVar(sParam,_toString(iDefValue)).c_str()));
}

double envVar::getVarDouble(const string& sParam, const double& iDefValue)
{
    char* endp;
    double d;
    string r = getVar(sParam,_toString(iDefValue));
    d = strtod(r.c_str(), &endp);
    return (d);
}


// checks if a specific param is set at the map or (fallback) the conf file
void envVar::checkVar(const string& sParam)
{
    string r;
    CRITICAL_SECTION(evm_cs,_lock);
    // first searches the map
    envVarIt it = _evm.find(sParam);
    if (it!=_evm.end()) {
        r = it->second + " (map)";
    }
    else {
        // if not found on map, searches the conf file
        if (_pfparser->keyExists(sParam)) {
            _pfparser->readInto(r,sParam,string("Not found"));
            r = r + " (conf)";
        }
        else {
            r = string("Not found");
        }        
    }
    TRACE( TRACE_ALWAYS, "%s -> %s\n", sParam.c_str(), r.c_str()); 
}


/*************************************************
 * 
 * System-related variables
 *
 *************************************************/

string envVar::getSysName()
{
    string sysName = "system";
    return (getSysVar(sysName));
}

int envVar::setSysName(const string& sysName)
{
    string configsys = getVar("db-config","invalid");
    configsys = configsys + "-system";
    return (setVar(configsys,sysName));
}

string envVar::getSysDesign()
{
    string sysDesign = "design";
    return (getSysVar(sysDesign));
}

int envVar::setSysDesign(const string& sysDesign)
{
    string configsys = getVar("db-config","invalid");
    configsys = configsys + "-design";
    return (setVar(configsys,sysDesign));
}

string envVar::getSysVar(string sParam)
{
    string config = getVar("db-config","invalid");
    config = config + "-" + sParam;
    return (getVar(config,"invalid"));
}


int envVar::getSysVarInt(string sParam)
{
    string config = getVar("db-config","invalid");
    config = config + "-" + sParam;
    return (getVarInt(config,0));
}

double envVar::getSysVarDouble(string sParam)
{
    string config = getVar("db-config","invalid");
    config = config + "-" + sParam;
    return (getVarDouble(config,0.0));
}


int envVar::setConfiguration(string sConfiguration)
{
    return (setVar("db-config",sConfiguration));
}


// prints all the env vars
void envVar::printVars(void)
{
    TRACE( TRACE_DEBUG, "Environment variables\n");
    CRITICAL_SECTION(evm_cs,_lock);
    for (envVarConstIt cit= _evm.begin(); cit != _evm.end(); ++cit)
        TRACE( TRACE_STATISTICS, "%s -> %s\n", cit->first.c_str(), cit->second.c_str()); 
}



// sets as input another conf file
void envVar::setConfFile(const string& sConfFile)
{
    assert (!sConfFile.empty());
    CRITICAL_SECTION(evm_cs,_lock);
    _cfname = sConfFile;
    _pfparser = new ConfigFile(_cfname);
    assert (_pfparser);
}

string envVar::getConfFile() const
{
    return (_cfname);
}


// parses a SET request
int envVar::parseOneSetReq(const string& in)
{
    string param;
    string value;
    char valuesep = '=';

    size_t sepos = in.find(valuesep);
    if(sepos == string::npos) {
        TRACE( TRACE_DEBUG, "(%s) is malformed\n", in.c_str());
        return(1);
    }
    param = in.substr(0, sepos);
    value = in.substr(sepos+1);
    if (value.empty()) {
        TRACE( TRACE_DEBUG, "(%s) is malformed\n", in.c_str());
        return(2);
    }
    TRACE( TRACE_DEBUG, "%s -> %s\n", param.c_str(), value.c_str()); 
    CRITICAL_SECTION(evm_cs,_lock);
    _evm[param] = value;
    return(0);
}
    


// parses a string of (multiple) SET requests
int envVar::parseSetReq(const string& in)
{
    int cnt=0;

    // FORMAT: SET [<clause_name>=<clause_value>]*
    char clausesep = ' ';
    size_t start = in.find(clausesep); // omit the SET cmd
    size_t end;
    for (; start != string::npos; start=end) {
	start++; // skip the separator
	end = in.find(clausesep, start);
	parseOneSetReq(in.substr(start, end-start));
	++cnt;
    }
    return (cnt);
}


