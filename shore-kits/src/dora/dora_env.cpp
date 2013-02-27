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

/** @file:   dora_env.cpp
 *
 *  @brief:  Implementation of a DORA environment class.
 *           Common functionality among DORA environments.
 *
 *  @author: Ippokratis Pandis, Jun 2009
 */

#include "dora/common.h"
#include "dora/dora_env.h"

using namespace shore;

ENTER_NAMESPACE(dora);

typedef partition_t<int> irpImpl;


/******************************************************************** 
 *
 *  @fn:    DoraEnv construction/destruction
 *
 *  @brief: Depending on the type of the DORA system it updated the 
 *          physical design characteristics of the Environment
 *
 ********************************************************************/

DoraEnv::DoraEnv()
{ 
    _check_type();
}


DoraEnv::~DoraEnv()
{ 
}




/******************************************************************** 
 *
 *  @fn:    Type related functions
 *
 ********************************************************************/

uint DoraEnv::dtype() const
{
    return (_dtype);
}

bool DoraEnv::is_dora() const
{
    return (_dtype & DT_PLAIN);
}

bool DoraEnv::is_plp() const 
{ 
    return (_dtype & DT_PLP); 
}

uint DoraEnv::update_pd(ShoreEnv* penv)
{
    assert (penv);

    // Check at what mode we run (DORA/PLP) 
    _check_type();
    
    // All the DORA flavors do not use centralized locks
    penv->add_pd(PD_NOLOCK);

    if (_dtype & DT_PLP) {
        penv->add_pd(PD_NOLATCH);
    }

    return (_dtype);
}


/******************************************************************** 
 *
 *  @fn:    statistics
 *
 *  @brief: Prints statistics for DORA Env. If DFlusher is enabled,
 *          it prints those stats. 
 *
 ********************************************************************/

int DoraEnv::_statistics(ShoreEnv* /* penv */)
{
    // DORA STATS
    TRACE( TRACE_STATISTICS, "----- DORA -----\n");
    uint sz=_irptp_vec.size();
    TRACE( TRACE_STATISTICS, "Tables  = (%d)\n", sz);
    for (uint i=0;i<sz;++i) {
        _irptp_vec[i]->statistics();
    }

#ifdef CFG_FLUSHER
    _flusher->statistics();
#endif
    return (0);
}




/****************************************************************** 
 *
 * @fn:    _check_type()
 *
 * @brief: Reads the system name and sets the appropriate dtype
 *
 ******************************************************************/

uint DoraEnv::_check_type()
{
    // Check what system is running
    string sysname = envVar::instance()->getSysName();

    if (sysname.compare("baseline")==0) {
        assert(0); // Shouldn't be here
    }

    if (sysname.compare("dora")==0) {
        _dtype = DT_PLAIN;
    }

    if (sysname.compare("plp")==0) {
        _dtype = DT_PLP;
    }

    return (_dtype);
}

/****************************************************************** 
 *
 * @fn:    _post_start()
 *
 * @brief: Resets the tables and starts the flusher, if using one.
 *
 * @note:  Should be called before the start function of the specific
 *         DORA environment instance
 *
 ******************************************************************/

int DoraEnv::_post_start(ShoreEnv* penv)
{
#ifdef CFG_FLUSHER
    // Start the flusher
    TRACE( TRACE_ALWAYS, "Creating dora-flusher...\n");
    _flusher = new dora_flusher_t(penv, c_str("DFlusher")); 
    assert(_flusher.get());
    _flusher->fork();
    _flusher->start();
#endif

    // Reset the tables
    for (uint_t i=0; i<_irptp_vec.size(); i++) {
        _irptp_vec[i]->reset();
    }

    penv->set_dbc(DBC_ACTIVE);
    return (0);
}


/****************************************************************** 
 *
 * @fn:    _post_stop()
 *
 * @brief: Stops the tables and the flusher, if using one.
 *
 * @note:  Should be called after the stop function of the specific
 *         DORA environment instance, if any.
 *
 ******************************************************************/

int DoraEnv::_post_stop(ShoreEnv* penv)
{
    // Stopping the tables
    TRACE( TRACE_ALWAYS, "Stopping...\n");
    for (uint i=0; i<_irptp_vec.size(); i++) {
        _irptp_vec[i]->stop();
    }
    _irptp_vec.clear();

#ifdef CFG_FLUSHER
    // Stopping the flusher
    TRACE( TRACE_ALWAYS, "Stopping dora-flusher...\n");
    _flusher->stop();
    _flusher->join();
#endif

    penv->set_dbc(DBC_STOPPED);
    return (0);
}



/****************************************************************** 
 *
 * @fn:    _newrun()
 *
 * @brief: Calls the prepareNewRun for all the tables
 *
 ******************************************************************/

w_rc_t DoraEnv::_newrun(ShoreEnv* /* penv */)
{
    TRACE( TRACE_DEBUG, "Preparing for new run...\n");
    for (uint i=0; i<_irptp_vec.size(); i++) {
        W_DO(_irptp_vec[i]->prepareNewRun());
    }
    return (RCOK);
}



/****************************************************************** 
 *
 * @fn:    _dump()
 *
 * @brief: Calls the dump for all the tables
 *
 ******************************************************************/

int DoraEnv::_dump(ShoreEnv* /* penv */)
{
    uint sz=_irptp_vec.size();
    TRACE( TRACE_ALWAYS, "Tables  = (%d)\n", sz);
    for (uint i=0; i<sz; i++) {
        _irptp_vec[i]->dump();
    }
    return (0);
}


/****************************************************************** 
 *
 * @fn:    _info()
 *
 * @brief: Calls the info for all the tables
 *
 ******************************************************************/

int DoraEnv::_info(const ShoreEnv* penv) const
{
    TRACE( TRACE_ALWAYS, "SF      = (%.1f)\n", penv->get_sf());
    uint sz=_irptp_vec.size();
    TRACE( TRACE_ALWAYS, "Tables  = (%d)\n", sz);
    for (uint i=0;i<sz;++i) {
        _irptp_vec[i]->info();
    }
    return (0);
}



/****************************************************************** 
 *
 * @fn:    _next_cpu()
 *
 * @brief: Deciding the distribution of tables
 *
 * @note:  Very simple (just increases processor id by DF_CPU_STEP) 
 *
 * @note:  This decision  can be based among others on:
 *
 *         - aprd                    - the current cpu
 *         - this->_max_cpu_count    - the maximum cpu count (hard-limit)
 *         - this->_active_cpu_count - the active cpu count (soft-limit)
 *         - this->_start_prs_id     - the first assigned cpu for the table
 *         - this->_prs_range        - a range of cpus assigned for the table
 *         - this->_sf               - the scaling factor of the db
 *         - this->_vec.size()       - the number of tables
 *         - atable                  - the current table
 *
 ******************************************************************/

processorid_t DoraEnv::_next_cpu(const processorid_t& aprd,
                                 const irpTableImpl* /* atable */,
                                 const int step)
{    
    int binding = envVar::instance()->getVarInt("dora-cpu-binding",0);
    if (binding==0)
        return (PBIND_NONE);

    int activecpu = envVar::instance()->getVarInt("active-cpu-count",64);
    processorid_t nextprs = ((aprd+step) % activecpu);
    TRACE( TRACE_DEBUG, "(%d) -> (%d)\n", aprd, nextprs);
    return (nextprs);
}


EXIT_NAMESPACE(dora);
