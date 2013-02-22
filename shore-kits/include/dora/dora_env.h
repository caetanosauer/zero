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

/** @file:   dora_env.h
 *
 *  @brief:  A generic Dora environment class
 *
 *  @note:   All the database need to inherit both from a shore_env, as well as, a dora_env
 *
 *  @author: Ippokratis Pandis, Jun 2009
 */


#ifndef __DORA_ENV_H
#define __DORA_ENV_H


#include <cstdio>

#include "tls.h"
#include "util.h"
#include "shore.h"

#include "dora/range_table_i.h"

#include "dora/dflusher.h"

using namespace shore;

ENTER_NAMESPACE(dora);




/******************************************************************** 
 *
 * @class: dora_env
 *
 * @brief: Generic container class for all the data partitions for
 *         DORA databases. 
 *
 * @note:  All the DORA databases so far use range partitioning over a 
 *         single integer (the SF number) as the identifier. This version 
 *         of DoraEnv is customized for this. That is, the partitioning 
 *         is only range, and DataType = int.
 *
 ********************************************************************/

class DoraEnv
{
public:

    typedef range_table_i<int>          irpTableImpl;
    typedef std::vector<irpTableImpl*>  irpTablePtrVector;
    typedef irpTablePtrVector::iterator irpTablePtrVectorIt;

    typedef partition_t<int>            irpImpl;
    typedef action_t<int>               irpAction;
    typedef vector<base_action_t*>      baseActionsList;

protected:

    // The type of DORA environment: (plain) dora, (normal) plp, 
    // plpp (plp-part), plpl (plp-leaf)
    uint _dtype;

    // A vector of pointers to integer-range-partitioned tables
    irpTablePtrVector _irptp_vec;    

    // Setup variables
    int _starting_cpu;
    int _cpu_table_step;
    int _cpu_range;

    // The dora-flusher thread
    guard<dora_flusher_t> _flusher;

public:
    
    DoraEnv();

    virtual ~DoraEnv();

    // Type-related calls
    uint dtype() const;
    bool is_dora() const;
    bool is_plp() const;


    //// Client API

    // Return the partition responsible for the specific integer identifier
    inline irpImpl* decide_part(irpTableImpl* atable, const int aid) {
        cvec_t key((char*)&aid,sizeof(int));
        lpid_t pid;
        w_rc_t r = atable->getPartIdxByKey(key,pid);
        if (r.is_error()) { assert(false); return (NULL); }
        return (atable->get(pid.page));
    }      


    inline void enqueue_toflush(terminal_rvp_t* arvp) {
        assert (_flusher.get());
        _flusher->enqueue_toflush(arvp);
    }


protected:

    uint _check_type();
    uint update_pd(ShoreEnv* penv);

    int _post_start(ShoreEnv* penv);
    int _post_stop(ShoreEnv* penv);
    w_rc_t _newrun(ShoreEnv* penv);
    int _dump(ShoreEnv* penv);
    int _info(const ShoreEnv* penv) const;
    int _statistics(ShoreEnv* penv);

    // algorithm for deciding the distribution of tables 
    processorid_t _next_cpu(const processorid_t& aprd,
                            const irpTableImpl* atable,
                            const int step=DF_CPU_STEP_TABLES);
    
}; // EOF: DoraEnv



EXIT_NAMESPACE(dora);

#endif /** __DORA_ENV_H */

