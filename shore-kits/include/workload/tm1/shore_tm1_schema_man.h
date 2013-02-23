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

/** @file:   shore_tm1_schema_man.h
 *
 *  @brief:  Declaration of the TM1 table managers
 *
 *  @author: Ippokratis Pandis, Feb 2008
 *
 */

#ifndef __SHORE_TM1_SCHEMA_MANAGER_H
#define __SHORE_TM1_SCHEMA_MANAGER_H


#include "workload/tm1/shore_tm1_schema.h"

using namespace shore;


ENTER_NAMESPACE(tm1);



/* ---------------------------------------------------------------- */
/* --- The managers of all the tables used in the TM1 benchmark --- */
/* ---------------------------------------------------------------- */


class sub_man_impl : public table_man_impl<subscriber_t>
{
    typedef table_row_t sub_tuple;
    typedef index_scan_iter_impl<subscriber_t> sub_idx_iter;

public:

    sub_man_impl(subscriber_t* aSubscriberDesc)
        : table_man_impl<subscriber_t>(aSubscriberDesc)
    { }
    ~sub_man_impl() { }

    /* --- access specific tuples  --- */
    w_rc_t sub_idx_probe(ss_m* db, 
                         sub_tuple* ptuple, 
                         const int s_id);
    
    w_rc_t sub_idx_upd(ss_m* db, 
                       sub_tuple* ptuple, 
                       const int s_id);
    
    w_rc_t sub_idx_nl(ss_m* db, 
                      sub_tuple* ptuple, 
                      const int s_id);

    // secondary index
    w_rc_t sub_nbr_idx_probe(ss_m* db, 
                             sub_tuple* ptuple, 
                             const char* s_nbr);
    
    w_rc_t sub_nbr_idx_upd(ss_m* db, 
                           sub_tuple* ptuple, 
                           const char* s_nbr);
    
    w_rc_t sub_nbr_idx_nl(ss_m* db, 
                          sub_tuple* ptuple, 
                          const char* s_nbr);



    /* --- access specific tuples with iter  --- */
    w_rc_t sub_get_idx_iter(ss_m* db,
                            sub_idx_iter* &iter,
                            sub_tuple* ptuple,
                            rep_row_t &replow,
                            rep_row_t &rephigh,
                            const int sub_id,
                            const uint range,
                            lock_mode_t alm = SH,
                            bool need_tuple = true);
    
    
}; // EOF: sub_man_impl



class ai_man_impl : public table_man_impl<access_info_t>
{
    typedef table_row_t ai_tuple;

public:

    ai_man_impl(access_info_t* aAIDesc)
        : table_man_impl<access_info_t>(aAIDesc)
    { }
    ~ai_man_impl() { }

    /* --- access specific tuples  --- */
    w_rc_t ai_idx_probe(ss_m* db, 
                        ai_tuple* ptuple, 
                        const int s_id, const short ai_type);
    
    w_rc_t ai_idx_upd(ss_m* db, 
                      ai_tuple* ptuple, 
                      const int s_id, const short ai_type);
    
    w_rc_t ai_idx_nl(ss_m* db, 
                     ai_tuple* ptuple, 
                     const int s_id, const short ai_type);
    
}; // EOF: ai_man_impl



class sf_man_impl : public table_man_impl<special_facility_t>
{
    typedef table_row_t sf_tuple;
    typedef index_scan_iter_impl<special_facility_t> sf_idx_iter;

public:

    sf_man_impl(special_facility_t* aSFDesc)
        : table_man_impl<special_facility_t>(aSFDesc)
    { }
    ~sf_man_impl() { }


    /* --- access specific tuples  --- */
    w_rc_t sf_idx_probe(ss_m* db, 
                        sf_tuple* ptuple, 
                        const int s_id, const short sf_type);
    
    w_rc_t sf_idx_upd(ss_m* db, 
                      sf_tuple* ptuple, 
                      const int s_id, const short sf_type);
    
    w_rc_t sf_idx_nl(ss_m* db, 
                     sf_tuple* ptuple, 
                     const int s_id, const short sf_type);


    /* --- access specific tuples with iter  --- */
    w_rc_t sf_get_idx_iter(ss_m* db,
                           sf_idx_iter* &iter,
                           sf_tuple* ptuple,
                           rep_row_t &replow,
                           rep_row_t &rephigh,
                           const int sub_id,
                           lock_mode_t alm = SH,
                           bool need_tuple = true);

    w_rc_t sf_get_idx_iter_nl(ss_m* db,
                              sf_idx_iter* &iter,
                              sf_tuple* ptuple,
                              rep_row_t &replow,
                              rep_row_t &rephigh,
                              const int sub_id,
                              bool need_tuple = true);
    
}; // EOF: sf_man_impl



class cf_man_impl : public table_man_impl<call_forwarding_t>
{
    typedef table_row_t cf_tuple;
    typedef index_scan_iter_impl<call_forwarding_t> cf_idx_iter;

public:

    cf_man_impl(call_forwarding_t* aCFDesc)
        : table_man_impl<call_forwarding_t>(aCFDesc)
    { }
    ~cf_man_impl() { }


    /* --- access specific tuples with probe  --- */
    w_rc_t cf_idx_probe(ss_m* db, 
                        cf_tuple* ptuple, 
                        const int s_id, const short sf_type, const short stime);
    
    w_rc_t cf_idx_upd(ss_m* db, 
                      cf_tuple* ptuple, 
                      const int s_id, const short sf_type, const short stime);
    
    w_rc_t cf_idx_nl(ss_m* db, 
                     cf_tuple* ptuple, 
                     const int s_id, const short sf_type, const short stime);


    /* --- access specific tuples with iter  --- */
    w_rc_t cf_get_idx_iter(ss_m* db,
                           cf_idx_iter* &iter,
                           cf_tuple* ptuple,
                           rep_row_t &replow,
                           rep_row_t &rephigh,
                           const int sub_id,
                           const short sf_type,
                           const short s_time,
                           lock_mode_t alm = SH,
                           bool need_tuple = true);

    w_rc_t cf_get_idx_iter_nl(ss_m* db,
                              cf_idx_iter* &iter,
                              cf_tuple* ptuple,
                              rep_row_t &replow,
                              rep_row_t &rephigh,
                              const int sub_id,
                              const short sf_type,
                              const short s_time,
                              bool need_tuple = true);

    
    
}; // EOF: sf_man_impl


EXIT_NAMESPACE(tm1);

#endif /* __SHORE_TM1_SCHEMA_MANAGER_H */
