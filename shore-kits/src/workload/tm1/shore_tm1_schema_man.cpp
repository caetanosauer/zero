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

/** @file:   shore_tpce_schema.cpp
 *
 *  @brief:  Definition of the TM1 tables
 *
 *  @author: Ippokratis Pandis
 *
 */

#include "workload/tm1/shore_tm1_schema_man.h"

using namespace shore;


ENTER_NAMESPACE(tm1);


/*********************************************************************
 *
 * Workload-specific access methods on tables
 *
 *********************************************************************/


/* ------------------- */
/* --- SUBSCRIBERS --- */
/* ------------------- */


w_rc_t sub_man_impl::sub_idx_probe(ss_m* db,
                                   sub_tuple* ptuple,
                                   const int s_id)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    return (index_probe_by_name(db, "S_IDX", ptuple));
}

w_rc_t sub_man_impl::sub_idx_upd(ss_m* db,
                                 sub_tuple* ptuple,
                                 const int s_id)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    return (index_probe_forupdate_by_name(db, "S_IDX", ptuple));
}

w_rc_t sub_man_impl::sub_idx_nl(ss_m* db,
                                sub_tuple* ptuple,
                                const int s_id)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    return (index_probe_nl_by_name(db, "S_IDX", ptuple));
}



w_rc_t sub_man_impl::sub_nbr_idx_probe(ss_m* db,
                                       sub_tuple* ptuple,
                                       const char* s_nbr)
{
    assert (ptuple);    
    ptuple->set_value(1, s_nbr);
    return (index_probe_by_name(db, "SUB_NBR_IDX", ptuple));
}

w_rc_t sub_man_impl::sub_nbr_idx_upd(ss_m* db,
                                     sub_tuple* ptuple,
                                     const char* s_nbr)
{
    assert (ptuple);    
    ptuple->set_value(1, s_nbr);
    return (index_probe_forupdate_by_name(db, "SUB_NBR_IDX", ptuple));
}

w_rc_t sub_man_impl::sub_nbr_idx_nl(ss_m* db,
                                    sub_tuple* ptuple,
                                    const char* s_nbr)
{
    assert (ptuple);    
    ptuple->set_value(1, s_nbr);
    return (index_probe_nl_by_name(db, "SUB_NBR_IDX", ptuple));
}



w_rc_t sub_man_impl::sub_get_idx_iter(ss_m* db,
                                      sub_idx_iter* &iter,
                                      sub_tuple* ptuple,
                                      rep_row_t &replow,
                                      rep_row_t &rephigh,
                                      const int sub_id,
                                      const uint range,
                                      lock_mode_t alm,
                                      bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("SUB_NBR_IDX");
    assert (pindex);

    int lowsz, highsz;
    char aSubNbr[STRSIZE(TM1_SUB_NBR_SZ)];
    memset(aSubNbr,0,STRSIZE(TM1_SUB_NBR_SZ));
	
    // SUB_NBR_IDX: {1}

    // Low bound
    sprintf(aSubNbr,"%015d",sub_id);
    ptuple->set_value(1, aSubNbr);

#ifdef USE_DORA_EXT_IDX
    // The extended DORA index: SUB_NBR_IDX: {1 - 0}
    if(!need_tuple) { ptuple->set_value(0, 0); }
#endif

    lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    // High bound
    sprintf(aSubNbr,"%015d",(sub_id+range));
    ptuple->set_value(1, aSubNbr);
	
#ifdef USE_DORA_EXT_IDX
    if(!need_tuple) { ptuple->set_value(0, MAX_INT); } // largest S_ID
#endif

    highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


/* ------------------- */
/* --- ACCESS_INFO --- */
/* ------------------- */


w_rc_t ai_man_impl::ai_idx_probe(ss_m* db,
                                 ai_tuple* ptuple,
                                 const int s_id, const short ai_type)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    ptuple->set_value(1, ai_type);
    return (index_probe_by_name(db, "AI_IDX", ptuple));
}

w_rc_t ai_man_impl::ai_idx_upd(ss_m* db,
                               ai_tuple* ptuple,
                               const int s_id, const short ai_type)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    ptuple->set_value(1, ai_type);
    return (index_probe_forupdate_by_name(db, "AI_IDX", ptuple));
}

w_rc_t ai_man_impl::ai_idx_nl(ss_m* db,
                              ai_tuple* ptuple,
                              const int s_id, const short ai_type)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    ptuple->set_value(1, ai_type);
    return (index_probe_nl_by_name(db, "AI_IDX", ptuple));
}



/* ------------------------ */
/* --- SPECIAL_FACILITY --- */
/* ------------------------ */


w_rc_t sf_man_impl::sf_idx_probe(ss_m* db,
                                 sf_tuple* ptuple,
                                 const int s_id, const short sf_type)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    ptuple->set_value(1, sf_type);
    return (index_probe_by_name(db, "SF_IDX", ptuple));
}

w_rc_t sf_man_impl::sf_idx_upd(ss_m* db,
                               sf_tuple* ptuple,
                               const int s_id, const short sf_type)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    ptuple->set_value(1, sf_type);
    return (index_probe_forupdate_by_name(db, "SF_IDX", ptuple));
}

w_rc_t sf_man_impl::sf_idx_nl(ss_m* db,
                              sf_tuple* ptuple,
                              const int s_id, const short sf_type)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    ptuple->set_value(1, sf_type);
    return (index_probe_nl_by_name(db, "SF_IDX", ptuple));
}




w_rc_t sf_man_impl::sf_get_idx_iter(ss_m* db,
                                    sf_idx_iter* &iter,
                                    sf_tuple* ptuple,
                                    rep_row_t &replow,
                                    rep_row_t &rephigh,
                                    const int sub_id,
                                    lock_mode_t alm,
                                    bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("SF_IDX");
    assert (pindex);

    // CF_IDX: { 0 - 1 }

    // prepare the key to be probed
    ptuple->set_value(0, sub_id);
    ptuple->set_value(1, (short)1); // smallest SF_TYPE (1-4)

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    ptuple->set_value(1, (short)4); // largest SF_TYPE (1-4)

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t sf_man_impl::sf_get_idx_iter_nl(ss_m* db,
                                       sf_idx_iter* &iter,
                                       sf_tuple* ptuple,
                                       rep_row_t &replow,
                                       rep_row_t &rephigh,
                                       const int sub_id,
                                       bool need_tuple)
{
    return (sf_get_idx_iter(db,iter,ptuple,replow,rephigh,sub_id,NL,need_tuple));
}




/* ----------------------- */
/* --- CALL_FORWARDING --- */
/* ----------------------- */


w_rc_t cf_man_impl::cf_idx_probe(ss_m* db,
                                 cf_tuple* ptuple,
                                 const int s_id, const short sf_type, 
                                 const short stime)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    ptuple->set_value(1, sf_type);
    ptuple->set_value(2, stime);
    return (index_probe_by_name(db, "CF_IDX", ptuple));
}

w_rc_t cf_man_impl::cf_idx_upd(ss_m* db,
                               cf_tuple* ptuple,
                               const int s_id, const short sf_type, 
                               const short stime)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    ptuple->set_value(1, sf_type);
    ptuple->set_value(2, stime);
    return (index_probe_forupdate_by_name(db, "CF_IDX", ptuple));
}

w_rc_t cf_man_impl::cf_idx_nl(ss_m* db,
                              cf_tuple* ptuple,
                              const int s_id, const short sf_type, 
                              const short stime)
{
    assert (ptuple);    
    ptuple->set_value(0, s_id);
    ptuple->set_value(1, sf_type);
    ptuple->set_value(2, stime);
    return (index_probe_nl_by_name(db, "CF_IDX", ptuple));
}




w_rc_t cf_man_impl::cf_get_idx_iter(ss_m* db,
                                    cf_idx_iter* &iter,
                                    cf_tuple* ptuple,
                                    rep_row_t &replow,
                                    rep_row_t &rephigh,
                                    const int sub_id,
                                    const short sf_type,
                                    const short s_time,
                                    lock_mode_t alm,
                                    bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("CF_IDX");
    assert (pindex);

    // CF_IDX: {0 - 1 - 2}

    // prepare the key to be probed
    ptuple->set_value(0, sub_id);
    ptuple->set_value(1, sf_type);
    ptuple->set_value(2, s_time);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);


    ptuple->set_value(2, (short)24); // largest S_TIME

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t cf_man_impl::cf_get_idx_iter_nl(ss_m* db,
                                       cf_idx_iter* &iter,
                                       cf_tuple* ptuple,
                                       rep_row_t &replow,
                                       rep_row_t &rephigh,
                                       const int sub_id,
                                       const short sf_type,
                                       const short s_time,
                                       bool need_tuple)
{
    return (cf_get_idx_iter(db,iter,ptuple,replow,rephigh,sub_id,sf_type,s_time,NL,need_tuple));
}


EXIT_NAMESPACE(tm1);
