/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
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

/*<std-header orig-src='shore' incl-file-exclusion='BTREE_H'>

 $Id: btree.h,v 1.132 2010/05/26 01:20:36 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef BTREE_H
#define BTREE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 *  Interface to btree manager.  
 *  NB: put NO INLINE FUNCTIONS here.
 *  Implementation is class btree_impl, in btree_impl.[ch].
 */
#ifdef __GNUG__
#pragma interface
#endif

class btree_p;
struct btree_stats_t;
class bt_cursor_t;
struct btree_lf_stats_t;
struct btree_int_stats_t;
class w_keystr_t;
class verify_volume_result;
/**
 * Data access API for B+Tree.
 * \ingroup SSMBTREE
 */
class btree_m : public smlevel_2 {
    friend class btree_p;
    friend class btree_impl;
    friend class bt_cursor_t;
    friend class btree_remove_log;
    friend class btree_insert_log;
    friend class btree_update_log;
    friend class btree_overwrite_log;
    friend class btree_ghost_mark_log;
    friend class btree_ghost_reclaim_log;

public:
    NORET                        btree_m()   {};
    NORET                        ~btree_m()  {};
    void                         construct_once();
    void                         destruct_once();

    static smsize_t                max_entry_size(); 

    /** Create a btree. Return the root page id in root. */
    static rc_t                        create(
        const stid_t &              stid,
        lpid_t&                     root
        );

    /**
    * Insert <key, el> into the btree.
    */
    static rc_t                        insert(
        const lpid_t&                     root,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);

    /**
    * Update el of key with the new data.
    */
    static rc_t                        update(
        const lpid_t&                     root,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);

    /**
    * Put <key, el> into the btree; if key didn't exist, inserts it, otherwise updates el of key
    * with the new data.
    */
    static rc_t                        put(
        const lpid_t&                     root,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);

    /**
    * Update specific part of el of key with the new data.
    */
    static rc_t                        overwrite(
        const lpid_t&                     root,
        const w_keystr_t&                 key,
        const char*                       el,
        smsize_t                          offset,
        smsize_t                          elen);

    /** Remove key from the btree. */
    static rc_t                        remove(
        const lpid_t&                    root,
        const w_keystr_t&                    key);

    /** Print the btree (for debugging only). */
    static void                 print(const lpid_t& root,  bool print_elem = true);

    /**
     * \brief Defrags the given page to remove holes and ghost records in the page.
     * \ingroup SSMBTREE
     * @copydetails btree_impl::_sx_defrag_page
    */
    static rc_t                 defrag_page(const lpid_t &pid);

    /**
    * Find key in btree. If found, copy up to elen bytes of the 
    *  entry element into el. 
    */
    static rc_t                        lookup(
        const lpid_t&                  root, 
        const w_keystr_t&              key_to_find, 
        void*                          el, 
        smsize_t&                      elen,
        bool&                          found);

    static rc_t                 get_du_statistics(
        const lpid_t&                    root, 
        btree_stats_t&                btree_stats,
        bool                            audit);

    /**
    *  Verifies the integrity of whole tree using the fence-key bitmap technique.
     * @copydetails btree_impl::_ux_verify_tree(const lpid_t&,int,bool&)
    */
    static rc_t                        verify_tree(
        const lpid_t &root_pid, int hash_bits, bool &consistent);
    
    /**
     * \brief Verifies consistency of all BTree indexes in the volume.
     * @copydetails btree_impl::_ux_verify_volume()
     */
    static rc_t            verify_volume(
        vid_t vid, int hash_bits, verify_volume_result &result);
protected:
    /* 
     * for use by logrecs for undo
     */
    static rc_t remove_as_undo(const lpid_t &root, const w_keystr_t &key);
    static rc_t update_as_undo(const lpid_t &root, const w_keystr_t &key, const cvec_t &elem);
    static rc_t overwrite_as_undo(const lpid_t &root, const w_keystr_t &key,
                                  const char *el, smsize_t offset, smsize_t elen);
    static rc_t undo_ghost_mark(const lpid_t &root, const w_keystr_t &key);
private:
    /** Return true in ret if btree at root is empty. false otherwise. */
    static rc_t                        is_empty(const lpid_t& root, bool& ret);

    /** Used by get_du_statistics internally to collect all nodes' statistics. */
    static rc_t _get_du_statistics_recurse(
        const lpid_t&        currentpid,
        btree_stats_t&        _stats,
        base_stat_t        &lf_cnt,
        base_stat_t        &int_cnt,
        btree_lf_stats_t        &lf_stats,
        btree_int_stats_t       &int_stats,
        bool                 audit);
};

/*<std-footer incl-file-exclusion='BTREE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
