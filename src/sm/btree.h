/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#ifndef BTREE_H
#define BTREE_H

/*
 *  Interface to btree manager.
 *  NB: put NO INLINE FUNCTIONS here.
 *  Implementation is class btree_impl, in btree_impl.[ch].
 */
#include "w_defines.h"
#include "vid_t.h"
#include "stid_t.h"

class btree_page_h;
struct btree_stats_t;
class bt_cursor_t;
struct btree_lf_stats_t;
struct btree_int_stats_t;
class w_keystr_t;
class verify_volume_result;
struct okvl_mode;
/**
 * Data access API for B+Tree.
 * \ingroup SSMBTREE
 */
class btree_m : public smlevel_0 {
    friend class btree_page_h;
    friend class btree_impl;
    friend class bt_cursor_t;
    friend class btree_remove_log;
    friend class btree_insert_log;
    friend class btree_insert_nonghost_log;
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
        const lpid_t&               root
        );

    /**
    * Insert <key, el> into the btree.
    */
    static rc_t                        insert(
        stid_t store,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);

    /**
    * Update el of key with the new data.
    */
    static rc_t                        update(
        stid_t store,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);

    /**
    * Put <key, el> into the btree; if key didn't exist, inserts it, otherwise updates el of key
    * with the new data.
    */
    static rc_t                        put(
        stid_t store,
        const w_keystr_t&                 key,
        const cvec_t&                     elem);

    /**
    * Update specific part of el of key with the new data.
    */
    static rc_t                        overwrite(
        stid_t store,
        const w_keystr_t&                 key,
        const char*                       el,
        smsize_t                          offset,
        smsize_t                          elen);

    /** Remove key from the btree. */
    static rc_t                        remove(
        stid_t store,
        const w_keystr_t&                    key);

    /** Print the btree (for debugging only). */
    static void                 print(const lpid_t& root,  bool print_elem = true);

    /** Touch all pages in the btree (for performance experiments). */
    static rc_t                 touch_all(stid_t stid, uint64_t &page_count);
    static rc_t                 touch(const btree_page_h& page, uint64_t &page_count);

    /**
     * \brief Defrags the given page to remove holes and ghost records in the page.
     * \ingroup SSMBTREE
     * @copydetails btree_impl::_sx_defrag_page
    */
    static rc_t                 defrag_page(btree_page_h &page);

    /**
    * Find key in btree. If found, copy up to elen bytes of the
    *  entry element into el.
    */
    static rc_t                        lookup(
        stid_t store,
        const w_keystr_t&              key_to_find,
        void*                          el,
        smsize_t&                      elen,
        bool&                          found);

    static rc_t                 get_du_statistics(
        const lpid_t &root_pid,
        btree_stats_t&                btree_stats,
        bool                            audit);

    /**
    *  Verifies the integrity of whole tree using the fence-key bitmap technique.
     * @copydetails btree_impl::_ux_verify_tree(const lpid_t&,int,bool&)
    */
    static rc_t                        verify_tree(
        stid_t store, int hash_bits, bool &consistent);

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
    static rc_t remove_as_undo(stid_t store,const w_keystr_t &key);
    static rc_t update_as_undo(stid_t store,const w_keystr_t &key, const cvec_t &elem);
    static rc_t overwrite_as_undo(stid_t store,const w_keystr_t &key,
                                  const char *el, smsize_t offset, smsize_t elen);
    static rc_t undo_ghost_mark(stid_t store,const w_keystr_t &key);
private:
    /** Return true in ret if btree at root is empty. false otherwise. */
    static rc_t                        is_empty(stid_t store, bool& ret);

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
