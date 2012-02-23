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

/*<std-header orig-src='shore' incl-file-exclusion='PAGE_S_H'>

 $Id: page_s.h,v 1.34 2010/07/26 23:37:12 nhall Exp $

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

#ifndef PAGE_S_H
#define PAGE_S_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

class xct_t;
/**\brief Basic page structure for all pages.
 * \details
 * These are persistent things. There is no hierarchy here
 * for the different page types. All the differences between
 * page types are handled by the handle classes, page_p and its
 * derived classes.
 * 
 * \section BTree-specific page headers
 * This page layout also contains the headers just for BTree to optimize on
 * the performance of BTree.
 * Anyways, remaining page-types other than BTree are only stnode and extlink.
 * For those page types, this header part is unused but not a big issue.
 * @see btree_p
 */
class page_s {
public:
    typedef int16_t  slot_offset_t; // negative value means ghost records.
    typedef uint16_t slot_length_t;
    typedef slot_offset_t  slot_index_t; // to avoid explicit sized-types below
    
    enum {
        hdr_sz = 64, // NOTICE always sync with the offsets below
        data_sz = smlevel_0::page_sz - hdr_sz,
        slot_sz = sizeof(slot_offset_t) + sizeof(slot_length_t),
        /** Poor man's normalized key length. */
        poormkey_sz = 2 
    };

 
    /** LSN (Log Sequence Number) of the last write to this page. */
    lsn_t    lsn;      // +8 -> 8
    
    /** id of the page.*/
    lpid_t    pid;      // +12 -> 20
    
    /** page_p::tag_t. */
    uint16_t    tag;     // +2 -> 22
    
    /** total number of slots including every type of slot. */
    slot_index_t  nslots;   // +2 -> 24
    
    /** number of ghost records. */
    slot_offset_t  nghosts; // +2 -> 26
    
    /** offset to beginning of record area (location of record that is located left-most). */
    slot_offset_t  record_head;     // +2 -> 28

    /** page_p::store_flag_t. */
    uint32_t    _private_store_flags; // +4 -> 32

    uint32_t    get_page_storeflags() const { return _private_store_flags;}
    uint32_t    set_page_storeflags(uint32_t f) { 
                         return (_private_store_flags=f);}
    /** page_p::page_flag_t. */
    uint32_t    page_flags;        //  +4 -> 36

#ifdef DOXYGEN_HIDE
///==========================================
///   BEGIN: BTree specific headers
///==========================================
#endif // DOXYGEN_HIDE

    /**
    * root page used for recovery (root page is never changed even while grow/shrink).
    * This can be removed by retrieving it from full pageid (storeid->root page id),
    * but let's do that later.
    */
    shpid_t    btree_root; // +4 -> 40
    /** first ptr in non-leaf nodes. used only in left-most non-leaf nodes. */
    shpid_t    btree_pid0; // +4 -> 44
    /**
    * B-link page (0 if not linked).
    * kind of "next", but other nodes don't know about it yet.
    */
    shpid_t    btree_blink;  // +4 -> 48
    /** 1 if leaf, >1 if non-leaf. */
    int16_t    btree_level; // +2 -> 50
    /**
    * length of low-fence key.
    * Corresponding data is stored in the first slot.
    */
    int16_t    btree_fence_low_length;  // +2 -> 52
    /**
    * length of high-fence key.
    * Corresponding data is stored in the first slot after low fence key.
    */
    int16_t    btree_fence_high_length;  // +2 -> 54
    /**
     * length of high-fence key of the Blink chain. 0 if not in a Blink chain or right-most of a chain.
     * Corresponding data is stored in the first slot after high fence key.
     * When this page belongs to a Blink chain,
     * we need to store high-fence of right-most sibling in every sibling
     * to do batch-verification with bitmaps.
     * @see btree_impl::_ux_verify_volume()
     */
    int16_t    btree_chain_fence_high_length; // +2 -> 56
    /**
    * counts of common leading bytes of the fence keys,
    * thereby of all entries in this page too.
    * 0=no prefix compression.
    * Corresponding data is NOT stored.
    * We can just use low fence key data.
    */
    int16_t    btree_prefix_length;  // +2 -> 58
    /**
    * Count of consecutive insertions to right-most or left-most.
    * Positive values mean skews towards right-most.
    * Negative values mean skews towards left-most.
    * Whenever this page receives an insertion into the middle,
    * this value is reset to zero.
    * Changes of this value will NOT be logged. It doesn't matter
    * in terms of correctness, so we don't care about undo/redo
    * of this header item.
    */
    int16_t   btree_consecutive_skewed_insertions; // +2 -> 60
#ifdef DOXYGEN_HIDE
///==========================================
///   END: BTree specific headers
///==========================================
#endif // DOXYGEN_HIDE

    /**
     * Checksum of this page.
     * Checksum is calculated from various parts of this page
     * and stored when this page is written out to disk from bufferpool.
     * It's checked when the bufferpool first reads the page from
     * the disk and not checked afterwards.
     */
    mutable uint32_t    checksum; // +4 -> 64
    
    /** Calculate the correct value of checksum of this page. */
    uint32_t    calculate_checksum () const;
    /**
     * Renew the stored value of checksum of this page.
     * Note that this is a const function. checksum is mutable property.
     */
    void       update_checksum () const {checksum = calculate_checksum();}

    /* MUST BE 8-BYTE ALIGNED HERE */
    char     data[data_sz];        // must be aligned

    page_s() {
        w_assert1(data - (const char *) this == hdr_sz);
    }
    ~page_s() { }
};

const uint32_t PAGE_S_CHECKSUM_MULT = 0x35D0B891;

inline uint32_t page_s::calculate_checksum () const
{
    const unsigned char *end_p = (const unsigned char *) (data + SM_PAGESIZE - sizeof(uint32_t));
    uint64_t value = 0;
    // these values(23/511) are arbitrary, but make sure it doesn't touch
    // the checksum field (located around 60-th bytes) itself!
    for (const unsigned char *p = (const unsigned char *) data + 23; p < end_p; p += 511) {
        // be aware of alignment issue on spark! so this code is not safe
        // const uint32_t*next = reinterpret_cast<const uint32_t*>(p);
        // value ^= *next;
        value = value * PAGE_S_CHECKSUM_MULT + p[0];
        value = value * PAGE_S_CHECKSUM_MULT + p[1];
        value = value * PAGE_S_CHECKSUM_MULT + p[2];
        value = value * PAGE_S_CHECKSUM_MULT + p[3];
    }
    return ((uint32_t) (value >> 32)) ^ ((uint32_t) (value & 0xFFFFFFFF));
}

/*<std-footer incl-file-exclusion='PAGE_S_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
