/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef GENERIC_PAGE_H
#define GENERIC_PAGE_H

#include "w_defines.h"
#include "sm_s.h"


/**
 * \brief Page headers shared by all Zero pages
 *
 * \details 
 *     All page datatypes (e.g., generic_page, alloc_page, btree_page,
 * stnode_page) inherit (indirectly) from this class.  This is a POD.
 *
 *     The checksum field is placed first to make the region of data
 * it covers continuous.  The order of the other fields has been
 * arranged to maximize packing into the fewest number of words
 * possible.
 *
 *     Because this class contains a 8-byte aligned object (lsn_t), it
 * must be a multiple of eight bytes.  The extra space not needed for
 * all pages (field reserved) is reserved for subclass usage.
 *
 *     Some page types (alloc_page, stnode_page) do not currently
 * (9/2013) use the lsn, reserved, or page_flags fields.
 */
class generic_page_header {
public:
    /// Size of all Zero pages
    static const size_t page_sz = SM_PAGESIZE;


    /**
     * \brief Checksum of this page.
     *
     * \details
     * Checksum is calculated from various parts of this page and
     * stored when this page is written out to disk.  It is checked
     * each time this page is read in from the disk.
     */
    mutable uint32_t checksum;     // +4 -> 4
    
    /// ID of the page
    lpid_t           pid;          // +4+4+4 (= +12) -> 16
    
    /// LSN (Log Sequence Number) of the last write to this page
    lsn_t            lsn;          // +8 -> 24

    /// Page type (a page_tag_t)
    uint16_t         tag;          // +2 -> 26

protected:
    friend class fixable_page_h;   // for access to page_flags&t_tobedeleted

    /// Page flags (an OR of page_flag_t's)
    uint16_t         page_flags;   //  +2 -> 28

    /// Reserved for subclass usage
    uint32_t         reserved;     //  +4 -> 32


public:
    /// Calculate the correct value of checksum for this page. 
    uint32_t    calculate_checksum () const;
};


/**
 * \brief The type of a page; e.g., is this a B-tree page, an
 * allocation page, or what?
 */
enum page_tag_t {
    t_bad_p    = 0,        ///< not used
    t_alloc_p  = 1,        ///< free-page allocation page 
    t_stnode_p = 2,        ///< store node page
    t_btree_p  = 5,        ///< btree page 
};


/**
 * \brief Flags that can be turned on or off per page; held in
 * generic_page_header::page_flags.
 */
enum page_flag_t {
    // Flags used by fixable pages:
    t_tobedeleted  = 0x01,     ///< this page will be deleted as soon as the page is evicted from bufferpool
};



inline uint32_t generic_page_header::calculate_checksum () const {
    const uint32_t CHECKSUM_MULT = 0x35D0B891;

    // FIXME: The current checksum ignores the headers and most of the
    // data bytes, presumably for speed reasons.  If you start
    // checksumming the headers, be careful of the checksum field.

    const unsigned char *data      = (const unsigned char *)(this + 1);          // start of data section of this page: right after these headers
    const unsigned char *data_last = (const unsigned char *)(this) + page_sz - sizeof(uint32_t);  // the last 32-bit word of the data section of this page

    uint64_t value = 0;
    // these values (23/511) are arbitrary
    for (const unsigned char *p = (const unsigned char *) data + 23; p <= data_last; p += 511) {
        // be aware of alignment issue on spark! so this code is not safe
        // const uint32_t*next = reinterpret_cast<const uint32_t*>(p);
        // value ^= *next;
        value = value * CHECKSUM_MULT + p[0];
        value = value * CHECKSUM_MULT + p[1];
        value = value * CHECKSUM_MULT + p[2];
        value = value * CHECKSUM_MULT + p[3];
    }
    return ((uint32_t) (value >> 32)) ^ ((uint32_t) (value & 0xFFFFFFFF));
}



/**
 * \brief Basic page structure for all pages.
 * 
 * \details
 * These are persistent things. There is no hierarchy here
 * for the different page types. All the differences between
 * page types are handled by the handle classes, generic_page_h and its
 * derived classes.
 * 
 * \section BTree-specific page headers
 * This page layout also contains the headers just for BTree to optimize on
 * the performance of BTree.
 * Anyways, remaining page-types other than BTree are only stnode_page and alloc_page
 * For those page types, this header part is unused but not a big issue.
 * @see btree_page
 */
class generic_page : public generic_page_header {
private:
    char undefined[page_sz - sizeof(generic_page_header)];
};



/**
 *  Basic page handle class.
 */
class generic_page_h {
public:
    generic_page_h(generic_page* s) : _pp(s) {
        // verify compiler tightly packed all of generic_page_header's fields:
        w_assert1(sizeof(generic_page_header) == 32);
        w_assert1(sizeof(generic_page) == generic_page_header::page_sz);
    }
    virtual ~generic_page_h() {}


    /// return pointer to underlying page
    generic_page* get_generic_page() const { return _pp; }


    const lpid_t& pid()   const { return _pp->pid; }
    vid_t         vid()   const { return _pp->pid.vol(); }
    volid_t       vol()   const { return _pp->pid.vol().vol; }
    snum_t        store() const { return _pp->pid.store(); }

    page_tag_t    tag()   const { return (page_tag_t) _pp->tag; }

    const lsn_t&  lsn()   const { return _pp->lsn; }
    void          set_lsns(const lsn_t& lsn) { _pp->lsn = lsn; }


    /// Returns the stored value of checksum of this page. 
    uint32_t      get_checksum()       const { return _pp->checksum; }
    /// Calculate the correct value of checksum of this page. 
    uint32_t      calculate_checksum() const { return _pp->calculate_checksum(); }
    /// Renew the stored value of checksum of this page.  Note that
    /// this is a const function because checksum is mutable.
    void          update_checksum()    const { _pp->checksum = calculate_checksum(); }

protected:
    generic_page_h(generic_page* s, const lpid_t& pid, page_tag_t tag) : _pp(s) {
        ::memset(_pp, 0, sizeof(*_pp));
        _pp->pid = pid;
        _pp->tag = tag;
    }    

    /// The actual page we are handling; may be NULL for fixable pages
    generic_page* _pp;
};

#endif
