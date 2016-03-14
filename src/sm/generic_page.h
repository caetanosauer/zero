/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef GENERIC_PAGE_H
#define GENERIC_PAGE_H

#include <boost/static_assert.hpp>

#include "basics.h"
#include "lsn.h"
#include "w_defines.h"


/**
 * \brief Page headers shared by all Zero pages
 *
 * \details
 *     All page data types (e.g., generic_page, alloc_page,
 * btree_page, stnode_page) inherit (indirectly) from this class.
 * This is a POD.
 *
 *     The checksum field is placed first to make the region of data
 * it covers continuous.  The order of the other fields has been
 * arranged to maximize packing into the fewest number of words
 * possible.
 *
 *     Because this class contains an 8-byte aligned object (lsn_t),
 * it must be a multiple of eight bytes.  The extra space not needed
 * for all pages (field reserved) is reserved for subclass usage.
 *
 *     Some page types (alloc_page, stnode_page) do not currently
 * (9/2013) use the lsn, reserved, or page_flags fields.
 */
class generic_page_header {
public:
    /// Size of all Zero pages
    static const size_t page_sz = SM_PAGESIZE;


    /**
     * \brief Stored checksum of this page.
     *
     * \details
     * Checksum is calculated from various parts of this page and
     * updated just before this page is written out to permanent
     * storage.  It is checked each time this page is read in from the
     * permanent storage.
     */
    mutable uint32_t checksum;     // +4 -> 4

    /// ID of this page
    PageID           pid;          // +4 -> 8

    /// LSN (Log Sequence Number) of the last write to this page
    lsn_t            lsn;          // +8 -> 16

    /// ID of the store to which this page belongs (0 if none)
    StoreID           store;        // +4 -> 20

    /// Page type (a page_tag_t)
    uint16_t         tag;          // +2 -> 22

protected:
    friend class fixable_page_h;   // for access to page_flags&t_to_be_deleted

    /// Page flags (an OR of page_flag_t's)
    uint16_t         page_flags;   //  +2 -> 24

    /// Reserved for subclass usage
    uint64_t         reserved;     //  +8 -> 32

public:
    /// Calculate the correct value of checksum for this page.
    uint32_t    calculate_checksum () const;

public:
    friend std::ostream& operator<<(std::ostream&, generic_page_header&);
};
// verify compiler tightly packed all of generic_page_header's fields:
BOOST_STATIC_ASSERT(sizeof(generic_page_header) == 32);


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
    t_to_be_deleted  = 0x01,     ///< this page will be deleted as soon as the page is evicted from bufferpool
};




/**
 * \brief A generic page view: any Zero page can be viewed as being of
 * this type but it only exposes fields shared by all Zero pages.  To
 * "downcast" and access page-type--specific fields, pass a pointer to
 * one of these to one of the page handle classes' (e.g., btree_page_h)
 * constructors.
 *
 * \details
 * All zero pages have the same size and initial headers
 * (generic_page_header's).  Each specific page type has an associated
 * data type that is a (indirect) subclass of generic_page_header as
 * well as an associated handle class.  For example, B-tree pages have
 * associated page class btree_page and handle class btree_page_h.
 * Casting between page types is done by the handle classes after
 * verifying the cast is safe according to the page tag.  No other
 * code should perform such casts.
 *
 * The corresponding handle class for this page type is generic_page_h.
 */
class generic_page : public generic_page_header {
private:
    char subclass_specific[page_sz - sizeof(generic_page_header)];
};
BOOST_STATIC_ASSERT(sizeof(generic_page) == generic_page_header::page_sz);



/**
 * \brief Page handle class for any page type.
 *
 * \details
 * This is the root superclass of all the Zero page handle classes.
 * It provides operations on the fields common to all pages.
 */
class generic_page_h {
public:
    generic_page_h(generic_page* s) : _pp(s) {}
    virtual ~generic_page_h() {}


    /// return pointer to underlying page
    generic_page* get_generic_page() const { return _pp; }


    PageID pid() const { return _pp->pid; }
    StoreID      store() const { return _pp->store; }

    page_tag_t    tag()   const { return (page_tag_t) _pp->tag; }

    const lsn_t&  lsn()   const { return _pp->lsn; }

protected:
    generic_page_h(generic_page* s, const PageID& pid, page_tag_t tag,
            StoreID store)
        : _pp(s)
    {
        ::memset(_pp, 0, sizeof(*_pp));
        _pp->pid = pid;
        _pp->store = store;
        _pp->tag = tag;
    }

    /// The actual page we are handling; may be NULL for fixable pages
    generic_page* _pp;
};

#endif
