/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef BTCURSOR_H
#define BTCURSOR_H

#include "w_defines.h"
#include "w_key.h"
#include "bf_tree.h"

class btree_page_h;


/**
 * \brief A cursor object to sequentially read BTree.
 * \details
 * This class is used as follows.
 * \verbatim
    // constructs a cursor with search conditions.
    bt_cursor_t cursor (volid, storeid,
        lower_bound, lower_inclusive,
        upper_bound, upper_inclusive,
        is_forward_direction);
    do {
        W_DO(cursor.next());
        if (cursor.eof()) {
            break;
        }
        w_keystr_t key = cursor.key();
        cvec_t el (cursor.elem(), cursor.elen());
        ....
    } while (true);\endverbatim
 *
 * \section LSN-and-Concurrency
 * A cursor object releases latches on the page
 * after each next() call to allow concurrent accesses.
 * Thus, it also checks if the current page has been
 * changed after its last access using \e _lsn variable.
 *
 * If it has been changed, and if the page still contains
 * the key we previously read, we just re-locate \e _slot
 * in the page and goes on.
 *
 * If it has been changed and also the page doesn't contain
 * the key we previously read any more, we re-locate \e _pid
 * by re-traversing the tree.
 *
 * These two events are expensive,
 * but happen only after the LSN check, so they are rare too.
 *
 * \section Locking-and-Concurrency
 * A cursor object also takes locks on the keys and their
 * intervals it read. Here, the complication is that
 * the cursor object has two modes; \e forward and \e backward.
 * Unfortunately, these two are not quite symmetric because
 * the key range locks are on half-open interval (a key
 * and open interval on its _right_).
 *
 * Also, there's a trade-off between concurrency and overhead.
 * In this class, we try to minimize overhead rather than
 * concurrency. See jira ticket:89 "Cursor case: overhead-concurrency trade-off" (originally trac ticket:91) for more details.
 * \ingroup SSMBTREE
 */
class bt_cursor_t : private smlevel_0 {
public:
    /**
     * Constructs a full scan cursor.
     * @param[in] vol Volume ID
     * @param[in] store Store ID
     * @param[in] forward true if this cursor goes forward from lower bound, false if
     * this cursor goes backwards from upper bound.
     */
    bt_cursor_t(StoreID store, bool forward);

    /**
     * Creates a BTree cursor object for the given search conditions.
     * @param[in] vol Volume ID
     * @param[in] store Store ID
     * @param[in] lower lower bound of the search range
     * @param[in] lower_inclusive true if returning a tuple exactly matching the lower bound
     * @param[in] upper upper bound of the search range
     * @param[in] upper_inclusive true if returning a tuple exactly matching the upper bound
     * @param[in] forward true if this cursor goes forward from lower bound, false if
     * this cursor goes backwards from upper bound.
     */
    bt_cursor_t(
        StoreID store,
        const w_keystr_t& lower, bool lower_inclusive,
        const w_keystr_t& upper, bool upper_inclusive,
        bool              forward);

    /**
     * Constructs an open-end scan, with a start condition only.
     * @param[in] vol Volume ID
     * @param[in] store Store ID
     * @param[in] bound start condition for the scan (lower if forward, upper otherwise)
     * @param[in] inclusive true if returning a tuple exactly matching the bound
     * @param[in] forward true if this cursor goes forward from lower bound, false if
     * this cursor goes backwards from upper bound.
     */
    bt_cursor_t(
        StoreID store,
        const w_keystr_t& bound, bool inclusive,
        bool              forward);

    ~bt_cursor_t() {close();}

    /**
     * Moves the BTree cursor to next slot.
     */
    rc_t next();

    bool          is_valid() const { return _first_time || !_eof; }
    bool          is_forward() const { return _forward; }
    void          close();

    const w_keystr_t& key()     { return _key; }
    /**
     * Admittedly bad naming, but this means if the cursor still has record to return.
     * So, even if it's not quite the end of file or index, it returns true
     * when it exceeds the upper-condition.
     */
    bool              eof()     { return _eof;  }
    int               elen() const     { return _elen; }
    char*             elem()     { return _eof ? 0 :  _elbuf; }

private:
    void        _init(
        StoreID store,
        const w_keystr_t& lower,  bool lower_inclusive,
        const w_keystr_t& upper,  bool upper_inclusive,
        bool              forward);
    rc_t        _locate_first();
    rc_t        _check_page_update(btree_page_h &p);
    rc_t        _find_next(btree_page_h &p, bool &eof);
    void        _release_current_page();
    void        _set_current_page(btree_page_h &page);

    /**
     * \brief Re-fix the page at which the cursor was on with SH mode.
     * \details
     * In most cases, this method just re-fixes the page ID we observed before.
     * However, fix_direct might fail if the disk page is corrupted. In that case, we need
     * the parent page to apply Single-Page-Recovery on the corrupted page (eBF_DIRECTFIX_SWIZZLED_PTR).
     * Thus, we re-locate the key by traversing from the root.
     * This is expensive, but does not happen often.
    * @param[out] p page handle that will hold the re-fixed page
     */
    w_rc_t      _refix_current_key(btree_page_h &p);

    /**
     * \brief Chooses next slot and potentially next page for cursor access.
     *  \details
    *  Computes the next slot, which might be on a successor to p,
    *  or even a successor to the successor,
    *  in which case, p will be re-fixed to the page, and "slot" indicate
    *  where on that page it is.
    *  Leave exactly one page fixed.
    * @param[in] p fixed current page
    * @param[out] eof whether this cursor reached the end
    */
    rc_t        _advance_one_slot(btree_page_h &p, bool &eof);

    /**
    *  Make the cursor point to record at "slot" on "page".
    */
    rc_t         _make_rec(const btree_page_h& page);

    StoreID      _store;
    w_keystr_t  _lower;
    w_keystr_t  _upper;
    bool        _lower_inclusive;
    bool        _upper_inclusive;
    bool        _forward;

    /** these are retrieved from xct() when the cursur object is made. */
    bool        _needs_lock;
    bool        _ex_lock;

    /**
     * Whether we should move on to next key on the subsequent next() call.
     */
    bool        _dont_move_next;

    /** true if the first next() has not been called. */
    bool        _first_time;

    /** true if no element left. */
    bool        _eof;

    /** id of current page. current page has additional pin_count for refix(). */
    PageID     _pid;
    /** current page's corresponding slot index in the bufferpool. */
    pin_for_refix_holder _pid_bfidx;
    /** current slot in the current page. */
    slotid_t    _slot;
    /** lsn of the current page AS OF last access. */
    lsn_t       _lsn;

    /** current key. */
    w_keystr_t  _key;
    /** only internally used as temporary variable. */
    w_keystr_t  _tmp_next_key_buf;
    /** length of current record(el). */
    smsize_t    _elen;
    /** buffer to store the current record (el). */
    char        _elbuf [SM_PAGESIZE];
};

#endif//BTCURSOR_H
