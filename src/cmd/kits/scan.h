#ifndef KITS_SCAN_H
#define KITS_SCAN_H

#include "table_man.h"

class base_scan_t
{
protected:
    index_desc_t* _pindex;
    bt_cursor_t* btcursor;
public:
    base_scan_t(index_desc_t* pindex)
        : _pindex(pindex), btcursor(NULL)
    {
        w_assert1(_pindex);
    }

    virtual ~base_scan_t() {
        if (btcursor) delete btcursor;
    };

    w_rc_t open_scan(bool forward = true) {
        if (!btcursor) {
            btcursor = new bt_cursor_t(_pindex->stid(), forward);
        }
        return (RCOK);
    }

    w_rc_t open_scan(char* bound, int bsz, bool incl, bool forward = true)
    {
        if (!btcursor) {
            w_keystr_t kstr;
            kstr.construct_regularkey(bound, bsz);
            btcursor = new bt_cursor_t(_pindex->stid(), kstr, incl, forward);
        }

        return (RCOK);
    }

    w_rc_t open_scan(char* lower, int lowsz, bool lower_incl,
                     char* upper, int upsz, bool upper_incl,
                     bool forward = true)
    {
        if (!btcursor) {
            w_keystr_t kup, klow;
            kup.construct_regularkey(upper, upsz);
            klow.construct_regularkey(lower, lowsz);
            btcursor = new bt_cursor_t(
                    _pindex->stid(),
                    klow, lower_incl, kup, upper_incl, forward);
        }

        return (RCOK);
    }

    virtual w_rc_t next(bool& eof, table_row_t& tuple) = 0;

};

template <class T>
class table_scan_iter_impl : public base_scan_t
{
public:

    table_scan_iter_impl(table_man_t<T>* pmanager)
        : base_scan_t(pmanager->table()->primary_idx())
    {}

    virtual ~table_scan_iter_impl() {}

    virtual w_rc_t next(bool& eof, table_row_t& tuple)
    {
        if (!btcursor) open_scan();

        W_DO(btcursor->next());

        eof = btcursor->eof();
        if (eof) { return RCOK; }

        // Load key
        btcursor->key().serialize_as_nonkeystr(tuple._rep_key->_dest);
        tuple.load_key(tuple._rep_key->_dest, _pindex);

        // Load element
        char* elem = btcursor->elem();
        tuple.load_value(elem, _pindex);

        return (RCOK);
    }

};

template <class T>
class index_scan_iter_impl : public base_scan_t
{
private:
    index_desc_t* _primary_idx;
    bool          _need_tuple;

public:
    index_scan_iter_impl(index_desc_t* pindex,
                         table_man_t<T>* pmanager,
                         bool need_tuple = false)
          : base_scan_t(pindex), _need_tuple(need_tuple)
    {
        assert (_pindex);
        assert (pmanager);
        _primary_idx = pmanager->table()->primary_idx();
    }

    virtual ~index_scan_iter_impl() { };

    virtual w_rc_t next(bool& eof, table_row_t& tuple)
    {
        assert (btcursor);

        W_DO(btcursor->next());

        eof = btcursor->eof();
        if (eof) { return RCOK; }

        bool loaded = false;

        if (!_need_tuple) {
            // Load only fields of secondary key (index key)
            btcursor->key().serialize_as_nonkeystr(tuple._rep_key->_dest);
            tuple.load_key(tuple._rep_key->_dest, _pindex);
        }
        else {
            // Fetch complete tuple from primary index
            index_desc_t* prim_idx = _primary_idx;
            char* pkey = btcursor->elem();
            smsize_t elen = btcursor->elen();

            // load primary key fields
            tuple.load_key(pkey, prim_idx);

            // fetch and load other fields
            w_keystr_t pkeystr;
            pkeystr.construct_regularkey(pkey, elen);
            ss_m::find_assoc(prim_idx->stid(), pkeystr, tuple._rep->_dest,
                    elen, loaded);
            w_assert0(loaded);

            tuple.load_value(tuple._rep->_dest, prim_idx);
        }
        return (RCOK);
    }
};

#endif
