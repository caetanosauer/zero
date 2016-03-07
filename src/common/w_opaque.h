/*<std-header orig-src='shore' incl-file-exclusion='W_OPAQUE_H'>

 $Id: w_opaque.h,v 1.8 2010/12/08 17:37:34 nhall Exp $

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

#ifndef W_OPAQUE_H
#define W_OPAQUE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <cctype>
#include <cstring>

#ifndef W_BASE_H
#include <w_base.h>
#endif

template <int LEN> class opaque_quantity;
template <int LEN> ostream &operator<<(ostream &o,
                       const opaque_quantity<LEN> &r);
template <int LEN> bool operator==(const opaque_quantity<LEN> &l,
                   const opaque_quantity<LEN> &r);

/**\brief A set of untyped bytes.
 *
 * \details
 *
 * This is just a blob.  Not necessarily large object,
 * but it is an untyped group of bytes. Used for
 * global transaction IDs and server IDs for two-phase
 * commit.  The storage manager has to log this information
 * for preparing a 2PC transaction, so it has to flow
 * through the API.
 */
template <int LEN>
class opaque_quantity
{

private:

    uint32_t         _length;
    unsigned char _opaque[LEN];

    public:
    opaque_quantity() {
        (void) set_length(0);
#ifdef ZERO_INIT
        memset(_opaque, '\0', LEN);
#endif
    }
    opaque_quantity(const char* s)
    {
#ifdef ZERO_INIT
        memset(_opaque, '\0', LEN);
#endif
        *this = s;
    }

    friend bool
    operator== <LEN> (
        const opaque_quantity<LEN>    &l,
        const opaque_quantity<LEN>    &r);

    friend ostream &
    operator<< <LEN> (
        ostream &o,
        const opaque_quantity<LEN>    &b);

    opaque_quantity<LEN>    &
    operator=(const opaque_quantity<LEN>    &r)
    {
        (void) set_length(r.length());
        memcpy(_opaque,r._opaque,length());
        return *this;
    }
    opaque_quantity<LEN>    &
    operator=(const char* s)
    {
        w_assert9(strlen(s) <= LEN);
        (void) set_length(0);
        while ((_opaque[length()] = *s++))
            (void) set_length(length() + 1);
        return *this;
    }
    opaque_quantity<LEN>    &
    operator+=(const char* s)
    {
        w_assert9(strlen(s) + length() <= LEN);
        while ((_opaque[set_length(length() + 1)] = *s++))
            ;
        return *this;
    }
    opaque_quantity<LEN>    &
    operator-=(uint32_t len)
    {
        w_assert9(len <= length());
        (void) set_length(length() - len);
        return *this;
    }
    opaque_quantity<LEN>    &
    append(const void* data, uint32_t len)
    {
        w_assert9(len + length() <= LEN);
        memcpy((void*)&_opaque[length()], data, len);
        (void) set_length(length() + len);
        return *this;
    }
    opaque_quantity<LEN>    &
    zero()
    {
        (void) set_length(0);
        memset(_opaque, 0, LEN);
        return *this;
    }
    opaque_quantity<LEN>    &
    clear()
    {
        (void) set_length(0);
        return *this;
    }
    void *
    data_at_offset(unsigned i)  const
    {
        w_assert9(i < length());
        return (void*)&_opaque[i];
    }
    uint32_t          wholelength() const {
        return (sizeof(_length) + length());
    }
    uint32_t          set_length(uint32_t l) {
        if(is_aligned()) {
            _length = l;
        } else {
            char *m = (char *)&_length;
            memcpy(m, &l, sizeof(_length));
        }
        return l;
    }
    uint32_t          length() const {
        if(is_aligned()) return _length;
        else {
            uint32_t l;
            char *m = (char *)&_length;
            memcpy(&l, m, sizeof(_length));
            return l;
        }
    }

    void          ntoh()  {
        if(is_aligned()) {
            _length = w_base_t::w_ntohl(_length);
        } else {
            uint32_t         l = w_base_t::w_ntohl(length());
            char *m = (char *)&l;
            memcpy(&_length, m, sizeof(_length));
        }
    }
    void          hton()  {
        if(is_aligned()) {
            _length = w_base_t::w_htonl(_length);
        } else {
            uint32_t         l = w_base_t::w_htonl(length());
            char *m = (char *)&l;
            memcpy(&_length, m, sizeof(_length));
        }
    }

    /* XXX why doesn't this use the aligned macros? */
    bool          is_aligned() const  {
        return (((ptrdiff_t)(&_length) & (sizeof(_length) - 1)) == 0);
    }

    ostream        &print(ostream & o) const {
        o << "opaque[" << length() << "]" ;

        uint32_t print_length = length();
        if (print_length > LEN) {
            o << "[TRUNC TO LEN=" << LEN << "!!]";
            print_length = LEN;
        }
        o << '"';
        const unsigned char *cp = &_opaque[0];
        for (uint32_t i = 0; i < print_length; i++, cp++) {
            o << *cp;
        }

        return o << '"';
    }
};


template <int LEN>
bool operator==(const opaque_quantity<LEN> &a,
    const opaque_quantity<LEN>    &b)
{
    return ((a.length()==b.length()) &&
        (memcmp(a._opaque,b._opaque,a.length())==0));
}

template <int LEN>
ostream &
operator<<(ostream &o, const opaque_quantity<LEN>    &b)
{
    return b.print(o);
}

/*<std-footer incl-file-exclusion='W_OPAQUE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
