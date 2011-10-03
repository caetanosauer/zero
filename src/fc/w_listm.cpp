/*<std-header orig-src='shore'>

 $Id: w_listm.cpp,v 1.13 2010/12/08 17:37:37 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <w_base.h>
#include <w_list.h>


void
w_link_t::attach(w_link_t* prev_link)
{
    w_assert2(_prev == this && _next == this); // not in any list
    _list = prev_link->_list;
    _next = prev_link->_next; 
    _prev = prev_link;
    prev_link->_next = this;
    _next->_prev = this;
    ++(_list->_cnt);
}

w_link_t*
w_link_t::detach()
{
    if (_next != this)  {
        w_assert2(_prev != this);
        _prev->_next = _next, _next->_prev = _prev;
        _list->_cnt--;
        w_assert2(_list->_cnt ||
               (_list->_tail._prev == & _list->_tail &&
                _list->_tail._next == & _list->_tail));
            _next = _prev = this, _list = 0;
    }
    return this;
}

ostream&
operator<<(ostream& o, const w_link_t& n)  
{
    o << "_list = " << n.member_of() << endl;
    o << "_next = " << n.next() << endl;
    o << "_prev = " << n.prev();
    return o;
}

void
w_list_base_t::dump()
{
    cout << "_tail = " << _tail << endl;
    cout << "_cnt = " << _cnt << endl;
    cout << "_adj = " << _adj << endl;
}

