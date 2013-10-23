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

/*<std-header orig-src='shore'>

 $Id: w_rc.cpp,v 1.30 2010/12/09 15:20:12 nhall Exp $

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
#include <vector>
#include <sstream>

bool w_rc_t::do_check = true; // default is on but it does nothing
// if W_DEBUG_RC is not defined.
bool w_rc_t::unchecked_is_fatal = true; 

const w_rc_t w_rc_t::rc_ok(w_error_t::no_error);

void
w_rc_t::set_return_check(bool on_off, bool is_fatal)
{
    do_check = on_off;
    unchecked_is_fatal = is_fatal;
}

NORET
w_rc_t::w_rc_t(
    const char* const    filename,
    uint32_t    line_num,
    w_rc_t::errcode_t    err_num)
    : _err( w_error_t::make(filename, line_num, err_num) )
{
  set_unchecked();
}

NORET
w_rc_t::w_rc_t(
    const char* const    filename,
    uint32_t    line_num,
    w_rc_t::errcode_t    err_num,
    int32_t     sys_err)
: _err( w_error_t::make(filename, line_num, err_num, sys_err) )
{
  set_unchecked();
}

w_rc_t&
w_rc_t::push(
    const char* const    filename,
    uint32_t    line_num,
    w_rc_t::errcode_t    err_num)
{
    _err = w_error_t::make(filename, line_num,
                   err_num, ptr());
    set_unchecked();
    return *this;
}

void
w_rc_t::fatal()
{
    stringstream s;
    s << *this << endl;
    fprintf(stderr, "FATAL ERROR: %s\n", s.str().c_str());
    w_base_t::abort();
}

w_rc_t&
w_rc_t::add_trace_info(
    const char* const   filename,
    uint32_t   line_num)
{
    ptr()->add_trace_info(filename, line_num);
    set_unchecked();
    return *this;
}

void 
w_rc_t::error_not_checked()
{
    cerr << "Error not checked: rc=" << (*this) << endl;
    if(unchecked_is_fatal)
        W_FATAL(fcINTERNAL);
}

ostream&
operator<<(
    ostream&            o,
    const w_rc_t&       obj)
{
    return o << *obj;
}

// The result of a clone will be used to initialize
// w_rc_t in a copy operator.
w_error_t *w_rc_t::_clone() const 
{
    // w_rc_t::clone() should enforce this
    w_assert2( ptr() != w_error_t::no_error );

    // need a deep copy

    std::vector<w_error_t const*> trace;
    w_rc_i it(*this);
    while(w_error_t const* e = it.next()) {
#if W_DEBUG_LEVEL > 2
        (void) e->get_more_info_msg(); // Just for assertion checking
#endif
        trace.push_back(e);
    }

    w_error_t* head = 0;
    while(!trace.empty()) {
        w_error_t const* e = trace.back();
        trace.pop_back();
        // creates a new w_error_t that points to head, returns the new one
        head = w_error_t::make(e->file, e->line, e->err_num, e->sys_err_num, head);

        for(unsigned int t=0; t<e->_trace_cnt; t++) {
            head->_trace_file[t] = e->_trace_file[t];
            head->_trace_line[t] = e->_trace_line[t];
        }
        head->_trace_cnt = e->_trace_cnt;

        head->clear_more_info_msg();
        const char *c=e->get_more_info_msg();
        if(c) head->append_more_info_msg(c);
    }

    return head;
}
