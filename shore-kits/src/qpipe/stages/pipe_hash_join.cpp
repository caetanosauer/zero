/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
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

/* hash_join_stage.cpp */
/* Implementation of the HASH_JOIN operator */
/* History: 
   3/6/2006: Uses the outtup variable of the output_tup_t, instead of the data.
*/


#include "qpipe/stages/pipe_hash_join.h"
#include <cstring>
#include <algorithm>

#if defined(linux) || defined(__linux)
/*GNU found*/
#include <ext/hash_set>
using __gnu_cxx::hashtable;
using __gnu_cxx::hash_set;
#else
/*Solaris*/
#include <hash_set>
using std::hash_set;
using std::hashtable;
#endif



ENTER_NAMESPACE(qpipe);


const c_str pipe_hash_join_packet_t::PACKET_TYPE = "PIPE_HASH_JOIN";

const c_str pipe_hash_join_stage_t::DEFAULT_STAGE_NAME = "PIPE_HASH_JOIN";


/* Helper classes used by the hashtable data structure. All of
   these are very short, so give the compiler the option of
   inlining. */

struct extractkey_t {
    size_t _offset;
   
    extractkey_t(tuple_join_t* join, bool left)
	: _offset(left? join->left_key_offset() : join->right_key_offset())
    {
    }
        
    const char* const operator()(const char* value) const {
	return value + _offset;
    }
};


/* Can be used for both EqualKey and EqualData template
   parameter. */
struct equalbytes_t {
        
    size_t _len;
    equalbytes_t(size_t len)
	: _len(len)
    {
    }

    bool operator()(const char *k1, const char *k2) const {
	return !memcmp(k1, k2, _len);
    }
};

    
struct hashfcn_t {
        
    size_t _len;
    hashfcn_t(size_t len)
	: _len(len)
    {
    }
        
    size_t operator()(const char *key) const {
	return fnv_hash(key, _len);
    }
};


typedef hash_set<char*, hashfcn_t, equalbytes_t>::allocator_type alloc_t;

typedef hashtable<char *, const char *,
                  hashfcn_t, extractkey_t,
                  equalbytes_t, alloc_t> tuple_hash_t;

// typedef hashtable<char *, const char *,
//                   extractkey_t,
//                   equalbytes_t, equalbytes_t, 
//                   hashfcn_t> tuple_hash_t;


void pipe_hash_join_stage_t::process_packet() {

    /*
     * A fully pipelined hash join operator. The distinction between
     * "left" and "right" relations is minor, since both are processed
     * at the same time.
     */
    pipe_hash_join_packet_t* packet = (pipe_hash_join_packet_t *)_adaptor->get_packet();

    // TODO: deal with relations that don't fit in memory


    /* Dispatch both producers */
    tuple_fifo *right_buffer = packet->_right_buffer;
    dispatcher_t::dispatch_packet(packet->_right);
    tuple_fifo *left_buffer = packet->_left_buffer;
    dispatcher_t::dispatch_packet(packet->_left);

    _join = packet->_join;
    tuple_t left(NULL, left_buffer->tuple_size());
    tuple_t right(NULL, right_buffer->tuple_size());
    extractkey_t left_ke(_join, true);
    extractkey_t right_ke(_join, false);
    equalbytes_t eq(_join->key_size());
    hashfcn_t hf(_join->key_size());

    // init the hash tables
    // tuple_hash_t left_hash(10001, left_ke, eq, eq, hf);
    // tuple_hash_t right_hash(10001, right_ke, eq, eq, hf);

    tuple_hash_t left_hash(10001, hf, eq, left_ke);
    tuple_hash_t right_hash(10001, hf, eq, right_ke);


    // and the page store
    page_trash_stack left_pages;
    page_trash_stack right_pages;

    int out_size = packet->_output_filter->input_tuple_size();
    array_guard_t<char> out_data = new char[out_size];
    tuple_t out(out_data, out_size);

    /*
     * Strategy: read from both input FIFOs in round-robin fashion; if
     * one isn't ready read from the other; if the other is not ready
     * either block on it with timeout.
     */
    int left_ready = left_buffer->check_read_ready();
    int right_ready = right_buffer->check_read_ready();

    typedef qpipe::page::iterator pit;
    typedef tuple_hash_t::iterator hit;
 do_left:
    if(left_ready == 1) {
	// process a left page
        qpipe::page* p = qpipe::page::alloc(left_buffer->tuple_size());
	left_buffer->copy_page(p);
	left_pages.add(p);
	pit it = p->begin();
	pit end = p->end();
	while(it != end) {
	    // this 'left' purposely masks the previously declared one
	    tuple_t left = it.advance();
	    left_hash.insert_equal(left.data);
	    hit probe = right_hash.find(left_ke(left.data));
	    for(; probe != right_hash.end(); ++probe) {
		right.data = *probe;
		_join->join(out, left, right);
		_adaptor->output(out);
	    }
	}
	
	left_ready = left_buffer->check_read_ready();
	right_ready = right_buffer->check_read_ready();
    }
    
 do_right:
    if(right_ready == 1) {
	// process a right page
        qpipe::page* p = qpipe::page::alloc(right_buffer->tuple_size());
        right_buffer->copy_page(p);
	right_pages.add(p);
	pit it = p->begin();
	pit end = p->end();
	while(it != end) {
	    // this 'right' purposely masks the previously declared one
	    tuple_t right = it.advance();
	    right_hash.insert_equal(right.data);
	    hit probe = left_hash.find(right_ke(right.data));
	    for(; probe != left_hash.end(); ++probe) {
		left.data = *probe;
		_join->join(out, left, right);
		_adaptor->output(out);
	    }
	}
	
	left_ready = left_buffer->check_read_ready();
	right_ready = right_buffer->check_read_ready();
    }
    
 do_decide:
    // now what?
    if(left_ready == 1)
	goto do_left;
    if(right_ready == 1)
	goto do_right;
    if(left_ready == -1) {
	if(right_ready == -1)
	    return; // both EOF
	    
	// left is EOF
	if(right_buffer->ensure_read_ready())
	    right_ready = 1;
	else
	    right_ready = -1;
    }
    else {
	// set a timeout of 1ms if right isn't EOF
	if(right_ready == 0) {
	    if(left_buffer->ensure_read_ready(1))
		left_ready = 1;
	    else
		left_ready = left_buffer->check_read_ready();
	    right_ready = right_buffer->check_read_ready();
	}
	else {
	    if(left_buffer->ensure_read_ready())
		left_ready = 1;
	    else
		left_ready = -1;
	}
    }
    goto do_decide;
}

EXIT_NAMESPACE(qpipe);
