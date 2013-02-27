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

/* sort_merge_join.cpp */
/* Implementation of the SORT_MERGE_JOIN operator */


#include "qpipe/stages/sort_merge_join.h"

#include <cstring>
#include <algorithm>


ENTER_NAMESPACE(qpipe);


const c_str sort_merge_join_packet_t::PACKET_TYPE = "SORT_MERGE_JOIN";

const c_str sort_merge_join_stage_t::DEFAULT_STAGE_NAME = "SORT_MERGE_JOIN";



void sort_merge_join_stage_t::process_packet() {

    sort_merge_join_packet_t* packet = (sort_merge_join_packet_t *)_adaptor->get_packet();

    _join = packet->_join;
    _compare = packet->_compare;

    tuple_fifo *right_buffer = packet->_right_buffer;
    dispatcher_t::dispatch_packet(packet->_right);
    tuple_fifo *left_buffer = packet->_left_buffer;
    dispatcher_t::dispatch_packet(packet->_left);


    if(!right_buffer->ensure_read_ready()) {
        // No right-side tuples.
        // TODO: Handle outer joins here. (herc)
        return;
    }

    if(!left_buffer->ensure_read_ready())
        // No left-side tuples... no join tuples.
        return;
    
    // Define current left tuple, current right tuple and current output tuple.
    tuple_t left(NULL, _join->left_tuple_size());
    tuple_t right(NULL, _join->right_tuple_size());
    array_guard_t<char> data = new char[_join->output_tuple_size()];
    
    // Get first instances of left and right tuples.
    if (!left_buffer->get_tuple(left))
        return;

    if (!right_buffer->get_tuple(right))
        return;
    
    // Define current left tuple's key and current right tuple's key.
    const char* left_key;
    const char* right_key;
    
    // Perform the Merge-Join of the two sorted inputs.
    while(1) {

        // Get tuple keys
        left_key = _join->left_key(left);
        right_key = _join->right_key(right);
        
        // While left tuples < right tuple,
        // continue scanning left tuples.
        
        while ((_compare->operator ()(left_key, right_key)) < 0) {
            if (!left_buffer->get_tuple(left))
                return;
            left_key = _join->left_key(left);
        }
        
        // Now, we have found a left tuple that is >= right tuple.
        // While left tuple > right tuples,
        // continue scanning right tuples.
        
        while ((_compare->operator ()(left_key, right_key)) > 0) {
            if (!right_buffer->get_tuple(right))
                return;
            right_key = _join->right_key(right);
        }
        
        // We found a left tuple = a right tuple.
        // Now, we must output the cartesian product of
        // left tuples and right tuples, for all these tuples that have
        // equal keys. We will do that by using two iterations.
        // The outer iteration will be over the left tuples.
        // The inner iteration will be over the right tuples.
        // These iterations will break when we find tuples with
        // different keys.
        
        // Because the input buffers are FIFO queues, we cannot iterate
        // over the right tuples more than once.
        // As such, we will copy these right tuples to new pages, so that in
        // the end we can iterate through them for every left tuple.
        
        page_pool* pool = malloc_page_pool::instance();
        qpipe::page* right_head_page = qpipe::page::alloc(_join->right_tuple_size(), pool);
        qpipe::page* right_page = right_head_page;
        
        // Start to find out all right tuples with the same key
        // and store them into the pages.
        const char* right_key_new = _join->right_key(right);
        while (memcmp(right_key_new, right_key, _join->key_size()) == 0) {
            // If page is full, create a new one
            if (right_page->full()) {
                qpipe::page* new_page = qpipe::page::alloc(_join->right_tuple_size(), pool);
                right_page->next = new_page;
                new_page->next = NULL;
                right_page = new_page;
            }
            
            // Add new tuple to page
            right_page->append_tuple(right);
            
            // Proceed to next right tuple
            if (!right_buffer->get_tuple(right))
                break;
            right_key_new = _join->right_key(right);
        }
        
        // Now, for every left tuple, iterate all over the right tuples
        // stored in the pages, to output the cartesian product.
        // The iteration stops when we find a left tuple with a different key.
        
        const char* left_key_new = _join->left_key(left);
        while (memcmp(left_key_new, left_key, _join->key_size()) == 0) {
            // Start reading the right tuples we stored and join them
            // with the left tuple.
            right_page = right_head_page;
            while (right_page != NULL) {
                qpipe::page::iterator it = right_page->begin();
                qpipe::page::iterator end = right_page->end();
                while (it != end) {
                    tuple_t right_tuple_to_add = it.advance();
                    tuple_t out(data, _join->output_tuple_size());
                    _join->join(out, left, right_tuple_to_add);
                    _adaptor->output(out);
                }

                right_page = right_page->next;
            }
            
            // Proceed to next left tuple
            if (!left_buffer->get_tuple(left))
                break;
            left_key_new = _join->left_key(left);
        }
        
        // Now destroy the pages we created
        right_page = right_head_page;
        while (right_page != NULL) {
            qpipe::page* next_right_page = right_page->next;
            right_page->free();
            right_page = next_right_page;
        }
    }

}

EXIT_NAMESPACE(qpipe);
