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

#include "qpipe/stages/sorted_in.h"



const c_str sorted_in_stage_t::DEFAULT_STAGE_NAME = "SORTED_IN_STAGE";



const c_str sorted_in_packet_t::PACKET_TYPE = "SORTED_IN";



/**
 * @brief implements "where left.key [not] in right". Assumes both
 * relations are sorted and that the right relation is distinct.
 *
 *  TODO we should be able to remove the "right relation is distinct"
 *  requirement by caching one right side tuple and supplying a full
 *  tuple comparator in the sorted_in_packet_t. If we are computing IN
 *  and we have the same right-side tuple as last time, we advance the
 *  right side without an output().
 *
 *  If we are computing NOT IN, we may not need to change the code
 *  below...
 */
void sorted_in_stage_t::process_packet() {

    // setup
    sorted_in_packet_t* packet;
    packet = (sorted_in_packet_t*) _adaptor->get_packet();
    tuple_fifo* left_input = packet->_left_input;
    tuple_fifo* right_input = packet->_right_input;
    key_extractor_t* left_extractor = packet->_left_extractor;
    key_extractor_t* right_extractor = packet->_right_extractor;
    key_compare_t* compare = packet->_compare;
    bool reject_matches = packet->_reject_matches;

    // fire up inputs
    dispatcher_t::dispatch_packet(packet->_left);

    tuple_t left;
    const char* left_key;
    int left_hint;

    tuple_t right;
    const char* right_key;
    int right_hint;
    
    // seed the process with the first left tuple
    if(!left_input->get_tuple(left))
        return;
    dispatcher_t::dispatch_packet(packet->_right);
    left_key = left_extractor->extract_key(left);
    left_hint = left_extractor->extract_hint(left_key);


    // We want to output the tuples from the left side that are either
    // IN or NOT IN the set of tuples on the right side. We can break
    // out of this while(1) loop when there are no more tuples left to
    // be read.
    while(1) {

        // "Process" (output or discard, depending on reject_matches
        // flag) right-side tuples that come BEFORE the current left
        // tuple. Once we get right tuple > left tuple, we must start
        // fetching more left tuples. For a join, we must loop when
        // right tuple == left tuple, but since we are computing
        // IN/NOT IN, there is no point in looping.
        int diff;
        do {

            // advance right side
            if(!right_input->get_tuple(right)) {

                // No more right-side tuples! Simply deal with
                // remaining left-side tuples. If we are computing IN,
                // we don't output them. If we are computing NOT IN,
                // we output them all.
                if(reject_matches) {
                    while(left_input->get_tuple(left)) 
                        _adaptor->output(left);
                }
                
                // done
                return;
            }

            // if we are here, we got another right-side tuple
            right_key = right_extractor->extract_key(right);
            right_hint = right_extractor->extract_hint(right_key);

            // Compare left tuple to right tuple. Compute hint
            // difference. Fall back to full key compare if necessary.
            diff = left_hint - right_hint;
            if(!diff && left_extractor->key_size() > sizeof(int))
                diff = (*compare)(left_key, right_key);

        } while(diff > 0);

        
        // Detected right tuple > left tuple. Process left side tuples
        // while this condition remains true.
        while(diff <= 0) {

            // output? The logic is strange, but it works.
            if((diff == 0) != reject_matches) 
                _adaptor->output(left);
            
            // if we are here, it is because the right side tuple is
            // at least as large as the left side tuple. Advance the
            // left side until it becomes larger than the right side.
            if(!left_input->get_tuple(left))
                // No more left tuples! See reasoning above to see why
                // we can stop.
                return;

            // if we are here, we got another left-side tuple
            left_key = left_extractor->extract_key(left);
            left_hint = left_extractor->extract_hint(left_key);

            // Compare left tuple to right tuple. Compute hint
            // difference. Fall back to full key compare if necessary.
            diff = left_hint - right_hint;
            if(!diff && left_extractor->key_size() > sizeof(int))
                diff = (*compare)(left_key, right_key);
        } 
    }

    unreachable();
}
