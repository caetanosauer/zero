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

/* sort_merge_join.h */
/* Declaration of the sort_merge_join packet and stage classes */


#ifndef __QPIPE_SORT_MERGE_JOIN_STAGE_H
#define __QPIPE_SORT_MERGE_JOIN_STAGE_H

#include "qpipe/core.h"

#include <string>

using std::string;
using std::vector;


ENTER_NAMESPACE(qpipe);


#define HASH_JOIN_STAGE_NAME  "SORT_MERGE_JOIN"
#define HASH_JOIN_PACKET_TYPE "SORT_MERGE_JOIN"



/**************************
 * sort_merge_join_packet *
 **************************/


class sort_merge_join_packet_t : public packet_t {

public:
    static const c_str PACKET_TYPE;

    guard<packet_t> _left;
    guard<packet_t> _right;
    guard<tuple_fifo> _left_buffer;
    guard<tuple_fifo> _right_buffer;
    guard<key_compare_t> _compare;
    guard<tuple_join_t> _join;
  
    /**
     *  @brief Constructor. Sort-Merge Join accepts two already sorted
     *  inputs (e.g. the outputs of two Sort packets) and outputs the inner join
     *  between the two. The join is based on an equality condition between
     *  keys of the left and right input. A comparer is used to define the
     *  relationship (less than, equal to, greater than) between the keys.
     *
     *  @param packet_id The ID of this packet. This should point to a
     *  block of bytes allocated with malloc(). This packet will take
     *  ownership of this block and invoke free() when it is
     *  destroyed.
     *
     *  @param output_buffer The buffer where this packet should send
     *  its data. A packet DOES NOT own its output buffer (we will not
     *  invoke delete or free() on this field in our packet
     *  destructor).
     *
     *  @param output_filter The filter that will be applied to any
     *  tuple sent to output_buffer. The packet OWNS this filter. It
     *  will be deleted in the packet destructor.
     *
     *  @param left Left side-input packet. This should
     *  be the larger input.
     *
     *  @param right Right-side packet. To produce the cartesian product between
     *  left and right tuples with equal keys, right-side tuples are temporarily
     *  copied, so that they can be iterated over and over for every
     *  left tuple. As such, this should be the smaller input.
     *
     *  @param join The joiner  we will be using for this
     *  packet. The packet OWNS this joiner. It will be deleted in
     *  the packet destructor.
     * 
     *  @param compare The comparer used to compare the keys of the left
     *  and right tuples. During the merge phase, the comparer is used to
     *  advance left and right tuples alternatively, until we find
     *  tuples with equal keys to output.
     *
     */
    sort_merge_join_packet_t(const c_str &packet_id,
                       tuple_fifo* out_buffer,
                       tuple_filter_t *output_filter,
                       packet_t* left,
                       packet_t* right,
                       tuple_join_t *join,
                       key_compare_t* compare)
        : packet_t(packet_id, PACKET_TYPE, out_buffer, output_filter,
                   create_plan(output_filter, join, false, false, left, right),
                   true, /* merging allowed */
                   true  /* unreserve worker on completion */
                   ),
          _left(left),
          _right(right),
          _left_buffer(left->output_buffer()),
          _right_buffer(right->output_buffer()),
          _join(join),
          _compare(compare)
    {
    }
  
    static query_plan* create_plan(tuple_filter_t* filter, tuple_join_t* join,
                                   bool outer, bool distinct,
                                   packet_t* left, packet_t* right)
    {
        c_str action("%s:%s:%d:%d", PACKET_TYPE.data(),
                     join->to_string().data(), outer, distinct);

        query_plan const** children = new query_plan const*[2];
        children[0] = left->plan();
        children[1] = right->plan();
        return new query_plan(action, filter->to_string(), children, 2);
    }

    virtual void declare_worker_needs(resource_declare_t* declare) {
        declare->declare(_packet_type, 1);
        _left->declare_worker_needs(declare);
        _right->declare_worker_needs(declare);
    }
};



/*************************
 * sort_merge_join_stage *
 *************************/

class sort_merge_join_stage_t : public stage_t {


private:

    /* fields */
    
    tuple_join_t* _join;
    key_compare_t* _compare;


public:

    typedef sort_merge_join_packet_t stage_packet_t;

    static const c_str DEFAULT_STAGE_NAME;


    virtual void process_packet();
    
    
    // Allocate in-memory partitions...
    sort_merge_join_stage_t()
    {
    }

    ~sort_merge_join_stage_t() {
    }

};


EXIT_NAMESPACE(qpipe);

#endif	// __SORT_MERGE_JOIN_H
