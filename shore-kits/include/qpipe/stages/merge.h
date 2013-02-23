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

#ifndef __QPIPE_MERGE_H
#define __QPIPE_MERGE_H

#include "qpipe/core.h"

#include <vector>

using std::vector;



using namespace qpipe;



/**
 *  @brief Packet definition for an N-way merge stage
 */
struct merge_packet_t : public packet_t {

    static const c_str PACKET_TYPE;
    typedef std::vector<tuple_fifo*> buffer_list_t;
    
    buffer_list_t _input_buffers;
    guard<key_extractor_t> _extract;
    guard<key_compare_t>   _compare;


    /**
     *  @brief aggregate_packet_t constructor.
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
     *  @param output_filter The filter that will be applied to any
     *  tuple sent to output_buffer. The packet OWNS this filter. It
     *  will be deleted in the packet destructor.
     *
     *  @param input_buffers A list of tuple_fifo pointers. This
     *  is the set of inputs that we are merging. This list should be
     *  set up by the meta-stage that creates this merge_packet_t. We
     *  will take ownership of the tuple_fifo's, but not the list
     *  itself. We will copy the list.
     *
     *  @param comparator A comparator for the tuples in our input
     *  buffers. This packet owns this comparator.
     */
    merge_packet_t(const c_str         &packet_id,
                   tuple_fifo*      output_buffer,
                   tuple_filter_t*      output_filter,
                   const buffer_list_t& input_buffers,
                   key_extractor_t* extract,
                   key_compare_t*  compare)
	: packet_t(packet_id, PACKET_TYPE, output_buffer, output_filter, NULL,
                   false, /* merging not allowed */
                   false  /* keep worker on completion */
                   ),
          _input_buffers(input_buffers),
          _extract(extract),
          _compare(compare)
    {
    }
    
    ~merge_packet_t() {
        buffer_list_t::iterator it=_input_buffers.begin();
        while(it != _input_buffers.end())
            delete *it++;
    }

    virtual void declare_worker_needs(resource_declare_t*) {
        /* Do nothing. The stage the that creates us is responsible
           for deciding how many MERGE workers it needs. */
    }
};



/**
 *  @brief Merge stage that merges N sorted inputs into one sorted
 *  output run.
 */
class merge_stage_t : public stage_t {

private:

    struct buffer_head_t {
        // for the linked list
        buffer_head_t*   next;
        tuple_fifo*  buffer;
        key_extractor_t* _extract;
        array_guard_t<char> data;
        tuple_t tuple;
        hint_tuple_pair_t item;
        buffer_head_t() { }
        bool init(tuple_fifo* buf, key_extractor_t* c);
        bool has_tuple();
    };
    
    buffer_head_t*   _head_list;
    key_compare_t*   _compare;
    key_extractor_t* _extract;
    
public:

    static const c_str DEFAULT_STAGE_NAME;
    typedef merge_packet_t stage_packet_t;

    merge_stage_t()
        : _head_list(NULL)
    {
    }
    
protected:

    virtual void process_packet();
    
private:

    void insert_sorted(buffer_head_t* head);
    int  compare(const hint_tuple_pair_t &a, const hint_tuple_pair_t &b);
};



#endif
