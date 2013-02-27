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

#ifndef __QPIPE_HASH_AGGREGATE_H
#define __QPIPE_HASH_AGGREGATE_H

#include "qpipe/core.h"
#include "util/resource_declare.h"

using namespace qpipe;


struct hash_aggregate_packet_t : public packet_t {
    static const c_str PACKET_TYPE;
    guard<packet_t> _input;
    guard<tuple_fifo> _input_buffer;
    guard<tuple_aggregate_t> _aggregate;
    guard<key_extractor_t> _extractor;
    guard<key_compare_t> _compare;

    hash_aggregate_packet_t(const c_str    &packet_id,
                               tuple_fifo* out_buffer,
                               tuple_filter_t* out_filter,
                               packet_t* input,
                               tuple_aggregate_t *aggregate,
                               key_extractor_t* extractor,
                               key_compare_t* compare)
        : packet_t(packet_id, PACKET_TYPE, out_buffer, out_filter,
                   create_plan(out_filter, aggregate, extractor, input->plan()),
                   true, /* merging allowed */
                   true  /* unreserve worker on completion */
                   ),
          _input(input), _input_buffer(input->output_buffer()),
          _aggregate(aggregate), _extractor(extractor), _compare(compare)
    {
    }

    // TODO: consider the key comparator as well
    static query_plan* create_plan(tuple_filter_t* filter, tuple_aggregate_t* agg,
                                   key_extractor_t* key, query_plan const* child)
    {
        c_str action("%s:%s:%s", PACKET_TYPE.data(),
                     agg->to_string().data(), key->to_string().data());

        query_plan const** children = new query_plan const*[1];
        children[0] = child;
        return new query_plan(action, filter->to_string(), children, 1);
    }
    
    virtual void declare_worker_needs(resource_declare_t* declare) {
        declare->declare(_packet_type, 1);
        _input->declare_worker_needs(declare);
    }
};



class hash_aggregate_stage_t : public stage_t {

    page_trash_stack _page_list;
    size_t _page_count;
    tuple_aggregate_t* _aggregate;
    qpipe::page* _agg_page;
    size_t _tuple_align;
public:
    static const c_str DEFAULT_STAGE_NAME;
    typedef hash_aggregate_packet_t stage_packet_t;

protected:
    virtual void process_packet();
    int alloc_agg(tuple_t &agg, const char* key);
};





#endif
