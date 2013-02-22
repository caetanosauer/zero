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

#include "qpipe/stages/partial_aggregate.h"



const c_str partial_aggregate_packet_t::PACKET_TYPE = "PARTIAL_AGGREGATE";



const c_str partial_aggregate_stage_t::DEFAULT_STAGE_NAME = "PARTIAL_AGGREGATE_STAGE";



// the maximum number of pages allowed in a single run
static const size_t MAX_RUN_PAGES = 10000;



int partial_aggregate_stage_t::alloc_agg(hint_tuple_pair_t &agg, const char* key) {
    // out of space?
    if(!_agg_page || _agg_page->full()) {
        if(_page_count >= MAX_RUN_PAGES)
            return 1;

        _agg_page = qpipe::page::alloc(_aggregate->tuple_size());
        _page_list.add(_agg_page);
        _page_count++;
    }

    // allocate the tuple
    agg.data = _agg_page->allocate();

    // initialize the data
    _aggregate->init(agg.data);
    
    // copy over the key
    key_extractor_t* extract = _aggregate->key_extractor();
    char* agg_key = extract->extract_key(agg.data);
    memcpy(agg_key, key, extract->key_size());
    return 0;
}

void partial_aggregate_stage_t::process_packet() {
    partial_aggregate_packet_t* packet;
    packet = (partial_aggregate_packet_t*) _adaptor->get_packet();
    tuple_fifo* input_buffer = packet->_input_buffer;
    dispatcher_t::dispatch_packet(packet->_input);
    _aggregate = packet->_aggregate;
    key_extractor_t* agg_key = _aggregate->key_extractor();
    key_extractor_t* tup_key = packet->_extractor;
    key_compare_t* compare = packet->_compare;
    
    
    // create a set to hold the sorted run
    tuple_less_t less(agg_key, compare);
    tuple_set_t run(less);

    // read in the tuples and aggregate them in the set
    while(!input_buffer->eof()) {
        _page_list.clear();
        _page_count = 0;
        _agg_page = NULL;
        while(_page_count < MAX_RUN_PAGES && !input_buffer->eof()) {
            guard<qpipe::page> page = NULL;
            tuple_t in;
            while(1) {
                // out of pages?
                if(!input_buffer->get_tuple(in))
                   break;

                int hint = tup_key->extract_hint(in);

                // fool the aggregate's key extractor into thinking
                // the tuple is an aggregate. Use pointer math to put
                // the tuple's key bits where the aggregate's key bits
                // are supposed to go. (the search only touches the
                // key bits anyway, which are guaranteed to be the
                // same for both...)
                size_t offset = agg_key->key_offset();
                char* key_data = tup_key->extract_key(in);
                hint_tuple_pair_t key(hint, key_data - offset);

		// Supposedly, insertion with a proper hint is
		// amortized O(1) time. However, the definition of
		// "proper" is ambiguous:
		// - SGI STL reference: insertion point *before* hint
		// - Solaris STL source: insert point *after* hint
		// - Dinkum STL reference: insertion point *adjacent* to hint
		// - GNU STL: no documentation (as usual); adjacent?
		// - Solaris profiler: same performance as vanilla insert either way
		
                // find the lowest aggregate such that candidate >=
                // key. This is either the aggregate we want or a good
                // hint for the insertion that otherwise follows
                tuple_set_t::iterator candidate = run.find(key);
                if(candidate == run.end()) {
                    // initialize a blank aggregate tuple
                    hint_tuple_pair_t agg(hint, NULL);
                    if(alloc_agg(agg, key_data))
                        break;
                    
                    // insert the new aggregate tuple
                    candidate = run.insert(agg).first;
                }
                else {
                    TRACE(TRACE_DEBUG, "Merging a tuple\n");
                }

                // update an existing aggregate tuple (which may have
                // just barely been inserted)
                _aggregate->aggregate(candidate->data, in);
            }
        }

        // TODO: handle cases where the run doesn't fit in memory
        assert(input_buffer->eof());

        // write out the result
        size_t out_size = packet->_output_filter->input_tuple_size();
        array_guard_t<char> out_data = new char[out_size];
        tuple_t out(out_data, out_size);
        for(tuple_set_t::iterator it=run.begin(); it != run.end(); ++it) {
            // convert the aggregate tuple to an output tuple
            _aggregate->finish(out, it->data);
            _adaptor->output(out);
        }
    }
}
