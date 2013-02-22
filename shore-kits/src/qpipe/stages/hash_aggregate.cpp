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

#include "qpipe/stages/hash_aggregate.h"

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


const c_str hash_aggregate_packet_t::PACKET_TYPE = "HASH_AGGREGATE";



const c_str hash_aggregate_stage_t::DEFAULT_STAGE_NAME = "HASH_AGGREGATE_STAGE";



// the maximum number of pages allowed in a single run
static const size_t MAX_RUN_PAGES = 10000;



/* Helper classes used by the hashtable data structure. All of
   these are very short, so give the compiler the option of
   inlining. */
    
struct extractkey_t {
    key_extractor_t *_extract;

    extractkey_t(key_extractor_t* extract)
	: _extract(extract)
    {
    }
        
    const char* const operator()(const char* value) const {
	return _extract->extract_key(value);
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


int hash_aggregate_stage_t::alloc_agg(tuple_t &agg, const char* key) {
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

void hash_aggregate_stage_t::process_packet() {
    hash_aggregate_packet_t* packet;
    packet = (hash_aggregate_packet_t*) _adaptor->get_packet();
    tuple_fifo* input_buffer = packet->_input_buffer;
    dispatcher_t::dispatch_packet(packet->_input);
    _aggregate = packet->_aggregate;
    key_extractor_t* agg_key = _aggregate->key_extractor();
    key_extractor_t* tup_key = packet->_extractor;
    //    key_compare_t* compare = packet->_compare;
    
    
    // create a set to hold the sorted run
    //    tuple_less_t less(agg_key, compare);
    size_t key_size = agg_key->key_size();
    hashfcn_t hf(key_size);
    equalbytes_t eql(key_size);
    extractkey_t ext(agg_key);

    // start with 10001 buckets
    tuple_hash_t run(10001, hf, eql, ext);

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

                // search for the key in the hash table
		char const* key = tup_key->extract_key(in);
		tuple_hash_t::iterator candidate = run.find(key);
                if(candidate == run.end()) {
                    // initialize a blank aggregate tuple
		    tuple_t agg;
                    if(alloc_agg(agg, key))
                        break;
                    
                    // insert the new aggregate tuple
                    candidate = run.insert_unique(agg.data).first;
                }
                else {
                    TRACE(TRACE_DEBUG, "Merging a tuple\n");
                }

                // update an existing aggregate tuple (which may have
                // just barely been inserted)
                _aggregate->aggregate(*candidate, in);
            }
        }

        // TODO: handle cases where the run doesn't fit in memory
        assert(input_buffer->eof());

        // write out the result
        size_t out_size = packet->_output_filter->input_tuple_size();
        array_guard_t<char> out_data = new char[out_size];
        tuple_t out(out_data, out_size);
        for(tuple_hash_t::iterator it=run.begin(); it != run.end(); ++it) {
            // convert the aggregate tuple to an output tuple
            _aggregate->finish(out, *it);
            _adaptor->output(out);
        }
    }
}


