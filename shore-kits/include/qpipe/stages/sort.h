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

#ifndef __QPIPE_SORT_H
#define __QPIPE_SORT_H

#include "qpipe/stages/merge.h"
#include "qpipe/core.h"

#include <list>
#include <map>
#include <deque>



using namespace qpipe;
using namespace std;



/* exported functions */



/**
 *@brief Packet definition for the sort stage
 */
struct sort_packet_t : public packet_t {

public:

    static const c_str PACKET_TYPE;


    guard<key_extractor_t> _extract;
    guard<key_compare_t>   _compare;
    guard<packet_t>        _input;
    guard<tuple_fifo>      _input_buffer;


    /**
     *  @brief sort_packet_t constructor.
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
     *  @param aggregator The aggregator we will be using for this
     *  packet. The packet OWNS this aggregator. It will be deleted in
     *  the packet destructor.
     *
     *  @param input The input packet for this aggregator. The packet
     *  takes ownership of this packet, but it will hand off ownership
     *  to a container as soon as this packet is dispatched.
     */
    sort_packet_t(const c_str        &packet_id,
                  tuple_fifo*     output_buffer,
                  tuple_filter_t*     output_filter,
                  key_extractor_t* extract,
                  key_compare_t* compare,
                  packet_t*           input)
	: packet_t(packet_id, PACKET_TYPE, output_buffer, output_filter,
                   create_plan(output_filter, extract, input),
                   true, /* merging allowed */
                   true  /* unreserve worker on completion */
                   ),
          _extract(extract), _compare(compare),
          _input(input),
          _input_buffer(input->output_buffer())
    {
        assert(_input != NULL);
        assert(_input_buffer != NULL);
    }

    static query_plan* create_plan(tuple_filter_t* filter,
                                   key_extractor_t* key, packet_t* input)
    {
        c_str action("%s:%s", PACKET_TYPE.data(), key->to_string().data());

        query_plan const** children = new query_plan const*[1];
        children[0] = input->plan();
        return new query_plan(action, filter->to_string(), children, 1);
    }
    
    virtual void declare_worker_needs(resource_declare_t* declare) {
        
        /* need to reserve one SORT worker, ... */
        declare->declare(_packet_type, 1);
        
        _input->declare_worker_needs(declare);
    }
};



/**
 * @brief Sort stage that partitions the input into sorted runs and
 * merges them into a single output run.
 */
class sort_stage_t : public stage_t {

private:


    static const unsigned int MERGE_FACTOR;
    static const unsigned int PAGES_PER_INITIAL_SORTED_RUN;

    
    // state provided by the packet
    tuple_fifo*     _input_buffer;
    key_extractor_t* _extract;
    key_compare_t* _compare;
    size_t              _tuple_size;
    

    typedef list<c_str> run_list_t;


    // all information we need for an active merge
    struct merge_t {
	c_str _output; // name of output file
	run_list_t _inputs;
        tuple_fifo* _signal_buffer;
	
	merge_t () { }
        merge_t(const c_str &output, const run_list_t &inputs, tuple_fifo * signal_buffer)
            : _output(output),
	      _inputs(inputs),
	      _signal_buffer(signal_buffer)
        {
        }
    };

    
    typedef std::map<int, run_list_t> run_map_t;
    typedef std::list<merge_t> merge_list_t;
    typedef std::map<int, merge_list_t> merge_map_t;
    typedef merge_packet_t::buffer_list_t buffer_list_t;
    typedef std::vector<hint_tuple_pair_t> hint_vector_t;


    // used to communicate with the monitor thread
    pthread_t _monitor_thread;
    notify_t  _monitor;


    volatile bool _sorting_finished;
    

    // run/merge management
    run_map_t   _run_map;
    merge_map_t _merge_map;
    
public:

    static const c_str DEFAULT_STAGE_NAME;
    typedef sort_packet_t stage_packet_t;


    sort_stage_t()
        : _input_buffer(NULL), _extract(NULL), _compare(NULL),
          _tuple_size(0), _monitor_thread(0)
    {
    }

    
    ~sort_stage_t() {

        // make sure the monitor thread exits before we do...
        {
	    //This critical section makes sure that the monitor thread will be joined only once.
            critical_section_t cs(_monitor._lock);
            if(_monitor_thread) {
                thread_join<void>(_monitor_thread);
                //MA: Have to make sure it is not breaking anything <-- Indeed it was breaking. But we need the changes in the sort.cpp
                //pthread_kill(_monitor_thread,SIGKILL);
            }
        }
        // also, remove any remaining temp files
        merge_map_t::iterator level_it=_merge_map.begin();
        while(level_it != _merge_map.end()) {
            merge_list_t::iterator it = level_it->second.begin();
            while(it != level_it->second.end()) {
                remove_input_files(it->_inputs);
                ++it;
            }
            ++level_it;
        }
    }

protected:

    virtual void process_packet();
    
private:

    bool final_merge_ready();
    int create_sorted_run(int page_count);

    tuple_fifo* monitor_merge_packets();

    void check_finished_merges();
    void start_new_merges();
    void start_merge(int new_level, run_list_t& runs, int merge_factor);
    void remove_input_files(run_list_t& files);

    // debug
    int print_runs();
    int print_merges();
};



#endif
