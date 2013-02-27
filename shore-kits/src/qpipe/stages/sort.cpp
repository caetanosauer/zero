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

#include "qpipe/stages/sort.h"
#include "qpipe/stages/merge.h"
#include "qpipe/stages/fdump.h"
#include "qpipe/stages/fscan.h"

#include <algorithm>
#include <string>
#include <cstdlib>
#include <deque>
#include <map>
#include <list>

using std::string;
using std::deque;
using std::map;
using std::list;




const c_str sort_packet_t::PACKET_TYPE = "SORT";



const c_str sort_stage_t::DEFAULT_STAGE_NAME = "SORT_STAGE";

const unsigned int sort_stage_t::MERGE_FACTOR = 8;

const unsigned int sort_stage_t::PAGES_PER_INITIAL_SORTED_RUN = 8 * 1024;



static void flush_page(qpipe::page* pg, FILE* file);



/**
 *  @brief At any point in our merge sort, we have a set of "runs"
 *  (sorted output files) and a set of active merge operations. This
 *  function checks the set of active merges to see if any of them
 *  have completed. If so, we promote the resulting sorted file to the
 *  "run set".
 */
void sort_stage_t::check_finished_merges() {

    
    /* The merge operations required to complete the sort form a
       tree. The lowest level (0) The _merge_map contains one entry
       per level in the merge tree.  0 being the lowest) to a
       merge_list_t (a list of merge operations happening at that
       level). */

    merge_map_t::iterator level_it = _merge_map.begin();
    while(level_it != _merge_map.end()) {
        int level = level_it->first;
        merge_list_t &merges = level_it->second;
        merge_list_t::iterator it = merges.begin();
        while(it != merges.end()) {
            // finished? 
            if(it->_signal_buffer->eof()) {

                // move it to the finished run list
                _run_map[level].push_back(it->_output);
                TRACE(TRACE_DEBUG, "Added finished merge file %s to _run_map[%d]\n",
                      it->_output.data(),
                      level);

                // delete the merge record and any input files it used
                remove_input_files(it->_inputs);
                it = merges.erase(it);
            }
            else {
                // next!
                ++it;
            }
        }

        // if that was the last merge at this level, remove it
        merge_map_t::iterator old_level_it = level_it++;
        if(merges.empty())
            _merge_map.erase(old_level_it);
    }
}



void sort_stage_t::start_merge(int new_level, run_list_t& runs, int merge_factor)
{

    assert(merge_factor > 0);
    assert(runs.size() >= (unsigned int)merge_factor);


    // allocate a new merge
    merge_list_t &merges = _merge_map[new_level];
    merges.resize(merges.size()+1);
    merge_t &merge = merges.back();


    run_list_t merge_inputs;
    buffer_list_t fscan_buffers;


    run_list_t::iterator it = runs.begin();
    for(int i = 0; i < merge_factor; i++) {

        // grab one of our runs
	c_str run_filename = *it;
        it = runs.erase(it);
	
        fscan_packet_t* p;
        tuple_fifo* buf = new tuple_fifo(_tuple_size);


        c_str fscan_packet_id("SORT_FSCAN_PACKET_%d", i);

        p = new fscan_packet_t(fscan_packet_id,
			       buf,
                               new trivial_filter_t(_tuple_size),
                               run_filename.data());
        dispatcher_t::dispatch_packet(p);

	merge._inputs.push_back(run_filename.data());
	fscan_buffers.push_back(buf);
    }


    // create a merge packet to consume the fscans
    merge_packet_t* mp;
    tuple_fifo* merge_out = new tuple_fifo(_tuple_size);

    TRACE(TRACE_ALWAYS,
          "Argh! Sending merge packet... This means we need to get serial merges working\n");
    assert(0);
    mp = new merge_packet_t("SORT_MERGE_PACKET",
                            merge_out,
                            new trivial_filter_t(_tuple_size),
                            fscan_buffers,
                            _extract->clone(), _compare->clone());
    dispatcher_t::dispatch_packet(mp);


    // if this is the last merge run, send output to SORT worker
    // thread, not to an FDUMP
    if(new_level < 0) {
        merge._signal_buffer = merge_out;
        return;
    }
    
    
    // otherwise, redirect merge to an FDUMP so we create a new run on
    // disk
    fdump_packet_t* fp;
    tuple_fifo* fdump_out;
    c_str file_name;


    // KLUDGE! the fdump stage will reopen the file
    fclose(create_tmp_file(file_name, "merged-run"));
    fdump_out = new tuple_fifo(_tuple_size);
    
    fp = new fdump_packet_t("SORT_FDUMP_PACKET",
                            fdump_out,
                            new trivial_filter_t(merge_out->tuple_size()),
                            merge_out,
                            file_name.data(), 
                            &_monitor);
    dispatcher_t::dispatch_packet(fp);


    merge._output = file_name;
    merge._signal_buffer = fdump_out;
}



/**
 * @brief searches each level of the merge hierarchy and fires off a
 * new merge whenever there are MERGE_FACTOR finished runs.
 */
void sort_stage_t::start_new_merges() {

    
    TRACE(TRACE_DEBUG, "_run_map has size %zd\n",
	  _run_map.size());
    

    // start up as many new runs as possible
    run_map_t::iterator level_it = _run_map.begin();
    while(level_it != _run_map.end()) {

	int level = level_it->first;
	TRACE(TRACE_DEBUG, "Running on level %d\n", level);

        
        run_list_t &runs = level_it->second;
        bool started_merges = false;
	while(runs.size() >= MERGE_FACTOR) {
	    // "normal" k-way merges
            start_merge(level+1, runs, MERGE_FACTOR);
            started_merges = true;
        }

        // increment the iterator. rename for clarity
        run_map_t::iterator curr_level_it = level_it++;
        run_map_t::iterator next_level_it = level_it;

	TRACE(TRACE_DEBUG, "curr_level = %d ; next_level = %d\n",
	      curr_level_it->first,
	      next_level_it->first);
        

        // If we have no runs on this level, erase this level's
        // _run_map entry. Now a level has no runs if no mapping
        // exists, not if a mapping exists to a set of size 0.
        if(runs.empty()) {
            _run_map.erase(curr_level_it);
            continue;
        }
        
        // special case -- if sorting has finished, we can't
        // necessarily wait for MERGE_FACTOR runs to arrive (they may
        // never come)
        if(!_sorting_finished || started_merges)
            continue;


	// Try to find (materialized) runs above this
        // level. 'next_level_it' is set to -1 if no runs exist.
        // Otherwise, it is set to the lowest level (above us) with
        // such runs available.
        int next_run_level =
	    (next_level_it == _run_map.end()) ? -1 : next_level_it->first;

	
        // Try to find in-progress merges at or below this
        // level. 'lowest_merge_level' is set to -1 if no merges are
        // found. Otherwise, it is set to the lowest level where a
        // merge is taking place.
        merge_map_t::iterator merges = _merge_map.begin();
        int lowest_merge_level = (merges == _merge_map.end())? -1 : merges->first;


	TRACE(TRACE_DEBUG, "lowest_merge_level = %d ; next_run_level = %d\n",
	      lowest_merge_level,
	      next_run_level);

	
        // We reduce system I/O when we merge small files together
        // before moving them up the merge hierarchy.


        int next_size, next_level;
	if ((lowest_merge_level >= 0) && (lowest_merge_level <= level)) {
	    // There are merges happening below us. Wait for them to
	    // finish. We really want to merge with the result of
	    // every merge below before shipping work to higher levels
	    // of merge hierarchy.
	    continue;
	}
	else if ((lowest_merge_level < 0) && (next_run_level < 0)) {
	    // No merges taking place in the system and no runs above
	    // us. Submit one final merge request. start_merge()
	    // should redirect output to the SORT worker thread and
	    // optimize for the single-merge case.
	    start_merge(-1, runs, runs.size());
	    _run_map.erase(curr_level_it);
	    continue;
	}
	else if (next_run_level < 0) {
	    // No runs above us, but there are merges taking place!
	    // Move up to the output level of the lowest merge.
	    next_level = lowest_merge_level;
	    next_size  = merges->second.size();
	}
        else if (lowest_merge_level < 0) {
            // No merges taking place, but there are runs above us
            next_level = next_run_level;
            next_size  = next_level_it->second.size();
        }
        else if(lowest_merge_level > next_run_level) {
            // only runs at the next level
            next_level = next_run_level;
            next_size  = next_level_it->second.size();
        }
        else if(lowest_merge_level < next_run_level) {
            // only merges at the next level
            next_level = lowest_merge_level;
            next_size  = merges->second.size();
        }
        else {
            // both runs and merges at the next level
            next_level = lowest_merge_level;
            next_size = merges->second.size() + next_level_it->second.size(); 
	}


	TRACE(TRACE_DEBUG, "next_size = %d ; next_level = %d\n",
	      next_size,
	      next_level);

                
        // promote or partial merge?
        int required_merges = (next_size + MERGE_FACTOR-1)/MERGE_FACTOR;
        int potential_merges = (next_size + runs.size() + MERGE_FACTOR-1)/MERGE_FACTOR;
        if(potential_merges > required_merges) {
            // partial merge
            start_merge(next_level, runs, runs.size());
        }
        else {
            // promote up
            run_list_t &next_runs = _run_map[next_level];
            next_runs.insert(next_runs.end(), runs.begin(), runs.end());
        }

        // no more runs at this level. Erase it so the special case
        // check works right
        _run_map.erase(curr_level_it);
    }
}



/**
 * @brief Executed by a separate thread to manage the hierachical
 * merge. Spends most of its time blocked, so very little overhead --
 * we just need a way to detect asynchronous merge completions and
 * handle them.
 */
tuple_fifo *sort_stage_t::monitor_merge_packets() {
    // always in a critical section, but usually blocked on cond_wait
    critical_section_t cs(_monitor._lock);
    while(1) {
        // wait for a merge to finish
        if(_monitor.wait_holding_lock())
	{
	    pthread_exit(NULL);
#warning "MA: Seems to be working but we have to verify nothing breaks, also changed the next return to pthread_exit"
	    //return NULL;
	}
        // find the finished merge run(s)
        check_finished_merges();

        // fire off new merges as needed
        start_new_merges();

        // have we started the final merge? The worker thread will take care of it
        merge_map_t::iterator merge = _merge_map.begin();

        if(merge != _merge_map.end() && merge->first < 0)
	{
	    pthread_exit(&merge->second.front()._signal_buffer);
	    //return merge->second.front()._signal_buffer;
	}
    }
    return NULL;//Put here to be able to compile
}



void sort_stage_t::process_packet() {

    sort_packet_t *packet = (sort_packet_t *)_adaptor->get_packet();

    _input_buffer = packet->_input_buffer;
    _tuple_size = _input_buffer->tuple_size();
    _compare = packet->_compare;
    _extract = packet->_extract;
    _sorting_finished = false;


    dispatcher_t::dispatch_packet(packet->_input);


    // quick optimization: if no input tuples, simply return
    if(!_input_buffer->ensure_read_ready())
        return;
    
    // create a buffer page for writing to file
    guard<qpipe::page> out_page = qpipe::page::alloc(_tuple_size);
    
    // create a key array 
    int capacity =
        qpipe::page::capacity(_input_buffer->page_size(), _tuple_size);
    int tuple_count = PAGES_PER_INITIAL_SORTED_RUN * capacity;
    hint_vector_t array;
    array.reserve(tuple_count);



    // use a monitor thread to control the hierarchical merge
    thread_t *monitor;
    monitor = member_func_thread(this,
                                 &sort_stage_t::monitor_merge_packets,
                                 "MERGE_MONITOR_THREAD");
    _monitor_thread = thread_create(monitor);


    // create sorted runs
    bool first_run = true;
    do {
        // TODO: check for stage cancellation at regular intervals
        
        page_trash_stack pages;
        array.clear();
        for(unsigned int i=0; i < PAGES_PER_INITIAL_SORTED_RUN; i++) {

            // read in a run of pages
            qpipe::page* p = qpipe::page::alloc(_input_buffer->tuple_size());
            if (!_input_buffer->copy_page(p)) {
                p->free();
                break;
            }

            // add new page to the list
            pages.add(p);

            // add the tuples in the page into the key array
            for(qpipe::page::iterator it=p->begin(); it != p->end(); ++it) {
                int hint = _extract->extract_hint(*it);
                array.push_back(hint_tuple_pair_t(hint, it->data));
            }
        }

        // sort the key array (gotta love the STL!)
        std::sort(array.begin(), array.end(), tuple_less_t(_extract, _compare));

         // are we done?
        bool eof = !_input_buffer->ensure_read_ready();

        // shortcut if we fit in memory...
        if(first_run && eof) {
            _monitor.cancel();
            tuple_t out(NULL, packet->_output_filter->input_tuple_size());
            for(hint_vector_t::iterator it=array.begin(); it != array.end(); ++it) {
                out.data = it->data;
                _adaptor->output(out);
            }

            return;
        }

        first_run = false;
        
        // open a temp file to hold the run
        c_str file_name;
        guard<FILE> file = create_tmp_file(file_name, "sorted-run");

        // dump the run to file
        //        for(int i=0; i < index; i++) {
        for(hint_vector_t::iterator it=array.begin(); it != array.end(); ++it) {
            // write the tuple
            tuple_t out(it->data, _tuple_size);
            out_page->append_tuple(out);

            // flush?
            if(out_page->full())
                flush_page(out_page, file);
        }

        // make sure to pick up the stragglers
        if(!out_page->empty())
            flush_page(out_page, file);

        // notify the merge monitor thread that another run is ready
        critical_section_t cs(_monitor._lock);
	run_list_t &runs = _run_map[0];
        runs.push_back(file_name);
        _sorting_finished = eof;
        _monitor.notify_holding_lock();

	TRACE(TRACE_DEBUG, "Added file %s to _run_map[0]\n", file_name.data());

    } while(!_sorting_finished);
    
    // wait for the output buffer from the final merge to arrive
    tuple_fifo* merge_output = thread_join<tuple_fifo>(_monitor_thread);

    {
        critical_section_t cs(_monitor._lock);
        _monitor_thread = 0;
    }
    if(merge_output == NULL)
        THROW1(QPipeException, "Merge failed. Terminating Sort");
    

    // transfer the output of the last merge to the stage output
    tuple_t out;
    while (merge_output->get_tuple(out))
        _adaptor->output(out);
    
}



/**
 * @brief flush (page) to (file) and clear it. PANIC on error.
 */
static void flush_page(qpipe::page* pg, FILE* file) {
    pg->fwrite_full_page(file);
    pg->clear();
}



int sort_stage_t::print_runs() {
    run_map_t::iterator level_it = _run_map.begin();
    for( ; level_it != _run_map.end(); ++level_it) {
        int level = level_it->first;
        run_list_t &runs = level_it->second;
        TRACE(TRACE_ALWAYS, "Level %d (%zd):\n", level, runs.size());
        run_list_t::iterator it = runs.begin();
        for( ; it != runs.end(); ++it)
            TRACE(TRACE_ALWAYS, "\t%s\n", it->data());
    }
    return 0;
}



int sort_stage_t::print_merges() {
    merge_map_t::iterator level_it = _merge_map.begin();
    for( ; level_it != _merge_map.end(); ++level_it) {
        int level = level_it->first;
        merge_list_t &merges = level_it->second;
        TRACE(TRACE_ALWAYS, "Level %d (%zd)\n", level, merges.size());
        merge_list_t::iterator it = merges.begin();
        for( ; it != merges.end(); ++it) {
            TRACE(TRACE_ALWAYS, "\t%s <=\n", it->_output.data());
            run_list_t::iterator run_it = it->_inputs.begin();
            for( ; run_it != it->_inputs.end(); ++run_it) 
                TRACE(TRACE_ALWAYS, "\t\t%s\n", run_it->data());
        }
    }
    return 0;
}



void sort_stage_t::remove_input_files(run_list_t& files) {
    // delete the files from disk. The delete will occur as soon as
    // all current file handles are closed
    for(run_list_t::iterator it=files.begin(); it != files.end(); ++it) {
        if(remove(it->data()))
            TRACE(TRACE_ALWAYS, "Unable to remove temp file %s", it->data());
	TRACE(TRACE_TEMP_FILE, "Removed finished temp file %s\n", it->data());
    }
}
