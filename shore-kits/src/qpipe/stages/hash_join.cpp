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


#include "qpipe/stages/hash_join.h"

#include <cstring>
#include <algorithm>


ENTER_NAMESPACE(qpipe);


const c_str hash_join_packet_t::PACKET_TYPE = "HASH_JOIN";

const c_str hash_join_stage_t::DEFAULT_STAGE_NAME = "HASH_JOIN";



void hash_join_stage_t::process_packet() {

    hash_join_packet_t* packet = (hash_join_packet_t *)_adaptor->get_packet();

    /* TODO: release partition resources! */
    bool outer_join = packet->_outer;
    _join = packet->_join;
    bool distinct = packet->_distinct;
    

    /* TERMINOLOGY: The 'right' relation is the inner relation. The
       'left' relation is the outer relation. With left-deep query
       plans, the right relation will be a table scan. */


    /* First divide the right relation into partitions. */
    tuple_fifo *right_buffer = packet->_right_buffer;
    dispatcher_t::dispatch_packet(packet->_right);
    tuple_fifo *left_buffer = packet->_left_buffer;
    dispatcher_t::dispatch_packet(packet->_left);


    /* Quick check for no-tuple case. */
    if(!right_buffer->ensure_read_ready()) {
        /* No right side tuples! "Normal" (inner) join returns
           nothing. Outer join returns everything in left relation
           with appropriate null values. */
        /* TODO Handle outer join here. */
        return;
    }
    
    
    hash_join_stage_t::extractkey_t extract_left (_join, false);
    hash_join_stage_t::extractkey_t extract_right(_join, true);
    hash_join_stage_t::hashfcn_t    hashfcn(_join->key_size());

    
    /* Continue with building hash partitions of 'right'
       relation. Read each tuple and assign it to the appropriate
       partition. We don't have a "primary" partition and secondary
       partitions.  If any of our partitions fill up, we flush to
       disk. */
    tuple_t right;
    while(1) {

        /* check for EOF */
        if(!right_buffer->get_tuple(right))
            break;

        /* Identify the partition that needs this tuple. */
        size_t hash_code = hashfcn(extract_right(right.data));
        int    hash_int  = (int)hash_code;
        int    partition = hash_int % partitions.size();

        /* Simple optimization: Flush _before_ inserting into a full
           page, not after we fill a page. This can avoid one
           unnecessary flush per partition. */
        // assert(0);
        test_overflow(partition);

        /* If the partition was full, we would have flushed its data
           and cleared it of tuples. We can now safely append to the
           page. */
        qpipe::page* &p = partitions[partition]._page;
        p->append_tuple(right);
    }

    /* TODO Flush all partitions to disk and free the partition
       memory. */

    /* We now have the right relation sitting on disk in partition
       files. */

    /* Create and fill the in-memory hash table. */
    size_t page_capacity =
        qpipe::page::capacity(get_default_page_size(),
                              _join->right_tuple_size());

    extractkey_t right_key_extractor(_join, true);
    equalbytes_t equal_key (_join->key_size());
    equalbytes_t equal_rtup(_join->right_tuple_size());
    hashfcn_t    hasher(_join->key_size());
    
    tuple_hash_t table(page_count * page_capacity,
                       right_key_extractor,
                       equal_key,
                       equal_rtup,
                       hasher);
    
    /* Flush any partitions that went to disk */
    right_action_t right_action(_join->left_tuple_size());
    for(partition_list_t::iterator it=partitions.begin(); it != partitions.end(); ++it) {

        qpipe::page* p = it->_page;
        if(p == NULL)
            // empty partition
            continue;

        // file partition? (make sure the file gets closed)
        if(it->file) 
            close_file(it, right_action);
        
        // build hash table out of in-memory partition
        else {
            while(p) {
                for(qpipe::page::iterator it=p->begin(); it != p->end(); ++it) {
                    // Distinguish between DISTINCT join and
                    // non-DISTINCT join, as DB/2 does
                    if(distinct)
                        table.insert_unique_noresize(it->data);
                    else
                        table.insert_noresize(it->data);
                }

                p = p->next;
            }
        }
    }


    // start building left side hash partitions
    if(!left_buffer->ensure_read_ready())
        // No left-side tuples... no join tuples.
        return;

    
    // read in the left relation now
    extractkey_t left_key_extractor(_join, false);
    tuple_t left(NULL, _join->left_tuple_size());
    array_guard_t<char> data = new char[_join->output_tuple_size()];
    while(1) {

        // eof?
        if(!left_buffer->get_tuple(left))
            break;
     
        // which partition?
        const char* left_key = left_key_extractor(left.data);
        int hash_code = hasher(left_key);
        int partition = hash_code % partitions.size();
        partition_t &p = partitions[partition];

        // empty partition?
        if(p.size == 0)
            continue;

        // add to file partition?
        if(p.file) {
            qpipe::page* pg = p._page;

            // flush to disk?
            if(pg->full()) {
                pg->fwrite_full_page(p.file);
                pg->clear();
            }

            // add the tuple to the page
            pg->append_tuple(left);
        }

        // check in-memory hash table
        else {
            std::pair<tuple_hash_t::iterator, tuple_hash_t::iterator> range;
            range = table.equal_range(left_key);

            tuple_t out(data, _join->output_tuple_size());
            if(outer_join && range.first == range.second) {
                _join->left_outer_join(out, left);
                _adaptor->output(out);
            }
            else {
                for(tuple_hash_t::iterator it = range.first; it != range.second; ++it) {
                    right.data = *it;
                    _join->join(out, left, right);
                    _adaptor->output(out);
                }
            }
        }
    }

    // close all the files and release in-memory pages
    table.clear();
    for(partition_list_t::iterator it=partitions.begin(); it != partitions.end(); ++it) {
        // delete the page list
        for(guard<qpipe::page> pg = it->_page; pg; pg = pg->next);
        if(it->file) 
            close_file(it, left_action_t());
    }

    // TODO: handle the file partitions now...

}



void hash_join_stage_t::test_overflow(int partition) {

    partition_t &p = partitions[partition];

    /* check for room on the current page */
    if(p._page && !p._page->full())
        return ;
    
    
    /* A partition is either a list of in-memory pages strung together
       or a file on disk with one in-memory page. 'page_count' is the
       total number of in-memory pages used by all partitions.

       As long as 'page_count' is less than 'page_quota', we grow
       in-memory partitions by simply tacking other pages onto the end
       their lists. When 'page_count' reaches 'page_quota', we pick
       the largest in-memory partition and turn it into a disk
       partition. */
    if(page_count == page_quota) {
        
        /* We need to flush to disk. We will flush the biggest
           partition. */
        
        /* Find the biggest in-memory partition. */
        int max = -1;
        for(unsigned i=0; i < partitions.size(); i++) {
            partition_t &p = partitions[i];
            if(!p.file && (max < 0 || p.size > partitions[max].size))
                max = i;
        }

        /* Create a file on disk. */
        partition_t &p = partitions[max];
        p.file = create_tmp_file(p.file_name1, "hash-join-right");

        /* Send the partition to the file. */
        guard<qpipe::page> head;
        for(head = p._page; head->next; head=head->next) {
            head->fwrite_full_page(p.file);
            page_count--;
        }
        
        /* Write the last page, but don't free it. */
        head->fwrite_full_page(p.file);
        head->clear();
        p._page = head.release();
    }
    else {
        
        /* No need to flush to disk. Simply add a page to the full
           in-memory partition. */
        qpipe::page* pg =
            qpipe::page::alloc(_join->right_tuple_size());
        pg->next = p._page;
        p._page = pg;
        page_count++;
    }

    // done!
    p.size++;
}



template <class Action>
void hash_join_stage_t::close_file(partition_list_t::iterator it, Action action) {

    /* I think this function is supposed to have no effect if called
       on an in-memory partition. If it is called on a file partition,
       it is supposed to flush the partition to disk. */
    qpipe::page *p = it->_page;
    
    /* File partition? */
    /* Write remaining tuples to disk and apply 'action' to it. */
    file_guard_t file = it->file;
    if(!p || p->empty())
        return;
    
    p->fwrite_full_page(file);
    action(it);
}

EXIT_NAMESPACE(qpipe);
