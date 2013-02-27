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

/* hash_join_stage.h */
/* Declaration of the hash_join_stage hash_join_packet classes */
/* History: 
   3/3/2006: Removed the static hash_join_cnt variable of the hash_join_packet_t class, instead it  uses the singleton stage_packet_counter class.
*/


#ifndef __QPIPE_HASH_JOIN_STAGE_H
#define __QPIPE_HASH_JOIN_STAGE_H

#include "qpipe/core.h"
#include "util/hashtable.h"

#if defined(linux) || defined(__linux)
#include <ext/hash_set>
#endif

#include <string>

using std::string;
using std::vector;


ENTER_NAMESPACE(qpipe);


#define HASH_JOIN_STAGE_NAME  "HASH_JOIN"
#define HASH_JOIN_PACKET_TYPE "HASH_JOIN"



/********************
 * hash_join_packet *
 ********************/


class hash_join_packet_t : public packet_t {

public:
    static const c_str PACKET_TYPE;

    guard<packet_t> _left;
    guard<packet_t> _right;
    guard<tuple_fifo> _left_buffer;
    guard<tuple_fifo> _right_buffer;

    guard<tuple_join_t> _join;
    bool _outer;
    bool _distinct;
    
    int count_out;
    int count_left;
    int count_right;
  
    /**
     *  @brief Constructor.
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
     *  @param left Left side-input packet. This will be dispatched
     *  second and become the outer relation of the join. This should
     *  be the larger input.
     *
     *  @param right Right-side packet. This will be dispatched first
     *  and will become the inner relation of the join. It should be
     *  the smaller input.
     *
     *  @param join The joiner  we will be using for this
     *  packet. The packet OWNS this joiner. It will be deleted in
     *  the packet destructor.
     *
     */
    hash_join_packet_t(const c_str &packet_id,
                       tuple_fifo* out_buffer,
                       tuple_filter_t *output_filter,
                       packet_t* left,
                       packet_t* right,
                       tuple_join_t *join,
                       bool outer=false,
                       bool distinct=false)
        : packet_t(packet_id, PACKET_TYPE, out_buffer, output_filter,
                   create_plan(output_filter, join, outer, distinct, left, right),
                   true, /* merging allowed */
                   true  /* unreserve worker on completion */
                   ),
          _left(left),
          _right(right),
          _left_buffer(left->output_buffer()),
          _right_buffer(right->output_buffer()),
          _join(join),
          _outer(outer), _distinct(distinct)
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



/*******************
 * hash_join_stage *
 *******************/

class hash_join_stage_t : public stage_t {


private:


    /* datatypes */

    /* Helper classes used by the hashtable data structure. All of
       these are very short, so give the compiler the option of
       inlining. */
    
    struct extractkey_t {
        
        tuple_join_t *_join;
        bool _right;

        extractkey_t(tuple_join_t *join, bool right)
            : _join(join)
            , _right(right)
        {
        }
        
        const char* const operator()(const char* value) const {
            return _right ? _join->right_key_bytes(value) : _join->left_key_bytes(value);
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

    typedef hashtable<char *,
                      const char *,
                      extractkey_t,
                      equalbytes_t,
                      equalbytes_t,
                      hashfcn_t> tuple_hash_t;
    

    /* These are our bookkeeping data structures. */

    struct partition_t {

        page* _page;
        int size;
        FILE *file;
        c_str file_name1;
        c_str file_name2;

        partition_t()
            : _page(NULL), size(0), file(NULL)
        {
        }
    };

    
    typedef std::vector<partition_t> partition_list_t;


    struct left_action_t {
        void operator ()(partition_list_t::iterator it) {
            it->file = NULL;
        }
    };


    struct right_action_t {
        size_t _left_tuple_size;
        right_action_t(size_t left_tuple_size)
            : _left_tuple_size(left_tuple_size)
        {
        }
        void operator()(partition_list_t::iterator it) {
            // open a new file for the left side partition
            it->file = create_tmp_file(it->file_name2, "hash-join-left");
            
            // resize the page to match left-side tuples
            it->_page = page::alloc(_left_tuple_size);
        }
    };
    
    
    
    /* fields */
    
    int page_quota;
    int page_count;
    tuple_join_t *_join;
    partition_list_t partitions;



    /* methods */
    void test_overflow(int partition);

    template<class Action>
    void close_file(partition_list_t::iterator it, Action a);
    
   

public:

    typedef hash_join_packet_t stage_packet_t;

    static const c_str DEFAULT_STAGE_NAME;


    virtual void process_packet();
    
    
    // Allocate in-memory partitions...
    hash_join_stage_t()
        : page_quota(10000)
        , page_count(0)
        , partitions(512)
    {
    }

    ~hash_join_stage_t() {
    }

};


EXIT_NAMESPACE(qpipe);

#endif	// __HASH_JOIN_H
