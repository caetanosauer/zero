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

#ifndef __QPIPE_FUNCTORS_H
#define __QPIPE_FUNCTORS_H

#include "qpipe/core/tuple.h"
#include <algorithm>



ENTER_NAMESPACE(qpipe);

// use this to force proper alignment of key arrays
#define ALIGNED __attribute__ ((aligned))



/** 
 * @brief Base selection/projection functor. Should be extended to
 * implement other selection/projection functors.
 */

class tuple_filter_t {

private:

    size_t _tuple_size;
    
public:


    size_t input_tuple_size() { return _tuple_size; }

    
    /**
     *  @brief Determine which attributes (if any) of a given source
     *  tuple pass an internal set of predicates. Should be applied to
     *  the output of a stage to implement either selection or
     *  projection.
     *
     *  This default implementation allows every attribute of every
     *  source tuple to pass.
     *
     *  @param src The tuple we are checking.
     *
     *  @return True if the specified tuple passes the
     *  selection. False otherwise.
     */

    virtual bool select(const tuple_t &) {
        return true;
    }


    /**
     *  @brief Project some threads from the src to the dest tuple.
     *
     *  This default implementation copies the entire tuple, so dest
     *  must point to a location with at least src.size bytes
     *  available.
     *
     *  @param src Our source tuple.
     *
     *  @param dest We copy the necessary attributes here. This tuple
     *  should already point into valid memory where we can store
     *  data.
     *
     *  @return void
     */

    virtual void project(tuple_t &dest, const tuple_t &src) {
        dest.assign(src);
    }


    // should simply return new <child-class>(*this);
    virtual tuple_filter_t* clone() const=0;

    // return a canonical representation of this filter
    virtual c_str to_string() const=0;
    

    tuple_filter_t(size_t input_tuple_size)
        : _tuple_size(input_tuple_size)
    {
    }
    

    virtual ~tuple_filter_t() { }

};



struct trivial_filter_t : public tuple_filter_t {
    
    virtual tuple_filter_t* clone() const {
        return new trivial_filter_t(*this);
    };

    virtual c_str to_string() const {
        // nothing happens
        return c_str::EMPTY_STRING;
    }
    
    trivial_filter_t(size_t input_tuple_size)
        : tuple_filter_t(input_tuple_size)
    {
    }
    
};



/**
 * @brief A key-tuple pair used for operations such as hashing and
 * sorting. The first 32 bits of the key (or hash) are stored directly
 * and should suffice for most comparisons; the full key can always be
 * accessed by following the pointer to the tuple's data.
 */

struct hint_tuple_pair_t {
    int hint;
    char *data;

    hint_tuple_pair_t()
        : hint(0), data(NULL)
    {
    }
    hint_tuple_pair_t(int h, char *d)
        : hint(h), data(d)
    {
    }
};



/**
 *  @brief Base comparison functor. Extend it to implement specific
 *  comparisons on tuple field(s).
 */

struct key_compare_t {
    
    /**
     * @brief Determines the lexicographical relationship between key1
     * and key2. Ascending sorts based on this comparison will produce
     * outputs ordered such that compare(key[i], key[i+1]) < 0 for
     * every i.
     *
     * @return negative if a < b, positive if a > b, and zero if a == b
     */
    
    virtual int operator()(const void* key1, const void* key2) const=0;
    virtual key_compare_t* clone() const=0;

    virtual ~key_compare_t() { }
};



/**
 * @brief a key extractor class. Assumes the key is stored
 * contiguously somewhere in the tuple and returns a pointer to the
 * proper offset.
 */
class key_extractor_t {

private:

    size_t _key_size;
    size_t _key_offset;
public:
    key_extractor_t(size_t key_size=sizeof(int), size_t key_offset=0)
        : _key_size(key_size), _key_offset(key_offset)
    {
    }
    size_t key_size() const { return _key_size; }
    size_t key_offset() const { return _key_offset; }


    /**
     * @brief extracts the full key of a tuple. 
     */
    const char* extract_key(const tuple_t &tuple) const {
        return extract_key(tuple.data);
    }
    char* extract_key(tuple_t &tuple) {
        return extract_key(tuple.data);
    }


    /**
     * @brief extracts the full key from a tuple's data.
     *
     * The default implementation assumes the key is the first part of
     * the tuple and simply returns its argument.
     */
    const char* extract_key(const char* tuple_data) const {
        return tuple_data + key_offset();
    }

    char* extract_key(char* tuple_data) {
        return tuple_data + key_offset();
    }

    /**
     * @brief extracts an abbreviated key that represents the most
     * significant 4 bytes of the full key. This allows quicker
     * comparisons but equal shortcut keys require a full key compare
     * to resolve the ambiguity.
     */
    int extract_hint(const tuple_t &tuple) const {
        return extract_hint(extract_key(tuple));
    }

    /**
     * @brief calculates an integer-sized "hint" to accelerate key
     * comparisons.
     *
     * Hints should take values such that hint1 < hint2 implies key1 <
     * key2 and hint1 > hint2 implies key1 > key2.
     *
     * hint1 == hint2 requires a full key comparison *EXCEPT* in the
     * special case where key_size() <= sizeof(int) -- in this case
     * the hint *is* the key (and can be automatically extracted from
     * it by the default implementation).
     *
     * If key_size() > sizeof(int) the hint need not be meaningful, as
     * long as it is not misleading.
     */
    

    virtual int extract_hint(const char* key) const {
        // this guarantees that we're not doing something dangerous
        if(key_size() > sizeof(int))
            return 0;
        
        // clear out any padding that might result from smaller keys
        int hint = 0;
        memcpy(&hint, key, key_size());
        return hint;
    }


    // should simply return new <child-class>(*this);
    virtual key_extractor_t* clone() const=0;

    // return a canonical string representation of this key extractor
    virtual c_str to_string() const {
        return c_str("key(%zd, %zd)", key_size(), key_offset());
    }


    virtual ~key_extractor_t() { }
};


/**
 * @brief extracts hints from a key assuming that it (1) is at least
 * sizeof(int) bytes long and (2) the first sizeof(int) bytes can be
 * used directly as a key hint
 */
struct default_key_extractor_t : public key_extractor_t {
    default_key_extractor_t(size_t key_size=sizeof(int), size_t key_offset=0)
        : key_extractor_t(key_size, key_offset)
    {
    }
    virtual int extract_hint(const char* key) const {
        int result = 0;
        memcpy(&result, key, std::min(sizeof(int), key_size()));
        return result;
    }
    
    virtual default_key_extractor_t* clone() const {
        return new default_key_extractor_t(*this);
    }

};



struct tuple_comparator_t {

    key_extractor_t* _extract;
    key_compare_t* _compare;


    tuple_comparator_t(key_extractor_t* extract, key_compare_t* compare)
        : _extract(extract), _compare(compare)
    {
    }


    int operator()(const hint_tuple_pair_t &a, const hint_tuple_pair_t &b) const {
        int diff = a.hint - b.hint;
        if(_extract->key_size() <= sizeof(int) || diff)
            return diff;

        const char* akey = _extract->extract_key(a.data);
        const char* bkey = _extract->extract_key(b.data);
        return (*_compare)(akey, bkey);
    }
};



struct tuple_less_t {

    tuple_comparator_t _compare;

    tuple_less_t(key_extractor_t* e, key_compare_t *c)
        : _compare(e, c)
    {
    }

    bool operator()(const hint_tuple_pair_t &a, const hint_tuple_pair_t &b) const {
        return _compare(a, b) < 0;
    }
};



/** 
 * @brief Base join functor. Should be used by join stages to
 * determine if two source tuples pass the join predicate. Should be
 * extended to implement other join functors.
 */

class tuple_join_t {

    /* The query must specify where to find the keys of the tuples it
       joins. The keys must be a contiguous region of memory that is
       at some offset into the tuple. */
    size_t _lt_tuple_size;
    size_t _lt_key_offset;
    size_t _rt_tuple_size;
    size_t _rt_key_offset;

    size_t _key_size;

    /* is this necessary? */
    size_t _out_tuple_size;

    /* No need to pass key and tuple comparators. Two keys are equal
       if and only if the bytes of their keys are equal (as stated by
       memcmp()). */
    
public:

    size_t left_tuple_size()   { return _lt_tuple_size; }
    size_t left_key_offset()   { return _lt_key_offset; }
    size_t right_tuple_size()  { return _rt_tuple_size; }
    size_t right_key_offset()  { return _rt_key_offset; }
    size_t key_size()          { return _key_size; }
    size_t output_tuple_size() { return _out_tuple_size; }
    
    tuple_join_t(size_t lt_tuple_size,
                 size_t lt_key_offset,
                 size_t rt_tuple_size,
                 size_t rt_key_offset,
                 size_t k_size,
                 size_t out_tuple_size)
        : _lt_tuple_size(lt_tuple_size)
        , _lt_key_offset(lt_key_offset)
        , _rt_tuple_size(rt_tuple_size)
        , _rt_key_offset(rt_key_offset)
        , _key_size(k_size)
        , _out_tuple_size(out_tuple_size)
    {
    }

    tuple_join_t(tuple_join_t const &other)
        : _lt_tuple_size(other._lt_tuple_size)
        , _lt_key_offset(other._lt_key_offset)
        , _rt_tuple_size(other._rt_tuple_size)
        , _rt_key_offset(other._rt_key_offset)
        , _key_size(other._key_size)
        , _out_tuple_size(other._out_tuple_size)
    {
    }
    
    const char* left_key_bytes(const char* tup_data) {
        return &tup_data[left_key_offset()];
    }

    const char* right_key_bytes(const char* tup_data) {
        return &tup_data[right_key_offset()];
    }
    
    const char* left_key(const tuple_t& tup) {
        return left_key_bytes((const char*)tup.data);
    }

    const char* right_key(const tuple_t& tup) {
        return right_key_bytes((const char*)tup.data);
    }


    /**
     *  @brief Join two tuples that have equal keys. Copy out the
     *  requested attributes from 'left' and 'right' into
     *  'dest'. Should be applied within a join stage once we have
     *  determined that two tuples have matching join attributes.
     *
     *  @param dest If the two tuples pass the specified join
     *  predicates, a composite of the two is assigned here.
     *
     *  @param A tuple from the outer relation.
     *
     *  @param A tuple from the inner relation.
     */
    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)=0;

    /**
     *  @brief If we are doing an left outer join, then we need a
     *  function we can call on a left tuple that does not match
     *  against any tuples on the right. This function does the
     *  equivalent of 'join' for left tuples that do not match tuples
     *  any tuples on the right.
     *
     *  @brief We can compute right outer joins by switching the order
     *  of the input relations to our hash join.
     *
     *  @param dest The output.
     *
     *  @param left The left tuple that did not hit anything on the
     *  right. 
     */
    virtual void left_outer_join(tuple_t &,
                                 const tuple_t &) {
        /* Provide default implementaiton since most tuple_join_t
           functors will never be used to perform outer joins. */
        assert(false);
    }

    virtual c_str to_string() const=0;

    // TODO: implement a join function that tests the predicates first
    virtual ~tuple_join_t() { }
};



/** 
 * @brief Base aggregation functor. Should be extended to implement
 * other aggregation functors.
 */
class tuple_aggregate_t {
    size_t _tuple_size;
public:
    tuple_aggregate_t(size_t tuple_size)
        : _tuple_size(tuple_size)
    {
    }

    /**
     * @brief size of an aggregate tuple (not necessarily the same as
     * the stage's input or output tuple size!)
     */
     
    size_t tuple_size() { return _tuple_size; }
    
    virtual key_extractor_t* key_extractor()=0;
    
    /**
     * @brief "Zeros out" the data fields of an aggregate tuple,
     * preparing it for first use. The stage will set the key field(s)
     * after calling this function.
     *
     * The default implementation simply memsets the data to zeroes.
     */
    
    virtual void init(char* agg_data) {
        memset(agg_data, 0, tuple_size());
    }
    

    
    /**
     *  @brief Applies a tuple to an aggregate's state
     */

    virtual void aggregate(char* agg_data, const tuple_t &tuple)=0;


    
    /**
     * @brief Merges (other_agg)'s internal state into (agg)
     *
     * This function allows partially aggregated runs to be merged.
     */
    
    virtual void merge(char*, const char*) {
        assert(false);
    }

    /**
     *  @brief Convert the internal aggregate state into a final
     *  output tuple. This method will be invoked when there are no
     *  more tuples to be aggregated for this group.
     *
     * This function allows running totals (eg sum and count) to be
     * converted into the desired results (eg avg).
     */

    virtual void finish(tuple_t &dest, const char* agg_data)=0;


    virtual tuple_aggregate_t* clone() const=0;

    virtual c_str to_string() const=0;
  
    virtual ~tuple_aggregate_t() { }

};



/**
 * @brief Used by the sieve stage. Do not confuse a tuple_sieve_t with
 * a tuple_filter_t. At the time of this writing, we apply a
 * tuple_filter_t (select and project) at every stage's output to
 * reduce the size of the data we pass up the plan. tuple_sieve_t only
 * execute at sieve nodes within that plan, nodes that exist only to
 * provide further pipelined parallelism.
 */
class tuple_sieve_t {
    
    size_t _tuple_size;

public:
    tuple_sieve_t(size_t tuple_size)
        : _tuple_size(tuple_size)
    {
    }

    /**
     * @brief Size of output tuple.
     */
     
    size_t tuple_size() { return _tuple_size; }
    
    
    /**
     *  @brief Return true if an output tuple is produced.
     */
    virtual bool pass(tuple_t& dest, const tuple_t &src)=0;
    

    /**
     *  @brief Return true of an output tuple is produced.
     */
    virtual bool flush(tuple_t&) {
        return false;
    }

    virtual tuple_sieve_t* clone() const=0;
    
    virtual c_str to_string() const=0;
  
    virtual ~tuple_sieve_t() { }
};



EXIT_NAMESPACE(qpipe);



#endif	// __FUNCTORS_H
