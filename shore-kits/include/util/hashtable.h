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

#ifndef __UTIL_HASHTABLE_H
#define __UTIL_HASHTABLE_H

#include <cstdlib>
#include <utility>
#include "util/guard.h"

using std::pair;



/**
 * @brief A generic RAII guard class.
 *
 * This class ensures that the object it encloses will be properly
 * disposed of when it goes out of scope, or with an explicit call to
 * done(), whichever comes first.
 *
 * This class is much like the auto_ptr class, other than allowing
 * actions besides delete upon destruct. In particular it is *NOT
 * SAFE* to use it in STL containers because it does not fulfill the
 * Assignable concept.
 *
 * TODO: make generic and configurable (ie, use templates to determine
 * what "null" and "action" are)
 *
 */
template <class Data, class Key, class ExtractKey, class EqualKey, class EqualData, class HashFcn>
class hashtable {
    
private:

    /* the table */
    int _capacity;
    int _size;
    array_guard_t<Data>  _data;
    array_guard_t<bool>  _exists;
    
    /* key-value functors */
    ExtractKey _extractkey;
    EqualKey   _equalkey;
    EqualData  _equaldata;
    HashFcn    _hashfcn;


public:
    
   
    hashtable(int capacity, ExtractKey extractkey,
              EqualKey equalkey, EqualData equaldata, HashFcn hashfcn)
        : _capacity(capacity)
        , _size(0)
        , _data(new Data[capacity])
        , _exists(new bool[capacity])
        , _extractkey(extractkey)
        , _equalkey(equalkey)
        , _equaldata(equaldata)
        , _hashfcn(hashfcn)
    {
        /* initialize _exists */
        for (int i = 0; i < capacity; i++) {
	    _exists[i] = false;
        }
    }

    
    /**
     * @brief 
     */
    void insert_noresize(Data d) {
        
        /* Check for space. */
        assert(_size < _capacity);
        size_t hash_code = _hashfcn(_extractkey(d));
        int hash_pos = (int)(hash_code % (size_t)_capacity);

        /* Linear probing */
        int pos;
        /* Loop 1: Probe until we find empty slot or hit the end of
           the array. */
        for (pos = hash_pos;
             (pos < _capacity) && _exists[pos]; pos++);
        if (pos == _capacity) {
            /* Reached end of table with no slot. */
            /* Loop 2: Continue probing from beginning of array. */
            for (pos = 0;
                 (pos < _capacity) && _exists[pos]; pos++);
        }
        /* At this point, we had better have stopped because we found
           an empty slot. We already verified at the start of the
           function that there is space available. */
        assert(pos != _capacity);
        assert(! _exists[pos] );

        _data[pos] = d;
        _exists[pos] = true;
        _size++;
    }
        

    /**
     * @brief 
     */
    bool insert_unique_noresize(Data d) {
        
        /* Don't check for free space! */
        size_t hash_code = _hashfcn(_extractkey(d));
        int hash_pos = (int)(hash_code % (size_t)_capacity);

        int pos;
        /* Linear probing */
        /* Loop 1: Probe until we find empty slot, find a copy, or hit
           the end of the array. */
        for (pos = hash_pos;
             (pos < _capacity)
                 && _exists[pos]
                 && !_equaldata(_data[pos],d); pos++);
        if (pos == _capacity) {
            /* Reached end of table with no slot. */
            /* Loop 2: Continue probing from beginning of array. */
            for (pos = hash_pos;
                 (pos < _capacity)
                     && _exists[pos]
                     && !_equaldata(_data[pos],d); pos++);
        }

        /* At this point, we may have reached the end of the array
           again (since we did not check for free space at the
           beginning). We have dealt with this kind of this with an
           assert() before... */
        assert(pos != _capacity);
        
        if (!_exists[pos]) {
            /* Found an empty slot. Insert here. */
            _data[pos] = d;
            _exists[pos] = true;
            _size++;
            return true;
        }

        
        /* If we are here, we must have found a copy of the data. */
        assert( _equaldata(_data[pos],d) );
        return false;
    }


    /**
     * @brief Check for the specified data.
     */
    bool contains(Data d) {

        /* Don't check for free space! */
        size_t hash_code = _hashfcn(_extractkey(d));
        int hash_pos = (int)(hash_code % (size_t)_capacity);

        int pos;
        /* Linear probing */
        /* Loop 1: Probe until we find empty slot, find a copy, or hit
           the end of the array. */
        for (pos = hash_pos;
             (pos < _capacity)
                 && _exists[pos]
                 && !_equaldata(_data[pos],d); pos++);
        if (pos == _capacity) {
            /* Reached end of table with no slot. */
            /* Loop 2: Continue probing from beginning of array. */
            for (pos = hash_pos;
                 (pos < _capacity)
                     && _exists[pos]
                     && !_equaldata(_data[pos],d); pos++);
        }

        if (pos == _capacity) {
            /* table is full and still does not contain 'd' */
            return false;
        }
        
        if (!_exists[pos]) {
            /* Found an empty slot. Table does not contain data. */
            return false;
        }

        /* If we are here, we have found a copy of the data. */        
        assert(_equaldata(_data[pos],d));
        return true;
    }


    void clear() {
        for (int i = 0; i < _capacity; i++)
            _exists[i] = false;
        _size = 0;
    }


    /**
     *  @brief Iterator over the tuples in this page. Each dereference
     *  returns a tuple_t.
     */

    class iterator {

    private:

        /* the enclosing hash table */
        hashtable* _parent;
        
        /* the data we use for equal_range() */
        Key _key;

        /* iterator position management */
        int  _start_index;
        int  _curr_index;

        /* For checking whether an iterator is equal to the END
           iterator. */
        bool _is_end;

        
    public:

	iterator()
	    : _parent(NULL)
            , _is_end(true)
	{
	}

	iterator(hashtable* parent)
	    : _parent(parent)
            , _is_end(true)
	{
	}

        iterator(hashtable* parent, Key const& key, int start_index)
            : _parent(parent)
            , _key(key)
            , _start_index(start_index)
            , _curr_index(start_index)
            , _is_end(false)
        {
            /* If there is no data at this location, immediately END. */
            if (!parent->_exists[start_index]) {
                _is_end = true;
                return;
            }
            
            /* If the data here has a key equal to the key we are
               searching for, stay here. */
            if (parent->_equalkey(key, parent->_extractkey(parent->_data[start_index])))
                return;
            
            /* Otherwise, use ++ to advance to the first key that does
               match (or go to END). */
            ++*this;
        }

        bool operator ==(const iterator &other) const {

            /* Handling the END case is a little tedious. */
            if (_is_end)
                return other._is_end;
            if (other._is_end)
                return _is_end;

            /* This should only be used on iterators from the same
               table. */
            assert(_parent == other._parent);

            /* Iterators should be at the same position, searching for
               the same key. */
            return
                   _start_index == other._start_index
                && _curr_index  == other._curr_index
                && _parent->_equalkey(_key, other._key)
                ;
        }

        bool operator !=(const iterator &other) const {
            return !(*this == other);
        }

	Data& operator*() {
	    return *get();
	}

        Data* operator ->() {
	    return get();
	}

	Data* get() {
            /* Make sure we did not wrap. */
            assert(!_is_end);
          
            if (0) {
                TRACE(TRACE_ALWAYS, "Looking at data %s stored at position %d\n",
                      _parent->_data[_curr_index],
                      _curr_index);
            }

            return &_parent->_data[_curr_index];
        }

        iterator &operator ++() {

            assert(!_is_end);

            int curr = _curr_index;
            while (1) {
                
                /* Continue to increment 'curr' (allowing for
                   wrapping). We can stop if parent's entry at 'curr'
                   is equal to our key. We enter the END state if the
                   parent stores nothing at 'curr' or if 'curr' wraps
                   back to '_start_index'. */
		if(++curr == _parent->_capacity)
		    curr = 0;

                /* check for end cases */
                if (!_parent->_exists[curr]) {
                    _is_end = true;
                    return *this;
                }
                
                if (curr == _start_index) {
                    /* wrapped to the beginning of the table */
                    _is_end = true;
                    return *this;
                }

                if (_parent->_equalkey(_key, _parent->_extractkey(_parent->_data[curr]))) {
                    _curr_index = curr;
                    return *this;
                }
            }
        }

        iterator operator ++(int) {
            iterator old = *this;
            ++*this;
            return old;
        }
    };
    
    
    std::pair<iterator, iterator> equal_range(Key const& k) {
        size_t hash_code = _hashfcn(k);
        int hash_pos = (int)(hash_code % (size_t)_capacity);
        return std::make_pair(iterator(this, k, hash_pos), iterator(this));
    }

};


#endif /** __UTIL_HASHTABLE_H */
