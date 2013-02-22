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

/** @file:   key.h
 *
 *  @brief:  Implementation of a template-based Key class
 *
 *  @author: Ippokratis Pandis, Oct 2008
 */

#ifndef __DORA_KEY_H
#define __DORA_KEY_H

#include "k_defines.h"

#include <iostream>
#include <sstream>
#include <vector>

#include "sm/shore/shore_env.h"


ENTER_NAMESPACE(dora);

using std::vector;

template<typename DataType> struct key_wrapper_t;
template<typename DataType> std::ostream& operator<< (std::ostream& os,
                                                      const key_wrapper_t<DataType>& rhs);


// A key can constitue by up to 5 DataType entries
const uint MAX_KEY_SIZE = 5; 

/******************************************************************** 
 *
 * @struct: key_wrapper_t
 *
 * @brief:  Template-based class used for Keys
 *
 * @note:   - Wraps a vector of key entries. Needed for STL  
 *          - All the entries of the key of the same type
 * 
 *
 ********************************************************************/

template<typename DataType>
struct key_wrapper_t
{
    //typedef typename PooledVec<DataType>::Type        DataVec;
    typedef std::vector<DataType>            DataVec;
    typedef typename DataVec::iterator       DataVecIt;
    typedef typename DataVec::const_iterator DataVecCit;

    // the vector with the entries - of the same type
    DataVec _key_v;

    // empty constructor
    key_wrapper_t() { }

    // copying needs to be allowed (stl...)
    key_wrapper_t(const key_wrapper_t<DataType>& rhs)
    {
        // if already set do not reallocate        
        //        _key_v = new DataVec( rhs._key_v->get_allocator() );
        copy_vector(rhs._key_v);
    }
    
    // copy constructor
    key_wrapper_t<DataType>& operator=(const key_wrapper_t<DataType>& rhs) 
    {        
        //_key_v = new DataVec( rhs._key_v->get_allocator() );
        copy_vector(rhs._key_v);
        return (*this);
    }
    
    // destructor
    ~key_wrapper_t() { }

    // push one item
    inline void push_back(DataType& anitem) {
        _key_v.push_back(anitem);
    }

    // reserve vector space
    inline void reserve(const uint keysz) {
        _key_v.reserve(keysz);
    }

    // drops the key
    //inline void drop() { if (_key_v) delete (_key_v); }

    inline void copy(const key_wrapper_t<DataType>& rhs) {
        copy_vector(rhs._key_v);
    }
    

    // helper functions
    inline void copy_vector(const DataVec& aVec) {
        assert (_key_v.empty());
        _key_v.reserve(aVec.size());
        _key_v.assign(aVec.begin(),aVec.end()); // copy vector content
    }

    // Returns a corresponding cvec_t 
    cvec_t toCVec() const {
        cvec_t acv;
        for (uint i=0; i<_key_v.size(); ++i) {
            acv.put(&_key_v[i],sizeof(DataType));
        }
        return (acv);
    }

    // Sets the key based on a cvec_t
    // Returns the number of DataTypes read
    uint readCVec(const cvec_t& acv) {
        // Clear key contents, if any
        reset();
        
        // Read the cvec_t into a char*
        uint sz = MAX_KEY_SIZE * sizeof(DataType);
        char* co = (char*)malloc(sz);
        size_t bwriten = acv.copy_to(co,sz);
        size_t bread = 0;

        // Read the DataTypes and insert them to the key vector
        uint dtread = 0;
        while (bread < bwriten) {
            DataType adt = *(DataType*)(co[bread]);
            _key_v.push_back(adt);

            // move to next
            bread += sizeof(DataType);
            ++dtread;
        }
        free (co);
        return (dtread);
    }

    // comparison operators
    bool operator<(const key_wrapper_t<DataType>& rhs) const;
    bool operator==(const key_wrapper_t<DataType>& rhs) const;
    bool operator<=(const key_wrapper_t<DataType>& rhs) const;


    // CACHEABLE INTERFACE

    void init() { }

    // Clear contents
    void reset() {
        _key_v.erase(_key_v.begin(),_key_v.end());
    }

    string toString() {
        std::ostringstream out = string("");
        for (DataVecIt it=_key_v.begin(); it!=_key_v.end(); ++it)
            out << out << (*it) << "|";
        return (out.str());
    }

    // friend function
    template<class T> friend std::ostream& operator<< (std::ostream& os, 
                                                       const key_wrapper_t<T>& rhs);

}; // EOF: struct key_wrapper_t


template<typename DataType> 
std::ostream& operator<< (std::ostream& os,
                          const key_wrapper_t<DataType>& rhs)
{
    typedef typename key_wrapper_t<DataType>::DataVecCit KeyDataIt;
    for (KeyDataIt it = rhs._key_v.begin(); it != rhs._key_v.end(); ++it) {
        os << (*it) << "|";
    }
    return (os);
}


//// COMPARISON OPERATORS ////



// @todo - IP: I am not sure if the rhs.size should be larger than the key.size.
//             for example for range queries, the queried key may be a shorter version
//             of the keys in the structure.
//
// workaround: 
//
// minsize = min(_key_v.size(), rhs._key_v.size());
// for (int i=0; i<minsize; ++) { ... }
//


// less
template<typename DataType>
inline bool key_wrapper_t<DataType>::operator<(const key_wrapper_t<DataType>& rhs) const 
{
    assert (_key_v.size()<=rhs._key_v.size()); // not necesserily of the same length
    for (uint i = 0; i <_key_v.size(); ++i) {
        // goes over the key fields until one inequality is found
        if (_key_v[i]==rhs._key_v[i])
            continue;
        return (_key_v[i]<rhs._key_v[i]);
    }
    return (false); // irreflexivity - f(x,x) must be false
}

// equal
template<typename DataType>
inline bool key_wrapper_t<DataType>::operator==(const key_wrapper_t<DataType>& rhs) const 
{    
    assert (_key_v.size()<=rhs._key_v.size()); // not necesserily of the same length
    for (uint i=0; i<_key_v.size(); i++) {
        // goes over the key fields until one inequality is found
        if (_key_v[i]==rhs._key_v[i])
            continue;
        return (false);        
    }
    return (true);
}

// less or equal
template<typename DataType>
inline bool key_wrapper_t<DataType>::operator<=(const key_wrapper_t<DataType>& rhs) const 
{
    assert (_key_v.size()<=rhs._key_v.size()); // not necesserily of the same length
    for (uint i=0; i<_key_v.size(); i++) {
        // goes over the key fields
        if (_key_v[i]==rhs._key_v[i])
            continue;
        return (_key_v[i]<rhs._key_v[i]);
    }
    // if reached this point all fields are equal so the two keys are equal
    return (true); 
}



EXIT_NAMESPACE(dora);

#endif /* __DORA_KEY_H */
