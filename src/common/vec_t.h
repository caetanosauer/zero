/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

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

/*<std-header orig-src='shore' incl-file-exclusion='VEC_T_H'>

 $Id: vec_t.h,v 1.67 2010/12/08 17:37:34 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef VEC_T_H
#define VEC_T_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/* NB: you must already have defined the type size_t,
 * (which is defined include "basics.h") before you include this.
 */

typedef const unsigned char * CADDR_T;
#define MAX_SMALL_VEC_SIZE 8
class w_keystr_t;
/*
 * Newer c++ compilers require
 * that copy constructors be available for classes which use anonymous
 * temporary variables.  However, vec_t are non-copyable, so you
 * must create named temporaries of these structures.
 */

/**\brief A helper class for VEC_t
 */
struct vec_pair_t {
    CADDR_T        ptr;
    size_t         len;
};

/**\brief A base class for vec_t.
 */
struct VEC_t {
    int                _cnt;
    size_t             _size;
    vec_pair_t*        _base;        // pointer to beginning of _pair or malloced
                        // space
    vec_pair_t         _pair[MAX_SMALL_VEC_SIZE];
};

/**\brief A constant vec_t  (meaning things pointed to cannot be changed).
 */
class cvec_t : protected VEC_t {
    friend class vec_t; // so vec_t can look at VEC_t
protected:
    static        CADDR_T  zero_location; // see zvec_t, which is supposed
                                    // to be for the server-side only
public:
    enum dummy_enumid { max_small = MAX_SMALL_VEC_SIZE };
public:
    cvec_t() {
        _cnt = 0;
        _size = 0;
        _base = &_pair[0];
    }
    cvec_t(const cvec_t& v1, const cvec_t& v2) {
        _base= &_pair[0];
        set(v1, v2);
    }
    cvec_t(const void* p, size_t l) {
        _base = &_pair[0];
        set(p, l);
    }
    cvec_t(const cvec_t& v, size_t offset, size_t limit) {
        _base = &_pair[0];
        set(v, offset, limit);
    }
    ~cvec_t();

    void split(size_t l1, cvec_t& v1, cvec_t& v2) const;
    /// append {p,l} pairs from vector v, (first ptr is v + offset),
    ///  as needed to append at most nbytes
    cvec_t& put(const cvec_t& v, size_t offset, size_t nbytes);
    /// append { p, l } pair to this vector.
    cvec_t& put(const void* p, size_t l);
    /// append { p, l } pairs from v to this vector.
    cvec_t& put(const cvec_t& v);
    /** Add the key string WITH sign byte. */
    cvec_t& put(const w_keystr_t& keystr);
    /** Add the key string WITH sign byte. */
    cvec_t& put(const w_keystr_t& keystr, size_t offset, size_t nbytes);

    /// Clear this vector.
    cvec_t& reset()  {
        _cnt = _size = 0;
        return *this;
    }
    /// reset, then copy over all {p,l} pairs from v1 and v2
    cvec_t& set(const cvec_t& v1, const cvec_t& v2)  {
        return reset().put(v1).put(v2);
    }
    /// reset, then copy over all {p,l} pairs from v
    cvec_t& set(const cvec_t& v) {
        return reset().put(v);
    }

    /// reset, then install {p,l} pair
    cvec_t& set(const void* p, size_t l)  {
        return reset().put(p, l);
    }

    /// reset, then install {p,l} pairs as needed to capture limit
    /// bytes starting at v + offset
    cvec_t& set(const cvec_t& v, size_t offset, size_t limit)  {
        return reset().put(v, offset, limit);
    }


    /// returns # bytes this vector references
    size_t size() const        {
        return _size;
    }

    /** Write from vector to p, no more than \a limit bytes. */
    size_t copy_to(void* p, size_t limit = 0x7fffffff) const;

    /** Write from vector to std::string, no more than \a limit bytes. */
    size_t copy_to(std::basic_string<unsigned char> &buffer, size_t limit = 0x7fffffff) const;

    /**
     * Return value : 0 if equal; common_size not set
     * : <0 if this < v or v is longer than this, but equal in length of this.
     * : >0 if this > v or this is longer than v, but equal in length of v.
     */
    int cmp(const cvec_t& v, size_t* common_size = 0) const;
    int cmp(const void* s, size_t len) const;

    static int cmp(const cvec_t& v1,
               const cvec_t& v2, size_t* common_size = 0)  {
        return v1.cmp(v2, common_size);
    }

    /// return number of {p,l} pairs
    int count() const {return _cnt;}

    int  checksum() const;
    void calc_kvl(uint32_t& h) const;
    void init()         { _cnt = _size = 0; }  // re-initialize the vector
    // Creator of the vec has responsibility for delete[]ing anything that
    // was dynamically allocated in the array.  These are convenience methods
    // for holders of vec_ts that dynamically allocated all parts and want
    // them delete[]-ed.
    // vecdelparts() calls delete[] on all parts.
    // delparts() calls delete on all parts.
    // Both leave the vector re-initialized (0 parts)
    void vecdelparts()      {   while(_cnt-->0) {
                                   delete[] _base[_cnt].ptr;
                                   _base[_cnt].ptr = NULL;
                                   _base[_cnt].len = 0;
                                }
                                init();
                            }
    void delparts()         {   while(_cnt-->0) {
                                   delete _base[_cnt].ptr;
                                   _base[_cnt].ptr = NULL;
                                   _base[_cnt].len = 0;
                                }
                                init();
                            }

    bool is_pos_inf() const        { return this == &pos_inf; }
    bool is_neg_inf() const        { return this == &neg_inf; }
    bool is_null() const        { return size() == 0; }

    friend inline bool operator<(const cvec_t& v1, const cvec_t& v2);
    friend inline bool operator<=(const cvec_t& v1, const cvec_t& v2);
    friend inline bool operator>=(const cvec_t& v1, const cvec_t& v2);
    friend inline bool operator>(const cvec_t& v1, const cvec_t& v2);
    friend inline bool operator==(const cvec_t& v1, const cvec_t& v2);
    friend inline bool operator!=(const cvec_t& v1, const cvec_t& v2);

    friend ostream& operator<<(ostream&, const cvec_t& v);
    friend istream& operator>>(istream&, cvec_t& v);

    static cvec_t pos_inf;
    static cvec_t neg_inf;

private:
    // disabled
    cvec_t(const cvec_t& v);
    // determine if this is a large vector (one where extra space
    // had to be malloc'd
    bool _is_large() const {return _base != &_pair[0];}

    // determine max number of elements in the vector
    int  _max_cnt() const {
        return (int)(_is_large() ? _pair[0].len : (int)max_small);
    }
    // grow vector to have total_cnt elements
    void _grow(int total_cnt);

    // disabled
    //    cvec_t(const cvec_t& v);
    cvec_t& operator=(cvec_t);

    size_t recalc_size() const;
    bool   check_size() const;

public:
    bool is_zvec() const {
#if W_DEBUG_LEVEL > 2
        if(count()>0) {
            if(_pair[0].ptr == zero_location) {
                w_assert3(count() == 1);
            }
        }
#endif
        return (count()==0)
                ||
                (count() == 1 && _pair[0].ptr == zero_location);
    }
};

/**\brief  Vector: a set of {pointer,length} pairs for memory manipulation.
 *
 * This class is used throughout the storage manager and in its API
 * for copy-in and copy-out.
 */
class vec_t : public cvec_t {
public:
    /// Construct empty vector.
    vec_t() : cvec_t()        {};
    /// Construct a vector that combines two others.
    vec_t(const cvec_t& v1, const cvec_t& v2) : cvec_t(v1, v2)  {};
    /// Construct a vector from a memory location and a length.
    vec_t(const void* p, size_t l) : cvec_t(p, l)        {};
    /// Construct a vector from a memory location + offset and a length.
    vec_t(const vec_t& v, size_t offset, size_t limit)
        : cvec_t(v, offset, limit)        {};


    /**\brief Overwrites the data area to which the vector points.
     *
     * Scatter limit bytes of data from the location at p
     * into the locations identified by this vector.
     */
    const vec_t& copy_from(
        const void* p,
        size_t limit,
        size_t offset = 0) const;        // offset tells where
                                //in the vec to begin to copy

    /**\brief Overwrites the data area to which the vector points.
     *
     * Write data from the vector v
     * into the locations identified by this vector.
     */
    vec_t& copy_from(const cvec_t& v);

    /**\brief Overwrites the data area to which the vector points.
     *
     * Write data from the vector v, starting at the given offset
     * from the start of vector v,
     * into the locations identified by this vector.
     */
    vec_t& copy_from(
        const cvec_t& v,
        size_t offset,                // offset in v
        size_t limit,                // # bytes
        size_t myoffset = 0);        // offset in this

    /// Return the pointer from the {pointer, length} pair at the given index.
    CADDR_T       ptr(int index) const { return (index >= 0 && index < _cnt) ?
                                        _base[index].ptr : (CADDR_T) NULL; }
    /// Return the length from the {pointer, length} pair at the given index.
    size_t        len(int index) const { return (index >= 0 && index < _cnt) ?
                                        _base[index].len : 0; }

    /**\cond skip */
    /// Lets you reformat the vector into "result" with maximum-sized
    // chunks.
    void mkchunk( int maxsize, // max size of result vec
                int skip,                 // # skipped in *this
                vec_t        &result      // provided by the caller
    ) const;
    /**\endcond skip */

    /// A constant vector representing infinity. Used for key-value pairs, scans.
    static vec_t& pos_inf;
    /// A constant vector representing negative infinity. Used for key-value pairs, scans.
    static vec_t& neg_inf;

 private:
    // disabled
    vec_t(const vec_t&) : cvec_t()  {
      cerr << "vec_t: disabled member called" << endl;
      cerr << "failed at \"" << __FILE__ << ":" << __LINE__ << "\"" << endl;
      W_FATAL (fcINTERNAL);
    }
 private:
    // disabled
    vec_t& operator=(vec_t);

};

inline bool operator<(const cvec_t& v1, const cvec_t& v2)
{
    return v1.cmp(v2) < 0;
}

inline bool operator<=(const cvec_t& v1, const cvec_t& v2)
{
    return v1.cmp(v2) <= 0;
}

inline bool operator>=(const cvec_t& v1, const cvec_t& v2)
{
    return v1.cmp(v2) >= 0;
}

inline bool operator>(const cvec_t& v1, const cvec_t& v2)
{
    return v1.cmp(v2) > 0;
}

inline bool operator==(const cvec_t& v1, const cvec_t& v2)
{
    return (&v1==&v2) || v1.cmp(v2) == 0;
}

inline bool operator!=(const cvec_t& v1, const cvec_t& v2)
{
    return ! (v1 == v2);
}


/*<std-footer incl-file-exclusion='VEC_T_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
