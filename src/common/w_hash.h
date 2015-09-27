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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='W_HASH_H'>

 $Id: w_hash.h,v 1.38 2010/10/27 17:04:22 nhall Exp $

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

#ifndef W_HASH_H
#define W_HASH_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\file w_hash.h
 */

#include <w_base.h>
#include <w_list.h>

template <class T, class LOCK, class K> class w_hash_t;
template <class T, class LOCK, class K> class w_hash_i;

BIND_FRIEND_OPERATOR_PART_1B(T,LOCK,K,w_hash_t<T,LOCK,K>)


/**\brief Templated hash table. Not particularly sophisticated.
 *
 * The hash function used here is :
 * - w_hash(key) & _mask
 *
 * Thus, to make this work, your key type needs to be a class containing
 * a public method 
 * uint32_t hash() const;
 * (This is somewhat inconvenient if you want the key to be an atomic type,
 * but it's a lot easier to find locate the hash functions this way and
 * we don't have to worry about any implicit construction of types by
 * the compiler this way.)
 *
 * Note that since the hash function uses the _mask to collect the lower
 * bits of the result of w_hash, the key.hash() function should be sensitive
 * to the way hash-table uses it, and the hash tables 
 * should be aware of the likely bit distribution
 * of the result of key.hash().
 *
 */
template <class T, class LOCK, class K>
class w_hash_t : public w_base_t {
public:
    /**\brief Construct hash table 
     * @param[in] sz Number of bits in result values. 
     * @param[in] key_offset Offset in object of type T where key K is found
     * @param[in] link_offset Offset in object of type T where w_link_t is found
     *            This w_link_t is used to hold the object in a hash table bucket
     * @param[in] lock Pointer to a lock used to protect the table.
     *
     * The size determines a mask used to restrict the resulting 
     * hash values to a desired size.
     *
     * The lock passed in is not used.  There is no enforcement of
     * locking these structures. The template contains the lock type
     * so that it is relatively easy to tell what structures are
     * unprotected by perusing the code.
     *
     * See \ref #W_HASH_ARG(class,key,link)
     */
    NORET                        w_hash_t(
        uint32_t                     sz,
        uint32_t                     key_offset,
        uint32_t                     link_offset,
        const LOCK *                lock);
    NORET                        ~w_hash_t();

private:
    uint32_t                     bucket_for(K const &k) const;
    uint32_t                     bucket_for(T const *t) const;

public:
    /// Insert an element in the table at the front of its bucket.
    w_hash_t&                   push(T* t);
    /// Insert an element in the table at the tail of its bucket.
    w_hash_t&                   append(T* t);
    /// Find an element in the table.
    T*                          lookup(const K& k) const;
    /// True if element is in the table.
    bool                        member(T const *t) const;
    /// Remove the (single) element with the given key 
    T*                          remove(const K& k);
    /// Remove the given element that is in the table.
    void                        remove(T* t);
    /// Total number of elements in the table.
    uint32_t                     num_members() const { return _cnt; }

    /// Standard ostream operator, despite the macro here (in \ref w_workaround.h)
    friend ostream&             operator<< 
                                     BIND_FRIEND_OPERATOR_PART_2B(T,LOCK,K) (
        ostream&                     o,
        const w_hash_t<T,LOCK,K>&        obj);
    

private:
    friend class w_hash_i<T,LOCK,K>;
    uint32_t                        _top;
    uint32_t                        _mask;
    uint32_t                        _cnt;
    uint32_t                        _key_offset;
    uint32_t                        _link_offset;
    w_list_t<T, LOCK>*             _tab;

    const K&                       _keyof(const T& t) const  {
        return * (K*) (((const char*)&t) + _key_offset);
    }
    w_link_t&                       _linkof(T& t) const  {
        return * (w_link_t*) (((char*)&t) + _link_offset);
    }

    // disabled
    NORET                           w_hash_t(const w_hash_t&)
    ;
    w_hash_t&                       operator=(const w_hash_t&)
    ;
};

// XXX They are the same for now, avoids offsetof duplication
/**\def W_HASH_ARG(class,key,link)
 * \brief Idiom for creating constructor argument for \ref #w_hash_t.
 *
 * This macro produces the last two arguments of the w_hash_t constructor.
 * Example :
 * \code
 * class key_t;
 * class entry_t {
 *    ...
 *    public:
 *       key_t    hashkey;
 *       w_link_t hashlink;
 * };
 *
 * w_hash_t<entry_t,key_t>(16, W_HASH_ARG(entry_t,hashkey,hashlink)) hashtable;
 * \endcode
 *
 */
#define        W_HASH_ARG(class,key,link)  W_KEYED_ARG(class, key, link)


/**\brief Iterate over hash table (for debugging)
 *
 * \note Not for general use.  Helper for w_hash_t,
 * and useful for writing debugging / dump-table code.
 *
 * Example:
 * \code
 * w_hash_t<entry_t,key_t>(16, W_HASH_ARG(entry_t,hashkey,hashlink)) hashtable;
 *
 * w_hash_i<entry_t,key_t> iter(hashtable);
 * entry_t *entry = NULL;
 * while( ( entry = iter.next()) != NULL) {
 *    ...
 * }
 * \endcode
 *
 * Since the w_hash_t is built of w_list_t, the same comments go for
 * next() and curr() here.  You can remove items from the table in
 * an iteration but you cannot insert.
 */
template <class T, class LOCK, class K>
class w_hash_i : public w_base_t {
public:
    NORET           w_hash_i(const w_hash_t<T,LOCK, K>& t) : _bkt(uint4_max), 
                                                    _htab(t) {};
        
    NORET           ~w_hash_i()        {};
    
    T*              next();
    T*              curr()                { return _iter.curr(); }

private:
    uint32_t                   _bkt;
    w_list_i<T,LOCK>         _iter;
    const w_hash_t<T,LOCK, K>&     _htab;
    
    NORET           w_hash_i(w_hash_i&);

    w_hash_i&       operator=(w_hash_i&)
    ;
};


template <class T, class LOCK, class K>
ostream& operator<<(
    ostream&                        o,
    const w_hash_t<T,LOCK, K>&        h)
{
    for (int i = 0; i < h._top; i++)  {
        o << '[' << i << "] ";
        w_list_i<T,LOCK> iter(h._tab[i]);
        while (iter.next())  {
            o << h._keyof(*iter.curr()) << " ";
        }
        o << endl;
    }
    return o;
}

/**\cond skip */
template <class T, class LOCK, class K>
NORET
w_hash_t<T,LOCK, K>::w_hash_t(
    uint32_t        sz,
    uint32_t        key_offset,
    uint32_t        link_offset,
    const LOCK *)
: _top(0), _cnt(0), _key_offset(key_offset),
  _link_offset(link_offset), _tab(0)
{
    for (_top = 1; _top < sz; _top <<= 1) ;
    _mask = _top - 1;
    
    w_assert1(!_tab); // just to check space
    _tab = new w_list_t<T,LOCK>[_top];
    w_assert1(_tab);
    for (unsigned i = 0; i < _top; i++)  {
        _tab[i].set_offset(_link_offset);
    }
}

template <class T, class LOCK, class K>
NORET
w_hash_t<T,LOCK, K>::~w_hash_t()
{
    w_assert1(_cnt == 0);
    delete[] _tab;
}
/**\endcond skip */

template<class T, class LOCK, class K>
uint32_t w_hash_t<T,LOCK, K>::bucket_for(T const* t) const {
    return bucket_for(_keyof(*t));
}

template<class T, class LOCK, class K>
uint32_t w_hash_t<T,LOCK, K>::bucket_for(K const &k) const {
    return k.hash() & _mask;
}

template <class T, class LOCK, class K>
w_hash_t<T,LOCK, K>&
w_hash_t<T,LOCK, K>::push(T* t)
{
    _tab[bucket_for(t)].push(t);
    ++_cnt;
    w_assert1(int(_cnt) > 0);
    return *this;
}

template <class T, class LOCK, class K>
w_hash_t<T,LOCK, K>& w_hash_t<T,LOCK, K>::append(T* t)
{
    _tab[bucket_for(t)].append(t);
    ++_cnt;
    w_assert1(int(_cnt) > 0);
    return *this;
}

template <class T, class LOCK, class K>
T*
w_hash_t<T,LOCK, K>::lookup(const K& k) const
{
    w_list_t<T,LOCK>& list = _tab[bucket_for(k)];
    w_list_i<T,LOCK> i( list );
    register T* t;
    int32_t count;
    for (count = 0; (t = i.next()) && ! (_keyof(*t) == k); ++count) ;
    /* FRJ: disable move-to-front because (a) it's expensive and (b)
       lists should be short and (c) read-only operations can't go in
       parallel if they insist on messing with the data structure
     */
    if (0 && t && count) {
        w_link_t& link = _linkof(*t);
        link.detach();
        list.push(t);
    }
        
    return t;
}
template <class T, class LOCK, class K>
bool
w_hash_t<T,LOCK, K>::member(T const* t) const
{
    w_list_base_t* list = &_tab[bucket_for(t)];
    return t->link.member_of() == list;
}

template <class T, class LOCK, class K>
T*
w_hash_t<T,LOCK, K>::remove(const K& k)
{
    w_list_i<T,LOCK> i(_tab[bucket_for(k)]);
    while (i.next() && ! (_keyof(*i.curr()) == k)) ;

    T *tmp = i.curr();
    if (tmp) {
        --_cnt;
        w_assert1(int(_cnt) >= 0);
        _linkof(*tmp).detach();
    }
    return tmp;
}

template <class T, class LOCK, class K>
void
w_hash_t<T,LOCK, K>::remove(T* t)
{
    w_assert1(_linkof(*t).member_of() ==
              &_tab[bucket_for(t)]);
    _linkof(*t).detach();
    --_cnt;
    w_assert1(int(_cnt) >= 0);
}

template <class T, class LOCK, class K>
T* w_hash_i<T,LOCK, K>::next()
{
    if (_bkt == uint4_max)  {
        _bkt = 0;
        _iter.reset(_htab._tab[_bkt++]);
    }

    if (! _iter.next())  {
        while (_bkt < _htab._top)  {
            
            _iter.reset( _htab._tab[ _bkt++ ] );
            if (_iter.next())  break;
        }
    }
    return _iter.curr();
}

/*<std-footer incl-file-exclusion='W_HASH_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
