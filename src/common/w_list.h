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

/*<std-header orig-src='shore' incl-file-exclusion='W_LIST_H'>

 $Id: w_list.h,v 1.56 2010/07/26 23:37:09 nhall Exp $

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

#ifndef W_LIST_H
#define W_LIST_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef W_BASE_H
#include <w_base.h>
#endif

#include <iostream>

/**\file w_list.h
 *
 * This file contains class definitions for linked lists of
 * various kinds.
 */

class w_list_base_t;
template <class T, class LOCK> class w_list_t;
template <class T, class LOCK> class w_list_i;
template <class T, class LOCK, class K> class w_hash_t;

/**\brief You can instantiate unsafe lists by using this type.
 * \details
 * This makes it relatively easy to grep for the unsafe lists in
 * the system.
 * \note There is no automagic enforcement of locking for w_list_t
 * w_hash_t or anything based on them.
 * Their templates just require a lock type so we can easily
 * find those that are locked in the code somewhere, somehow, and
 * those that are not.
 */
class unsafe_list_dummy_lock_t
{};
unsafe_list_dummy_lock_t* const unsafe_nolock=NULL; // instantiate with this 


/**\brief Link structure for membership in any class to be put on a w_list*
 *
 * Classes that will always be on one or more lists may contain a member
 * of type w_link_t. This contains the \b next and \b prev pointers
 * for putting the class instance into a doubly-linked list. 
 *
 * Embedding the w_link_t structures in the objects to be linked together
 * into a list avoids heap activity for list insertion and removal.
 */
class w_link_t {
public:
    NORET            w_link_t();
    NORET            ~w_link_t();
    NORET            w_link_t(const w_link_t&);
    w_link_t&             operator=(const w_link_t&);

    /// insert object containing this link into the list of
    /// which prev_link is a member. Insert after prev_link.
    void            attach(w_link_t* prev_link);
    void            check() {
                        w_assert9(_prev == this && _next == this); // not in a list
                    }
    /// remove containing object from the list.
    w_link_t*       detach();
    w_list_base_t*  member_of() const;

    w_link_t*       next() const;
    w_link_t*       prev() const;

private:
    w_list_base_t*        _list;
    w_link_t*            _next;
    w_link_t*            _prev;

    friend class w_list_base_t;
};

/**\brief Base class for various list classes.
 *
 */
class w_list_base_t : public w_vbase_t {
    
public:
    bool             is_empty() const;
    uint32_t          num_members() const;

    void             dump();
protected:
    NORET            w_list_base_t();
    NORET            w_list_base_t(uint32_t offset);
    NORET            ~w_list_base_t();

    void            set_offset(uint32_t offset);
    
    w_link_t            _tail;
    uint32_t            _cnt;
    uint32_t            _adj;

private:
    NORET            w_list_base_t(w_list_base_t&); // disabled
    w_list_base_t&        operator=(w_list_base_t&);
    
    friend class w_link_t;
};

inline NORET
w_link_t::w_link_t()
: _list(0) 
{
    _next = this;
    _prev = this;
    /* empty body */
}

inline NORET
w_link_t::~w_link_t()
{
    w_assert1(_next == this && _prev == this && _list == 0);
}

inline NORET
w_link_t::w_link_t(const w_link_t&)
: _list(0)
{
    _next = _prev = this;
}

inline w_link_t&
w_link_t::operator=(const w_link_t&)
{
    _list = 0;
    return *(_next = _prev = this);
}

inline w_list_base_t*
w_link_t::member_of() const
{
    return _list;
}

inline w_link_t*
w_link_t::prev() const
{
    return _prev;
}

inline w_link_t*
w_link_t::next() const
{
    return _next;
}

inline NORET
w_list_base_t::w_list_base_t()
    : _cnt(0), _adj(uint4_max)  // _adj must be set by a later call
                // to set_offset().  We init _adj
                // with a large number to detect
                // errors
{
    _tail._list = 0;
    w_assert9(_tail._next == &_tail && _tail._prev == &_tail);
}

inline NORET
w_list_base_t::w_list_base_t(uint32_t offset)
    : _cnt(0), _adj(offset)
{
    _tail._list = this;
    w_assert9(_tail._next == &_tail && _tail._prev == &_tail);
}

inline void 
w_list_base_t::set_offset(uint32_t offset)
{
    w_assert9(_cnt == 0 && _adj == uint4_max && _tail._list == 0);
    _tail._list = this;
    _adj = offset;
}

inline NORET
w_list_base_t::~w_list_base_t()
{
  _tail._list = 0;
    w_assert9(_cnt == 0);
}

inline bool
w_list_base_t::is_empty() const
{
    return _cnt == 0;
}

inline uint32_t
w_list_base_t::num_members() const
{
    return _cnt;
}

template <class T, class L> class w_list_t;

BIND_FRIEND_OPERATOR_PART_1(T, L, w_list_t<T,L>)
    
/**\brief Templated list of type T.
 *
 * \attention These lists are not thread-safe.
 * The user must provide the locks to access the lists
 * if thread-safety is necessary.
 * In order to identify the locations of all these, I am
 * adding a template type for the lock, and I'm going
 * to assert lock.is_mine, at least in debugging mode.
 */
template <class T, class LOCK>
class w_list_t : public w_list_base_t {
protected:
    /// return non-const ptr to link member of
    /// an object in this list
    w_link_t*             link_of(T* t) {
        w_assert3(t);
        return CAST(w_link_t*, CAST(char*,t)+_adj);
    }

    /// const version of the above
    const w_link_t*         link_of(const T* t) const {
        w_assert3(t);
        return CAST(w_link_t*, CAST(char*,t)+_adj);
    }

    /// return object of which the given link is a member
    T*                 base_of(w_link_t* p) const {
        w_assert3(p);
        return CAST(T*, CAST(char*, p) - _adj);
    }

    /// const version of the above
    const T*             base_of(const w_link_t* p) const {
        w_assert3(p);
        return CAST(T*, CAST(char*, p) - _adj);
    }

private:
    LOCK *lock;
public:

    /**\brief Linked list constructor.
     * \details
     * @param[in] link_offset Offset into type T of the w_link_t used
     * for this list.
     * @param[in] l Pointer to the lock that will protect this list.
     *
     * \note These lists are not locked; it is up to the client to 
     * provide synchronization for the list.  The LOCK template argument
     * is solely to make the list uses somewhat self-documenting.
     * By code perusal you might be able to tell what lock type and
     * what lock is meant to protect this list.
     *
     */
    NORET            w_list_t(uint32_t link_offset, LOCK *l)
    : w_list_base_t(link_offset), lock(l)
    {
#ifdef __GNUC__
#else
        w_assert2(link_offset + sizeof(w_link_t) <= sizeof(T));
#endif
    }

public:
    /// create a list sans offset.  Client must use set_offset later,
    ///before using the list.
    // This is used by w_hash_t  and the lock manager's list of lock heads
    NORET            w_list_t() : lock(NULL) {}

public:

    NORET            ~w_list_t() {}

    /// Tell the list where to find the offset of the w_link_t in 
    /// the item-type T.
    /// Lists constructed with the vacuous constructor \b must
    /// have this called before the list is used.
    void             set_offset(uint32_t link_offset) {
                        w_list_base_t::set_offset(link_offset);
    }

    /// For ordered lists, this uses the ordering. For unordered
    /// lists (simply w_list_t<T,LOCK>), it just inserts.
    virtual void        put_in_order(T* t)  {
        w_list_t<T,LOCK>::push(t);
    }

    // a set of consistent and intuitive names
    // TODO: replace throughout the SM
    w_list_t<T,LOCK>&	push_front(T* t) { return push(t); }
    w_list_t<T,LOCK>&	push_back (T* t) { return append(t); }
    T*	front() { return top(); }
    T*	back()  { return bottom(); }
    T*	pop_front() { return pop(); }
    T*	pop_back()  { return chop(); }
    
    /// Insert 
    w_list_t<T,LOCK>&   push(T* t)   {
        link_of(t)->attach(&_tail);
        return *this;
    }

    /// Insert at tail
    w_list_t<T,LOCK>&   append(T* t) {
        link_of(t)->attach(_tail.prev());
        return *this;
    }
 
    // Insert t after pos (for log buffer)
    void insert_after(T* t, T* pos) {
        link_of(t)->attach(link_of(pos));
    }

    // Insert t before pos (for log buffer)
    void insert_before(T* t, T* pos) {
        link_of(t)->attach(link_of(pos)->prev());
    }

    /// Remove
    T*                  pop()   {
        return _cnt ? base_of(_tail.next()->detach()) : 0;
    }

    /// Remove from rear
    T*                  chop()  {
        return _cnt ? base_of(_tail.prev()->detach()) : 0;
    }

    // Remove t (for log buffer)
    void remove(T* t) {
        link_of(t)->detach();
    }

    /// Get first but don't remove
    T*                  top()   {
        return _cnt ? base_of(_tail.next()) : 0;
    }

    /// Get last but don't remove
    T*                  bottom(){
        return _cnt ? base_of(_tail.prev()) : 0;
    }

    /// Get next
    T*                next(w_link_t* p) {
        w_assert1(p->member_of() == this);
        return base_of(p->next());
    }

    /// Get prev
    T*                prev(w_link_t* p) {
        w_assert1(p->member_of() == this);
        return base_of(p->prev());
    }

    // Get next (for log buffer)
    T*                next_of(T* t) {
        w_link_t *p = link_of(t);
        w_assert1(p->member_of() == this);
        if (p->next() != &_tail)
            return base_of(p->next());
        else
            return NULL;
    }

    // Get prev (for log buffer)
    T*                prev_of(T* t) {
        w_link_t *p = link_of(t);
        w_assert1(p->member_of() == this);
        if (p->prev() != &_tail)
            return base_of(p->prev());
        else
            return NULL;
    }

    // Get count
    uint32_t count() {
        return this->_cnt;
    }


    /// streams output
    friend ostream&        operator<< BIND_FRIEND_OPERATOR_PART_2(T, LOCK) (
        ostream&             o,
        const w_list_t<T,LOCK>&         l);

private:
    // disabled
    NORET                  w_list_t(const w_list_t<T,LOCK>&x) ;
    w_list_t<T,LOCK>&      operator=(const w_list_t<T,LOCK>&) ;
    
    friend class w_list_i<T, LOCK>;
};

/// Macro used to name the member of the object that is the link
#define    W_LIST_ARG(class,member)    w_offsetof(class,member)


/**\brief Iterator for a list.
 *
 * \attention This iterator is not thread-safe. It is up to the user to
 * provide thread-safety for the list.
 *
 * \attention Modifying the list while iterating over it: 
 * You can remove items from the list while iterating over it thus:
 * \code
 * while(iter.next()) {
 *    item = iter.curr();
 *    \< Now you can remove the item. \>
 * }
 * \endcode
 * Adding elements to the list while iterating yields undefined behavior.
 *
 * Example of use:
 * \code
 * w_list_t<sthread_t*> thread_list;
 * w_list_i<sthread_t*> i(thread_list);
 *
 * while(i.next()) {
 *    sthread_t *t = i.curr();
 *    if (t-> .....) ....
 * }
 * \endcode
 */
template <class T, class LOCK>
class w_list_i : public w_base_t {
public:
    /// Create a forward iterator. Since the list to be iterated
    /// isn't given, you must call reset() before you can use this
    ///iterator.
    NORET            w_list_i()
    : _list(0), _next(0), _curr(0), _backwards(false)        {};

    /// Create a forward or backward iterator iterator for the given
    /// list. Don't allow updating of the list while iterating.
    NORET            w_list_i(const w_list_t<T,LOCK>& l, bool backwards = false)
    :   _list(&l), _curr(0), _backwards(backwards) {
        _next = (backwards ? l._tail.prev() : l._tail.next());
    }

    virtual NORET    ~w_list_i()    {};

    /// Make an iterator usable (possibly again), for the given list,
    /// backward or forward.
    void            reset(const w_list_t<T, LOCK>& l, bool backwards = false)  {
        _list = &l;
        _curr = 0;
        _backwards = backwards;
        _next = (_backwards ? l._tail.prev() : l._tail.next());
    }
    
    /// Adjust the iterator to point to the next item in the list and return 
    /// a pointer to that next item.
    /// Returns NULL if there is no next item.
    /// Note that this depends on the results of the previous next() call,
    /// but what we do with curr() from the prior call is immaterial.
    T*                next()     
    {
        if (_next)  {
            _curr = (_next == &(_list->_tail)) ? 0 : _list->base_of(_next);
            _next = (_backwards ? _next->prev() : _next->next());
            // Note: once we ever had anything in the list, next() will
            // be non-null. We will return NULL when _curr hits the tail.
            // _next can be null only if we started with an empty list.
            w_assert1(_next != NULL);
        }
        return _curr;
    }

    /**\brief Return the current item in the list.
     * \details
     * Returns NULL if there is no current item.
     * There is no current item until next() is called at
     * least once, thus, one must call next() to get the first item.
     */
    T*                curr() const  {
        return _curr;
    }
    
protected: 
    const w_list_t<T, LOCK>*        _list;
private:
    w_link_t*           _next;
    T*                  _curr;
    bool                _backwards;

    // disabled
    NORET               w_list_i(w_list_i<T, LOCK>&) ;
    w_list_i<T, LOCK>&        operator=(w_list_i<T, LOCK>&) ;
};


/**\brief Const iterator for a list.
 *
 * A const version of w_list_i.  The pointers it
 * returns are const pointers to list members.
 */
template <class T, class LOCK>
class w_list_const_i : public w_list_i<T, LOCK> {
public:
    NORET            w_list_const_i()  {};
    NORET            w_list_const_i(const w_list_t<T,LOCK>& l)
                        : w_list_i<T,LOCK>(* (w_list_t<T,LOCK>*)(&l))    {};
    NORET            ~w_list_const_i() {};
    
    void            reset(const w_list_t<T,LOCK>& l) {
        w_list_i<T,LOCK>::reset(* (w_list_t<T,LOCK>*) (&l));
    }

    const T*            next() { return w_list_i<T,LOCK>::next(); }
    const T*            curr() const { 
        return w_list_i<T,LOCK>::curr(); 
    }
private:
    // disabled
    NORET            w_list_const_i(w_list_const_i<T,LOCK>&);
    w_list_const_i<T,LOCK>&        operator=(w_list_const_i<T,LOCK>&);
};

/**\brief Base class for sorted lists.
 *
 * T is the type of the objects going into the list.
 * K is the type of the key for sorting.
 */
template <class T, class LOCK, class K>
class w_keyed_list_t : public w_list_t<T,LOCK> {
public:
    T*                first() { return w_list_t<T,LOCK>::top(); }
    T*                last()  { return w_list_t<T,LOCK>::bottom(); }
    virtual T*            search(const K& k);

    NORET            w_keyed_list_t(LOCK *lock);
    NORET            w_keyed_list_t(
                        uint32_t        key_offset,
                        uint32_t        link_offset,
                        LOCK *                   lock 
                        );

    NORET            ~w_keyed_list_t()    {};

    void            set_offset(
                        uint32_t        key_offset,
                        uint32_t         link_offset);

protected:
    const K&            key_of(const T& t)  {
                        return * (K*) (((const char*)&t) + _key_offset);
                        }

    using w_list_t<T,LOCK>::_tail;
    using w_list_t<T,LOCK>::base_of;

private:
    // disabled
    NORET            w_keyed_list_t(const     w_keyed_list_t<T,LOCK,K>&);
    uint32_t        _key_offset;

    // disabled
    w_list_t<T,LOCK>&            push(T* t);
    w_list_t<T,LOCK>&            append(T* t) ;
    T*                      chop();
    T*                      top();
    T*                      bottom();
};

#define    W_KEYED_ARG(class,key,link)    \
    W_LIST_ARG(class,key), W_LIST_ARG(class,link) 

/**\brief List maintained in ascending order. 
 *
 * T is the type of the objects in the list.
 * K is the type of the key used for sorting.
 *   This type must have an operator <=.
 */
template <class T, class LOCK, class K>
class w_ascend_list_t : public w_keyed_list_t<T,LOCK, K>  {
    using w_keyed_list_t<T,LOCK, K>::_tail;
    using w_keyed_list_t<T,LOCK, K>::base_of;

public:
    NORET            w_ascend_list_t(
        uint32_t         key_offset,
        uint32_t        link_offset,
        LOCK *lock)
        : w_keyed_list_t<T,LOCK, K>(key_offset, link_offset, lock)   { };
    NORET            ~w_ascend_list_t()    {};

    virtual T*          search(const K& k);
    virtual void        put_in_order(T* t);

private:
    NORET               w_ascend_list_t(
                            const w_ascend_list_t<T,LOCK,K>&); // disabled
};

/**\brief List maintained in descending order. 
 *
 * T is the type of the objects in the list.
 * K is the type of the key used for sorting.
 *   This type must have an operator >=.
 */
template <class T, class LOCK, class K>
class w_descend_list_t : public w_keyed_list_t<T,LOCK, K> 
{
    using w_keyed_list_t<T,LOCK, K>::_tail;
    using w_keyed_list_t<T,LOCK, K>::base_of;

public:
    NORET            w_descend_list_t(
                        uint32_t         key_offset,
                        uint32_t        link_offset,
                        LOCK *l)
                        : w_keyed_list_t<T,LOCK, K>(
                                key_offset, link_offset, l)   { };
    NORET            ~w_descend_list_t()    {};

    virtual T*        search(const K& k);
    virtual void      put_in_order(T* t);

private:
    NORET            w_descend_list_t(
                            const w_descend_list_t<T,LOCK,K>&); // disabled
};



template <class T, class LOCK>
ostream&
operator<<(
    ostream&            o,
    const w_list_t<T,LOCK>&        l)
{
    const w_link_t* p = l._tail.next();

    cout << "cnt = " << l.num_members();

    while (p != &l._tail)  {
    const T* t = l.base_of(p);
    if (! (o << endl << '\t' << *t))  break;
    p = p->next();
    }
    return o;
}


template <class T, class LOCK, class K>
NORET
w_keyed_list_t<T,LOCK, K>::w_keyed_list_t(
    uint32_t        key_offset,
    uint32_t        link_offset,
    LOCK*                    lock
    )
    : w_list_t<T,LOCK>(link_offset, lock), _key_offset(key_offset)    
{
#ifdef __GNUC__
#else
    w_assert9(key_offset + sizeof(K) <= sizeof(T));
#endif
}

template <class T, class LOCK, class K>
NORET
w_keyed_list_t<T,LOCK, K>::w_keyed_list_t(LOCK *l)
    : w_list_t<T,LOCK>(0,l), _key_offset(0)
{
}

template <class T, class LOCK, class K>
void
w_keyed_list_t<T,LOCK, K>::set_offset(
    uint32_t        key_offset,
    uint32_t        link_offset) 
{
    w_assert3(_key_offset == 0);
    w_list_t<T,LOCK>::set_offset(link_offset);
    _key_offset = key_offset;
}

template <class T, class LOCK, class K>
T*
w_keyed_list_t<T,LOCK, K>::search(const K& k)
{
    w_link_t    *p;
    for (p = _tail.next();
     p != &_tail && (key_of(*base_of(p)) != k);
     p = p->next()) ;
    return (p && (p!=&_tail)) ? base_of(p) : 0;
}

template <class T, class LOCK, class K>
T*
w_ascend_list_t<T,LOCK, K>::search(const K& k)
{
    w_link_t    *p;
    for (p = _tail.next();
     p != &_tail && (this->key_of(*base_of(p)) < k);
     p = p->next()) ;

    return p ? base_of(p) : 0;
}

template <class T, class LOCK, class K>
void
w_ascend_list_t<T,LOCK, K>::put_in_order(T* t)
{
    w_link_t    *p;
    for (p = _tail.next();
     p != &_tail && (this->key_of(*base_of(p)) <= this->key_of(*t));
     p = p->next()) ;

    if (p)  {
    this->link_of(t)->attach(p->prev());
    } else {
        this->link_of(t)->attach(_tail.prev());
    }
}

template <class T, class LOCK, class K>
T*
w_descend_list_t<T,LOCK, K>::search(const K& k)
{
    w_link_t    *p;
    for (p = _tail.next();
     p != &_tail && (this->key_of(*base_of(p)) > k);
     p = p->next()) ;

    return p ? base_of(p) : 0;
}

template <class T, class LOCK, class K>
void
w_descend_list_t<T,LOCK, K>::put_in_order(T* t)
{
    w_link_t    *p;
    for (p = _tail.next();
     p != &_tail && (this->key_of(*base_of(p)) >= this->key_of(*t));
     p = p->next()) ;

    if (p)  {
        this->link_of(t)->attach(p->prev());
    } else {
        this->link_of(t)->attach(_tail.prev());
    }
}
/*<std-footer incl-file-exclusion='W_LIST_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
