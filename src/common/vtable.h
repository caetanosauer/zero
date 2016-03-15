/*<std-header orig-src='shore' incl-file-exclusion='VTABLE_INFO_H'>

 $Id: vtable.h,v 1.4 2010/07/07 20:50:12 nhall Exp $

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

#ifndef VTABLE_INFO_H
#define VTABLE_INFO_H

#include "w_defines.h"
#include "w_base.h"

/*  -- do not edit anything above this line --   </std-header>*/

/* skip documentation for this for now. */

#include <cstring>

/**\brief Structure for converting arbitrary info to a row of a virtual table.  
 *
 * \details
 * A datum in a virtual table is an 64-byte string.
 * The strings usually take the form "name value".
 * A vtable_row_t is an array of these data, and represents a row in
 * a virtual table.
 * It is a structure "imposed" on an already-allocated block of data. 
 * This struct
 * does NOT allocate or deallocate anything.
 *
 * Each "attribute" or entry of the row is identified by an integer.
 */
class vtable_row_t {
private:
    int            N;
    int            M; // max value size
    int            _in_use;
    char           *_list;
    char           *_list_end;
    char           *_entry[1];

public:
    // Must construct a row with at least 1 attribute.
    NORET vtable_row_t() : N(0), M(0), _in_use(0), _list(NULL),
                                _list_end(NULL) { dump("construct"); }
    NORET ~vtable_row_t() { }

    /// Number of attributes in the row.
    int   quant() const { return _in_use; }

    // vtable_t needs to make sure this doesn't exceed the space
    // it allocated.
    char *end() const { return _list_end; }
    int  size_in_bytes() const { return _list_end - (char *)this; }

    /// Return # bytes user must allocate to accommodate n attributes of
    /// m bytes each plus whatever overhead we have in this structure.
    static int   bytes_required_for(int n, int m) {
        return 
            sizeof(vtable_row_t) 
            + (n-1) * sizeof(char * /*_entry[0]*/) 
            + (n * m);
    }
    int   value_size() const { return M; }

    /// Initialize a row for n attributes. Must be at least 1.
    /// Assumes we have enough space for this to happen.
    /// User of this row must have allocated enough space.
    /// Let the maximum value size be m.
    void init_for_attributes(int n, int m) 
    {
        w_assert0(n>0);
        N = n; 
        M = m; 

        // had better have been allocated by caller.
        memset(&_entry[0], '\0', n * m +
                + (n*sizeof(_entry[0])));

        _in_use=0;
        _list = (char *)&_entry[N]; // one past the last _entry
        _list_end = _list; // none in use
        _entry[0] = _list;

/*#if W_DEBUG_LEVEL > 3
        // had better have been initialized by caller
        for(int a=0; a<n; a++) {
            w_assert3(strlen(_get_const(a)) == 0); 
        }
#endif*/
        w_assert1(size_in_bytes() <= bytes_required_for(n,m));
    }

    /// realloc for larger number of attributes. This means we
    /// have to shift around the values. We don't allow change of
    /// maximum value size (M)
    void reinit_for_attributes(int n) 
    {
        int additional = n - N;
        if(additional > 0) {
            int bytes = sizeof(_entry[0]) * additional;

            // move the data. args: dest, src, amt
            memmove(&_list[additional], &_list[0], bytes);
            for(int i=N; i < n; i++) {
                _entry[i] = 0;
            }
            _list = &_list[bytes];
            _list_end = &_list_end[bytes];
            // _in_use doesn't change.
        }
    }

    /// Convert the unsigned int to a string datum at the location for \e a.
    void set_uint(int a, unsigned int v);
    /// Convert the int to a string datum at the location for \e a.
    void set_int(int a, int v);
    /// Convert the base_stat_t to a string datum at the location for \e a.
    void set_base(int a, w_base_t::base_stat_t v);
    /// Convert the base_float_t to a string datum at the location for \e a.
    void set_base(int a, w_base_t::base_float_t v);
    /// Copy the string to a string datum at the location for \e a.
    void set_string(int a, const char *v);

    /// Return whatever string is already written to the location for \e a.
    const char *operator[](int a) const {
        return _get_const(a);
    }


    ostream& operator<<(ostream &o);
    /// Return the number of entries/locations/attributes for this "row".
    int n() const { return N; }

    void dump(const char *msg)const;
private:
    vtable_row_t(const vtable_row_t&); // disabled 
    vtable_row_t& operator=(const vtable_row_t&); // disabled 
    /// From a given int a, return a pointer to the entry
    /// for that int so that we can stuff something into 
    /// that entry. NO ALIGNMENT!
    char *_insert_attribute(int a) {
        w_assert1(a < N);
        w_assert1(a >= 0);
        char * v = (char *)_entry[a];
        if(v==NULL) {
            v = (_entry[a] = _list_end);
        }
        return v;
    }

    const char *_get_const(int a) const {
        const char *x =  (const char *)_entry[a];
        w_assert1(x < _list_end);
        return x;
    }

    /// Notify the row that we inserted something in attribute #a.
    void _inserted(int a);
};

/**\brief Structure for converting lower layer info to a virtual table.  
 * \details
 * This structure holds a set of vtable_row_t, which is a set of attributes.
 * The space for the whole mess is allocated by the vtable_t, and the rows
 * must be populated strictly in sequence, and the attributes of the rows
 * likewise must be populated in sequence.  As a row is populated,
 * space for the attributes is peeled off the chunk allocated by the vtable.
 * The idea here is to create, without undue heap activity, a 
 * string-based (untyped)
 * virtual table containing various and sundry information.
 * The things that can be gathered and stuffed into virtual tables include:
 * Info about the threads, transactions.
 * A server could, if it so chose, convert this information into a
 * relational table.
 * Originally this was used to send across the wire, via RPC, this info
 * to a client in client/server SHORE. Now it has been stripped down.
 * Someday if this facility proves useful, we should make 
 * this collect typed information so that the attributes
 * can be int or double.  But for now, they are string representations of the
 * values.
 *
 * Test programs may use ss_m::xct_collect(vtable &) and kin,
 * and output the results with the operator<< for vtable_t.
 */
class vtable_t {
public:
    NORET vtable_t() : _rows(0), _rows_filled(0),
       _rowsize_attributes(0), _rowsize_bytes(0),
       _array_alias(NULL) {}

    /// Initialize table with \e R rows, with up to \e A attributes, 
    /// each with a maximum size of \e S bytes.
    int init(int R, int A, int S);

    NORET ~vtable_t() {
        delete[] _array_alias;
        _array_alias = NULL;
    }

    vtable_row_t& operator[] (int i) const {
        // called by updater so can't use this assertion
        // w_assert1(i < _rows_filled);
        return *_get_row(i);
    }

    ostream& operator<<(ostream &o) const;

    /// Return number of rows filled.
    int            quant() const { return _rows_filled; }
    int            size() const { return _rows; }
    void           filled_one();
    void           back_out(int n) {  _rows_filled -= n; }
    int            realloc();
private:
    vtable_t(const vtable_t&); // disabled
    vtable_t& operator=(const vtable_t&); // disabled

    vtable_row_t* _get_row(int i) const;

    int            _rows;
    int            _rows_filled;
    int            _rowsize_attributes; //#strings
    int            _rowsize_bytes;//#string * sizeof string
    char*          _array_alias;

};

/**\brief Template class for converting lower layer info to a virtual table.  
 * \details
 * This function is called for each entity that will be represented by
 * a row in a virtual table.
 * We construct it with an entire virtual table, which must have been
 * pre-allocated with the right number for rows, N. TODO FIX
 * 
 * Then we invoke it (call its operator ()) N times,  passing  in
 * each time a reference to the entity (of type T) we want it to "convert" to
 * a row.  
 * Each class T whose instances are to be represented in
 * a virtual table must have a method
 * \code
        T::vtable_collect(vtable_row_t &)
 * \endcode
 */
template <class T>
class vtable_func 
{
public: 
    NORET vtable_func(vtable_t &v): _curr(0), _array(v) { }
    NORET ~vtable_func() { }

    void insert_names() {
        T::vtable_collect_names(_array[_curr]);
        _array.filled_one();
        w_assert9(_curr < _array.size());
        _curr++;
    }
    /// Gather information about t and stuff it into row @ index _curr.
    /// It gathers by calling the T::vtable_collect(vtable_row_t &)
    void operator()(const T& t) // gather func
    {
        // escape the const-ness if possible
        T &t2 = (T &)t;
        t2.vtable_collect( _array[_curr] );

        // bump its counter
        _array.filled_one();
        w_assert9(_curr < _array.size());
        _curr++;
    }
    /// Un-collect the last \e n entries. 
    void        back_out(int n) {  
        _curr -= n; 
        _array.back_out(n);
    }

    int            realloc() {
        return _array.realloc();
    }

protected:
    int                  _curr;
    vtable_t&            _array; // uses vtable_t::operator[]
};


/**\cond skip */
class  vtable_names_init_t {
    size_t _size;
    int _argc;
    const char * const * _argv;
public:
    NORET vtable_names_init_t(int argc, const char *const argv[]) : _size(0),
        _argc(argc), _argv(argv)
    {
        // Let _size be the larger of sizeof(double) and the
        // the longest string name of the sthread vtable 
        // attributes.  The minimum being sizeof(double) is so that
        // we can ultimately have these attribute be typed rather than
        // all strings.
        int mx=0;
        for(int i=0; i < _argc; i++) {
             if(_argv[i]==NULL) break;
             int j = strlen(_argv[i]);
             if(j > mx) mx = j;
        }
        _size = size_t(mx)+1; // leave room for trailing null.
        if(_size < sizeof(double)) _size = sizeof(double);
    }
    const char *name(int i) const {
        return _argv[i];
    }
    void collect_names(vtable_row_t &t) {
        for(int i=0; i < _argc; i++)
        {
            if(_argv[i]) t.set_string(i, _argv[i]);
        }
    }
    int max_size() const { return int(_size); }
};
/**\endcond  skip */

/*<std-footer incl-file-exclusion='VTABLE_INFO_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
