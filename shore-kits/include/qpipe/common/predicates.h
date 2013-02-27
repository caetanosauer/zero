// -*- mode:c++; c-basic-offset:4 -*-
#ifndef __QPIPE_PREDICATES_H
#define __QPIPE_PREDICATES_H

#include "util.h"
#include "qpipe/core/tuple.h"
#include <vector>
#include <algorithm>
#include <functional>
#include <string>
#include <cmath>
#include <cassert>

using std::vector;
using std::string;
using std::less;
using std::greater;
using std::less_equal;
using std::greater_equal;
using std::equal_to;
using std::not_equal_to;


ENTER_NAMESPACE(qpipe);



/**
 * @brief Composable predicate base class.
 */
struct predicate_t {
    virtual bool select(const tuple_t &tuple)=0;

    virtual predicate_t* clone() const=0;
    
    virtual ~predicate_t() { }
};



/**
 * @brief scalar predicate. Given a field type and offset in the
 * tuple, it extracts the field and tests it against the given
 * value. select returns the test result.
 *
 * V must have proper relational operators defined
 */
  
template <typename V, template<class> class T=equal_to>
class scalar_predicate_t : public predicate_t {
    V _value;
    size_t _offset;
public:
    scalar_predicate_t(V value, size_t offset)
        : _value(value), _offset(offset)
    {
    }
    virtual bool select(const tuple_t &tuple) {
        V* field = aligned_cast<V>(tuple.data + _offset);
        return T<V>()(*field, _value);
    }
    virtual scalar_predicate_t* clone() const {
        return new scalar_predicate_t(*this);
    }
};

/**
 * @brief string predicate. Given a field type and offset in the
 * tuple, it extracts the field and tests it against the given
 * value. select returns the test result.
 *
 * V must have proper relational operators defined
 */
  
template <template<class> class T=equal_to>
class string_predicate_t : public predicate_t {
    string _value;
    size_t _offset;
public:
    string_predicate_t(const string &value, size_t offset)
        : _value(value), _offset(offset)
    {
    }
    virtual bool select(const tuple_t &tuple) {
        const char* field = tuple.data + _offset;
        return T<int>()(strcmp(field, _value.c_str()), 0);
    }
    virtual string_predicate_t* clone() const {
        return new string_predicate_t(*this);
    }
};


/**
 * @brief string predicate that performs 'like' comparisons.
 */
template <bool INVERTED=false>
class like_predicate : public predicate_t {
    size_t _offset;
    
    // the first fragment in tests without a leading '%' (eg "abc%")
    string _bol;
    // the last fragment in tests without a trailing '%' (eg "%abc")
    string _eol;
    // the fragments surrounded by '%' on both sides
    typedef vector<string> fragment_list;
    fragment_list _fragments;
    void init(const string &value) {
        size_t beg = 0;
        size_t end = value.find('%');

        // bol fragment?
        if(beg != end)
            _bol = value.substr(beg, end);

        // inner fragments
        while(1) {
            beg = end;
            end = value.find('%', end+1);
            if(end == string::npos)
                break;
            
            _fragments.push_back(value.substr(beg, end));
        }

        // eol fragment?
        if((beg + 1) != value.size())
            _eol = value.substr(beg, value.size());
    }
public:
    like_predicate(const string &value, size_t offset)
        : _offset(offset)
    {
        assert(value.size());
        init(value);
    }
    virtual bool select(const tuple_t &tuple) {
        const char* field = tuple.data + _offset;
        const char* mark;

        // check bol fragment
        if(_bol.size()) {
            mark = strstr(field, _bol.c_str());
            if(mark != field)
                return INVERTED;
            
            // bol match
            field = mark + _bol.size();
        }

        // check inner fragments
        for(fragment_list::iterator it=_fragments.begin(); it != _fragments.end(); ++it) {
            mark = strstr(field, it->c_str());
            if(!mark)
                return INVERTED;
            
            field = mark + it->size();
        }
            
        // check eol fragment
        if(_eol.size()) {
            mark = strstr(field, _eol.c_str());
            // not found || not at the end (hit the '\0' terminator)
            if(!mark || mark[_eol.size()])
                return INVERTED;
        }

        // full match
        return !INVERTED;
    }
    virtual like_predicate* clone() const {
        return new like_predicate(*this);
    }
};



typedef like_predicate<false> like_predicate_t;
typedef like_predicate<true> not_like_predicate_t;



template <typename V, template<class> class T=equal_to>
class field_predicate_t : public predicate_t {
    size_t _offset1;
    size_t _offset2;
public:
    field_predicate_t(size_t offset1, size_t offset2)
        : _offset1(offset1), _offset2(offset2)
    {
    }
    virtual bool select(const tuple_t &tuple) {
        V* field1 = aligned_cast<V>(tuple.data + _offset1);
        V* field2 = aligned_cast<V>(tuple.data + _offset2);
        return T<V>()(*field1, *field2);
    }
    virtual field_predicate_t* clone() const {
        return new field_predicate_t(*this);
    }
};



/**
 * @brief Conjunctive predicate. Selects a tuple only if all of its
 * internal predicates pass.
 */
template<bool DISJUNCTION>
class compound_predicate_t : public predicate_t {
public:
    typedef std::vector<predicate_t*> predicate_list_t;

private:
    predicate_list_t _list;
    
    // if DISJUNCTION, returns true when the argument selects its
    // tuple; else returns true when the argument does not select its
    // tuple
    struct test_t {
        const tuple_t &_t;
        test_t(const tuple_t &t) : _t(t) { }
        bool operator()(predicate_t* p) {
            return p->select(_t) == DISJUNCTION;
        }
    };

    // clones its argument
    struct clone_t {
        predicate_t* operator()(predicate_t* p) {
            return p->clone();
        }
    };

    // deletes its argument
    struct delete_t {
        void operator()(predicate_t* p) {
            delete p;
        }
    };
    void clone_list() {
        // clone the predicates in the list
        std::transform(_list.begin(), _list.end(), _list.begin(), clone_t());
    }

public:
    void add(predicate_t* p) {
        _list.push_back(p);
    }
    virtual bool select(const tuple_t  &t) {
        // "search" for the answer
        predicate_list_t::iterator result;
        result = std::find_if(_list.begin(), _list.end(), test_t(t));
        
        // if disjunction, success means we didn't reach the end of
        // the list; else success means we did
        return DISJUNCTION? result != _list.end() : result == _list.end();
    }
    virtual compound_predicate_t* clone() const {
        return new compound_predicate_t(*this);
    }
    compound_predicate_t(const compound_predicate_t &other)
        : predicate_t(other), _list(other._list)
    {
        clone_list();
    }
    compound_predicate_t() { }
    compound_predicate_t &operator=(const compound_predicate_t &other) {
        _list = other._list;
        clone_list();
    }
    virtual ~compound_predicate_t() {
        std::for_each(_list.begin(), _list.end(), delete_t());
    }
};



typedef compound_predicate_t<false> and_predicate_t;
typedef compound_predicate_t<true> or_predicate_t;



/**
 * @brief Use a special wrapper class around randgen_t when we
 * generate predicates so we can control whether predicates are
 * generated at random or deterministically.
 */
class predicate_randgen_t {

    enum {
        INVALID,
        USE_THREAD_LOCAL,
        USE_CALLER
    } _type;
    randgen_t  _caller_randgen;
    randgen_t* _thread_local_randgen;


    /* Needed to construct empty object. */
    predicate_randgen_t ()
        : _type(INVALID),
          _caller_randgen(),
          _thread_local_randgen(NULL)
    {
    }
    
    predicate_randgen_t (randgen_t* randgen)
        : _type(USE_THREAD_LOCAL),
          _caller_randgen(),
          _thread_local_randgen(randgen)
    {
    }
    
    predicate_randgen_t (const char* caller_tag)
        : _type(USE_CALLER),
          _caller_randgen(RSHash(caller_tag, strlen(caller_tag))),
          _thread_local_randgen(NULL)
    {
    }
        
public:

    randgen_t* randgen() {
        switch (_type) {
        case USE_THREAD_LOCAL:
            assert(_thread_local_randgen != NULL);
            return _thread_local_randgen;
        case USE_CALLER:
            return &_caller_randgen;
        default:
            assert(0);
        }
    }

    int rand() {
        return randgen()->rand();
    }

    int rand(int n) {
        return randgen()->rand(n);
    }

    static predicate_randgen_t acquire(const char* caller_tag);
};



/**
 * @brief How to generate caller_tag? We probably want to use
 * __FUNCTION__, but just in case, wrap the caller_tag selection in a
 * macro...
 */
#define ACQUIRE_PREDICATE_RANDGEN(lval) \
   predicate_randgen_t lval=predicate_randgen_t::acquire(__FUNCTION__)



EXIT_NAMESPACE(qpipe);



#endif
