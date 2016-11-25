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

/*<std-header orig-src='shore'>

 $Id: vec_t.cpp,v 1.76 2010/12/08 17:37:34 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define VEC_T_C
#include <cstdlib>
#include <cstring>
#include <w_stream.h>
#include <w_debug.h>
#include <w_base.h>
#include <w_minmax.h>
#include "basics.h"
#include "vec_t.h"
#include "w_key.h"
#include "umemcmp.h"


#ifdef EXPLICIT_TEMPLATE
// these templates are not used by vec_t, but are so common that
// we instantiate them here
template class w_auto_delete_array_t<cvec_t>;
template class w_auto_delete_array_t<vec_t>;
#endif


/**\cond skip */
cvec_t                cvec_t::pos_inf;
cvec_t                cvec_t::neg_inf;
CADDR_T               cvec_t::zero_location=(CADDR_T)&(cvec_t::neg_inf);
vec_t&                vec_t::pos_inf = *(vec_t*) &cvec_t::pos_inf;
vec_t&                vec_t::neg_inf = *(vec_t*) &cvec_t::neg_inf;

cvec_t::cvec_t(const cvec_t& /*v*/)
{
    cerr << "cvec_t: disabled member called" << endl;
    cerr << "failed at \"" << __FILE__ << ":" << __LINE__ 
         << "\"" << endl;
    abort();
}

cvec_t::~cvec_t()
{
    if (_is_large()) {
        free(_base);
    }
}

void cvec_t::split(size_t l1, cvec_t& v1, cvec_t& v2) const
{
    size_t min = 0;
    int i;
    for (i = 0; i < _cnt; i++)  {
        if (l1 > 0)  {
            min = (_base[i].len > l1) ? l1 : _base[i].len;
            v1.put(_base[i].ptr, min);
            l1 -= min; 
        }
        if (l1 <= 0) break;
    }
    
    for ( ; i < _cnt; i++)  {
        v2.put(_base[i].ptr + min, _base[i].len - min);
        min = 0;
    }
}

cvec_t& cvec_t::put(const cvec_t& v, size_t start, size_t num_bytes)
{
    int i;
    size_t start_byte, start_elem, total;

    if (v.size() < start+num_bytes) {
        w_assert3(v.size() >= start+num_bytes );
    }

    // find start in v
    for (i = 0, total = 0; i < v._cnt && total <= start; i++) {
        total += v._base[i].len;
    }
   
    // check for possible overflow
    if (_cnt+v._cnt > max_small) {
        _grow(_cnt+v._cnt);
    }

    start_elem = i-1;
    // first byte in the starting array element
    start_byte = start - (total - v._base[start_elem].len);

    // fill in the new vector
    _base[_cnt].ptr = v._base[start_elem].ptr+start_byte;
    _base[_cnt].len = v._base[start_elem].len-start_byte;
    _cnt++;
    w_assert3(_cnt <= _max_cnt()); 
    for (i = 1, total = _base[_cnt-1].len; total < num_bytes; i++) {
        _base[_cnt].ptr = v._base[start_elem+i].ptr;
        _base[_cnt].len = v._base[start_elem+i].len;
        total += _base[_cnt++].len;
        w_assert3(_cnt <= _max_cnt()); 
    }
    _base[_cnt - 1].len -= total-num_bytes;

    _size += num_bytes;
    w_assert3(check_size());
    return *this;
}

cvec_t& cvec_t::put(const void* p, size_t l)  
{
    if (_cnt+1 > max_small) {
        _grow(_cnt+1);
    }

    // to make zvecs work:
    _base[_cnt].ptr = (unsigned char*)p; 
    _base[_cnt].len = l;
    if(l>0) {
        _cnt++;
           w_assert3(_cnt <= _max_cnt()); 
        _size += l;
    }
    return *this;
}

bool cvec_t::check_size() const
{
    w_assert1(_size == recalc_size());
    return true;
}

size_t cvec_t::recalc_size() const 
{
    // w_assert1(! is_pos_inf() && ! is_neg_inf() );
    size_t l;
    int i;
    for (i = 0, l = 0; i < _cnt; l += _base[i++].len) ;
    return l;
}

// Scatter:
// write data from void *p into the area described by this vector.
// Copy no more than "limit" bytes. 
// Does NOT update the vector so caller must keep track
// of the limit to know what is good and what isn't.
// It is a const function because it does not update *this
// even though it does update that which *this describes.
const vec_t& vec_t::copy_from(
    const void* p,
    size_t limit,
    size_t offset)                 const// offset in the vector
{
    w_assert1(! is_pos_inf() && ! is_neg_inf() && !is_null() );
    w_assert1( _base[0].ptr != zero_location );

    char* s = (char*) p;
    for (int i = 0; (i < _cnt) && (limit>0); i++) {
        if ( offset < _base[i].len ) {
            size_t elemlen = ((_base[i].len - offset > limit)?
                                 limit : (_base[i].len - offset)) ;
            memcpy((char*)_base[i].ptr + offset, s, elemlen); 
            w_assert3(limit >= elemlen);
            limit -= elemlen;
            s += elemlen;
        } 
        if (_base[i].len > offset) {
            offset = 0;
        } else {
            offset -= _base[i].len;
        }
    }
    return *this;
}

// copies from vec to p;  returns # bytes copied
size_t cvec_t::copy_to(void* p, size_t limit) const 
{
    w_assert1(! is_pos_inf() && ! is_neg_inf() );
    char* s = (char*) p;
    for (int i = 0; i < _cnt && limit > 0; i++) {
        size_t n = limit > _base[i].len ? _base[i].len : limit;
        if(_base[i].ptr == zero_location) {
            memset(s, '\0', n);
        } else {
            memcpy(s, _base[i].ptr, n);
        }
        w_assert3(limit >= n);
        limit -= n;
        s += n;
    }
    return s - (char*)p;
}
size_t cvec_t::copy_to(std::basic_string<unsigned char> &buffer, size_t limit) const
{
    w_assert1(! is_pos_inf() && ! is_neg_inf() );
    buffer.clear();
    for (int i = 0; i < _cnt && limit > 0; i++) {
        size_t n = limit > _base[i].len ? _base[i].len : limit;
        if(_base[i].ptr == zero_location) {
            buffer.append('\0', n);
        } else {
            buffer.append(_base[i].ptr, n);
        }
        w_assert3(limit >= n);
        limit -= n;
    }
    return buffer.size();
}

// copies from s to this
vec_t& vec_t::copy_from(const cvec_t& s) 
{
    bool        zeroing=false;
    int         j = 0;
    char*         p = (char*) _base[j].ptr;
    size_t         l = _base[j].len;

    w_assert1(size() >= s.size());
    w_assert1(_base[0].ptr != zero_location);
    
    for (int i = 0; i < s._cnt; i++)  {
        const unsigned char* pp = s._base[i].ptr;
        if(pp == zero_location) zeroing = true;
        size_t left = s._base[i].len;
        while (l <= left && j < _cnt)  {
            if( zeroing) {
                memset(p, '\0', l);
            } else {
                memcpy(p, pp, l);
            }
            pp += l;
            left -= l;
            j++;
            if (j >= _cnt) break;  // out of while loop
            l = _base[j].len;
            p = (char*) _base[j].ptr;
        }
        if (left)  {
            if( zeroing) {
                memset(p, '\0', left);
            } else {
                memcpy(p, pp, left);
            }
            pp += left;
            w_assert3(l >= left);
            l -= left;
        }
    }
    return *this;
}

vec_t& vec_t::copy_from(const cvec_t& ss, size_t offset, size_t limit, size_t myoffset)
{
    bool        zeroing=false;
    vec_t s;
    s.put(ss, offset, limit);

    w_assert1(size() >= s.size());
    w_assert1(_base[0].ptr != zero_location);

    size_t ssz = s.size(), dsz = size();
    if (offset > ssz)                 offset = ssz;
    if (limit > ssz - offset)   limit = ssz - offset;
    if (myoffset > dsz)                offset = dsz;

    int j;
    for (j = 0; j < _cnt; j++)  {
        if (myoffset > _base[j].len)
            myoffset -= _base[j].len;
        else  
            break;
    }
    char* p = ((char*)_base[j].ptr) + myoffset;
    size_t l = _base[j].len - myoffset;

    w_assert1(dsz <= limit);

    size_t done;
    int i;
    for (i = 0, done = 0; i < s._cnt && done < limit; i++)  {
        const unsigned char* pp = s._base[i].ptr;
        if(pp == zero_location) zeroing = true;
        size_t left = s._base[i].len;
        if (limit - done < left)  left = limit - done;
        while (l < left)  {
            if(zeroing) {
                memset(p, '\0', l);
            } else {
                memcpy(p, pp, l);
            }
            done += l, pp += l;
            left -= l;
            j++;
            if (j >= _cnt) break;  // out of while loop
            l = _base[j].len;
            p = (char*) _base[j].ptr;
        }
        if (left)  {
            if(zeroing) {
                memset(p, '\0', left);
            } else {
                memcpy(p, pp, left);
            }
            pp += left;
            l -= left;
            done += left;
        }
    }
    return *this;
}

cvec_t& cvec_t::put(const cvec_t& v)
{
    w_assert1(! is_pos_inf() && ! is_neg_inf());
    if (_cnt+v._cnt > max_small) {
        _grow(_cnt+v._cnt);
    }
    for (int i = 0; i < v._cnt; i++)  {
        _base[_cnt + i].ptr = v._base[i].ptr;
        _base[_cnt + i].len = v._base[i].len;
    }
    _cnt += v._cnt;
    w_assert3(_cnt <= _max_cnt()); 
    _size += v._size;

    w_assert3(check_size());
    return *this;
}

int cvec_t::cmp(const void* s, size_t l) const
{
    if (is_pos_inf()) return 1;
    if (is_neg_inf()) return -1;
    if (is_null()) {
        if(l==0) return 0;
        // l > 0 means this < l
        return -1;
    }

    size_t acc = 0;
    for (int i = 0; i < _cnt && acc < l; i++)  {
        int d = umemcmp(_base[i].ptr, ((char*)s) + acc,
                       _base[i].len < l - acc ? _base[i].len : l - acc);
        if (d) return d;
        acc += _base[i].len;
    }
    return acc - l;        // longer wins
}

int cvec_t::cmp(const cvec_t& v, size_t* common_size) const
{
    // w_assert1(! (is_pos_inf() && v.is_pos_inf()));
    // w_assert1(! (is_neg_inf() && v.is_neg_inf()));

    // it is ok to compare +infinity with itself or -infinity with itself
    if (&v == this) {        // same address
        if (common_size) *common_size = v.size();
        return 0; 
    }

    // Common size is not set when one of operands is infinity or null
    if (is_null() && !v.is_null())  return -1;
    if (is_neg_inf() || v.is_pos_inf())  return -1;
    if (is_pos_inf() || v.is_neg_inf())  return 1;

        
    // Return value : 0 if equal; common_size not set
    //              : <0 if this < v
    //                 or v is longer than this, but equal in
    //                 length of this
    //              : >0 if this > v
    //                       or this is longer than v, but equal in 
    //                 length of v

    int result = 0;

    // l1, i1, j1 refer to this
    // l2, i2, j2 refer to v

    size_t l1 = size();
    size_t l2 = v.size();
    int i1 = 0;
    int i2 = 0;
    size_t j1 = 0, j2 = 0;
    
    while (l1 && l2)  {
        w_assert3(i1 < _cnt);
        w_assert3(i2 < v._cnt);
        size_t min = _base[i1].len - j1;
        if (v._base[i2].len - j2 < min)  min = v._base[i2].len - j2;

        w_assert3(min > 0);
        result = umemcmp(&_base[i1].ptr[j1], &v._base[i2].ptr[j2], min);
        if (result) break;
        
        if ((j1 += min) >= _base[i1].len)        { j1 = 0; ++i1; }
        if ((j2 += min) >= v._base[i2].len)        { j2 = 0; ++i2; }

        l1 -= min, l2 -= min;
    }

    if (result)  {
        if (common_size)  {
            while (_base[i1].ptr[j1] == v._base[i2].ptr[j2])  {
                ++j1, ++j2;
                --l1;
            }
            *common_size = (l1 < size() ? size() - l1 : 0);
        }
    } else {
        result = l1 - l2; // longer wins
        if (result && common_size)  {
            // common_size is min(size(), v.size());
            if (l1 == 0) {
                *common_size = size();
            } else {
                w_assert3(l2 == 0);
                *common_size = v.size();
            }
            w_assert3(*common_size == MIN(size(), v.size()));
        }
    }
    return result;
}

int cvec_t::checksum() const 
{
    int sum = 0;
    w_assert1(! is_pos_inf() && ! is_neg_inf());
    for (int i = 0; i < _cnt; i++) {
        for(size_t j=0; j<_base[i].len; j++) sum += ((char*)_base[i].ptr)[j];
    }
    return sum;
}

void cvec_t::calc_kvl(uint32_t& rh) const
{
    if (size() <= sizeof(uint32_t))  {
        rh = 0;
        copy_to(&rh, size());
    } else {
        uint32_t h = 0;
        for (int i = 0; i < _cnt; i++)  {
            const unsigned char* s = _base[i].ptr;
            for (size_t j = 0; j < _base[i].len; j++)  {
                if (h & 0xf8000000)  
                {
                    h = (h & ~0xf8000000) + (h >> 27);
                }
                h = (h << 5) + *s++;
            }
        }
        rh = h;
    }
}

void cvec_t::_grow(int total_cnt)
{
    w_assert3(total_cnt > max_small);
    w_assert3(_cnt <= _max_cnt()); 

    int prev_max = _max_cnt();
  
    if (total_cnt > prev_max) {
        // overflow will occur

        int grow_to = MAX(prev_max*2, total_cnt);
        vec_pair_t* tmp = NULL;

        if (_is_large()) {
            tmp = (vec_pair_t*) realloc((char*)_base, grow_to * sizeof(*_base));
            if (!tmp) W_FATAL(fcOUTOFMEMORY);
        } else {
            tmp = (vec_pair_t*) malloc(grow_to * sizeof(*_base));
            if (!tmp) W_FATAL(fcOUTOFMEMORY);
            for (int i = 0; i < prev_max; i++) {
                tmp[i] = _base[i];
            }
        }
        _pair[0].len = grow_to;
        _base = tmp;
    }
}

#include <cctype>

ostream& operator<<(ostream& o, const cvec_t& v)
{
    char        *p;
    u_char         c, oldc;
    int                repeated;
    int                nparts = v.count();
    int                i = 0;
    size_t        j = 0;
    size_t        l = 0;

    o << "{ ";
    for(i=0; i< nparts; i++) {
        // l = len(i);
        l = (i < v._cnt) ? v._base[i].len : 0;

        // p = (char *)v.ptr(i);
        p = (i < v._cnt) ? (char *)v._base[i].ptr : (char *)NULL; 

        o << "{" << l << " " << "\"" ;

        repeated=0;
        oldc = '\0';
        for(j=0; j<l; j++,p++) {
            c = *p;
            if(c == oldc) {
                repeated++;
            } else {
                if(repeated>0) {
                    o << "<" <<repeated << " times>";
                    repeated = 0;
                }
                if( c == '"' ) {
                    o << "\\\"" ;
                } else if( isprint(c) ) {
                    if( isascii(c) ) {
                        o << c ;
                    } else {
                        // high bit set: print its octal value
                        o << "\\0" << oct << c << dec ;
                    }
                } else if(c=='\0') {
                    o << "\\0" ;
                } else {
                    o << "\\0" << oct << (unsigned int)c << dec ;
                }
            }
            oldc = c;
        }
        if(repeated>0) {
            o << "<" <<repeated << " times>";
            repeated = 0;
        }
        o <<"\" }";
        w_assert3(j==l);
        w_assert3(j==v._base[i].len);
    }
    o <<"}";
    return o;
}

istream& operator>>(istream& is, cvec_t& v)
{
    char        c=' ';
    size_t      len=0;
    int         err = 0;
    char        *temp = 0;
    const char leftbracket='{';
    const char rightbracket='}';

    is.clear();
    v.reset();

    enum        {
        starting=0,
        getting_nparts, got_nparts,
        getting_pair, got_len,got_string,
        done
    } state;

    state = starting;
    while(state != done) {
        is >> ws; // swallow whitespace
        c = is.peek();
        /*
        cerr << __LINE__ << ":" 
            << "state=" << state
            << " peek=" << (char)is.peek() 
            << " pos=" << is.tellg()
            << " len=" << len
            << " err=" << err
            <<endl;
        */

        if(is.eof()) {
            err ++;
        } else {
            switch(state) {
            case done:
                break;

            case starting:
                if(c==leftbracket) {
                    is >> c;
                    state = getting_nparts;
                } else err ++;
                break;

            case getting_nparts:
                is >> ws; // swallow whitespace
                if(is.bad()) { err ++; }
                else state = got_nparts;
                break;

            case got_nparts:
                is >> ws; // swallow whitespace
                if(is.peek() == leftbracket) {
                    state = getting_pair;
                } else {
                    err ++;
                }
                break;

            case getting_pair:
                is >> ws; 
                is >> c;
                if( c == leftbracket ) {
                    is >> ws; // swallow whitespace
                    is >> len; // extract a len
                    if(is.bad()) { 
                        err ++;
                    } else state = got_len;
                } else if( c== rightbracket ) {
                    state = done;
                } else {
                    err ++;
                } 
                break;

            case got_len:
                if(c == '"') {
                    (void) is.get();

                    char *t;
                    // NOTE: delegates responsibility for
                    // delete[]-ing temp to the caller/holder of the vec
                    temp = new char[len];
                    size_t j = 0;
                    for(t=temp; j<len; j++, t++) {
                        is >> *t;
                    }
                    state = got_string;
                    c = is.peek();
                }
                if(c != '"') {
                    err++;
                } else {
                    (void) is.get();
                }
                break;

            case got_string:
                is >> c; // swallow 1 char
                if(c != rightbracket ) {
                    err ++;
                } else {
                    v.put(temp, len);
                    // NOTE: delegates responsibility for
                    // delete[]-ing temp to the caller/holder of the vec
                    temp = 0;
                    len = 0;
                    state = getting_pair;
                }
                break;

            }
            /*
            cerr << __LINE__ << ":" 
                << "state=" << state
                << " peek=" << (char)is.peek() 
                << " pos=" << is.tellg()
                << " len=" << len
                << " err=" << err
                <<endl;
            */
        }
        if(err >0) {
            is.setstate(ios::badbit);
            state = done;
            err = is.tellg();
            //cerr << "error at byte #" << err <<endl;
        }
    }
    return is;
}

cvec_t& cvec_t::put(const w_keystr_t& keystr) {
    return put (keystr.buffer_as_keystr(), keystr.get_length_as_keystr());
}
cvec_t& cvec_t::put(const w_keystr_t& keystr, size_t offset, size_t nbytes) {
    if (nbytes == 0) {
        return *this;
    }
    return put (((const unsigned char*)keystr.buffer_as_keystr()) + offset, nbytes);
}

// to eliminate dependency, this method is implemented in cvec_t.cpp
bool w_keystr_t::construct_from_vec(const cvec_t &vect) {
    clear();
    if (vect.is_neg_inf()) {
        return construct_neginfkey();
    }
    if (vect.is_pos_inf()) {
        return construct_posinfkey();
    }
    _data = new unsigned char[vect.size() + 1];
    if (_data == NULL) {
        return false;
    }
    _data[0] = SIGN_REGULAR;
    vect.copy_to(_data + 1, vect.size());
    _strlen = vect.size() + 1;
    return true;
}

bool w_keystr_t::copy_from_vec(const cvec_t &vect) {
    // Different from construct_from_vec
    // this function copies the data from cvec_t into w_keystr only,
    // it does not add the leading type byte
    clear();

    _data = new unsigned char[vect.size()];
    if (_data == NULL) {
        return false;
    }
    vect.copy_to(_data, vect.size());
    _strlen = vect.size();
    return true;
}

/////////////////////////////////////////////////////
//
// CS: THIS IS USED ONLY IN UNIT TESTS
//
// "result" is a vector with size() no larger than
// maxsize, whose contents are taken from the part
// of *this that's left after you skip the first
// "offset" bytes of *this.
// CALLER provides result, which is reset() right
// away, and re-used each time this is called.
//
// The general idea is that this allows you to take
// a vector of arbitrary configuration (in the context
// of writes, say) and break it up into vectors of
// "maxsize" sizes so that you can limit the sizes of
// writes.
/////////////////////////////////////////////////////
void
vec_t::mkchunk(
    int                maxsize,
    int                offset, // # skipped
    vec_t            &result // provided by the caller
) const
{
    int i;

    w_assert1( _base[0].ptr != zero_location );

    DBG(<<"offset " << offset << " in vector :");
    result.reset();

    // return a vector representing the next
    // maxsize bytes starting at the given offset
    // from the data represented by the input vector
    int        first_chunk=0, first_chunk_offset=0, first_chunk_len=0;
    {
        // find first_chunk
        int skipped=0, skipping;

        for(i=0; i<this->count(); i++) {
            skipping = this->len(i);
            if(skipped + skipping > offset) {
                // found
                first_chunk = i;
                first_chunk_offset = offset - skipped;
                first_chunk_len = skipping - first_chunk_offset;
                if(first_chunk_len > maxsize) {
                    first_chunk_len = maxsize;
                }

        DBG(<<"put " << W_ADDR(this->ptr(i)) <<
            "+" << first_chunk_offset << ", " << first_chunk_len);

                result.put((char*)this->ptr(i)+first_chunk_offset,first_chunk_len);
                break;
            }
            skipped += skipping;
        }
        if(first_chunk_len == 0) return;
    }

    if(first_chunk_len < maxsize) {
        // find next chunks up to the last
        int used, is_using ;

        used = first_chunk_len;
        for(i=first_chunk+1; i<this->count(); i++) {
            is_using = this->len(i);
            if(used + is_using <= maxsize) {
                // use the whole thing
                used += is_using;

                DBG(<<"put " << W_ADDR(this->ptr(i)) << ", " << is_using);
                result.put(this->ptr(i),is_using);
            } else {
                // gotta use part
                result.put(this->ptr(i),maxsize-used);
                used = maxsize;
                break;
            }
        }
    }
}

/**\endcond skip */
