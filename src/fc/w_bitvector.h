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

/*<std-header orig-src='shore' incl-file-exclusion='W_BASE_H'>

 $Id: w_bitvector.h,v 1.2 2010/05/26 01:20:23 nhall Exp $

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
#include <w.h>

/**\brief Templated bitmap for arbitrary size in bits
 *
 */
template<int BIT_COUNT>
class w_bitvector_t 
{
public:
    enum { BITS = BIT_COUNT };
    typedef uint64_t Word;
private:
    enum { BITS_PER_WORD=8*sizeof(Word) };
    enum { WORDS = (BITS+BITS_PER_WORD-1)/BITS_PER_WORD };
    uint64_t data[WORDS];

public:

    w_bitvector_t() { clear(); }

    /// return size in bits
    int num_bits() const { 
        return BIT_COUNT;
    }

    /// return size in words (unsigned long)
    int num_words() const { 
        int n= sizeof(data)/sizeof(data[0]); 
        w_assert1(n==WORDS);
        return n;
    }

    /** \brief OR-together and return merged vector.
     *
     * OR-together this bitmap with other bitmap and stuff result into
     * merged.
     * Return true if this entire bitmap is found in the other.
     */
    bool overlap(w_bitvector_t &merged, const w_bitvector_t &other)  const
    {
        return words_overlap(merged, other) == num_words();
    }

    /** \brief OR-together and return merged vector.
     *
     * OR-together this bitmap with other bitmap and stuff result into
     * merged.
     * Return the number of words in which this bitmap is found in
     * the other bitmap.  
     */
    int words_overlap(w_bitvector_t &merged, const w_bitvector_t &other)  const
    {
        const uint64_t *mine=&data[0];
        const uint64_t *theirs=&other.data[0];
        uint64_t *newvec=&merged.data[0];

        int matches=0;
        for(int i=0; i < num_words(); i++)
        {
            if (*mine == (*mine & *theirs)) matches++;
            *newvec = (*mine | *theirs);
            newvec++;
            mine++;
            theirs++;
        }
        return matches;
    }
    /**
     * Returns if all ON-bits in subset are also ON in this bitmap.
     */
    bool contains (const w_bitvector_t &subset) const
    {
        for(int i = 0; i < WORDS; ++i) {
            if ((data[i] & subset.data[i]) != subset.data[i]) {
                return false;
            }
        }
        return true;
    }
    /**
     * Updates this bitmap by taking OR with the given bitmap.
     */
    void merge (const w_bitvector_t &added)
    {
        for(int i = 0; i < WORDS; ++i) {
            data[i] |= added.data[i];
        }
    }

    ostream &print(ostream &o) const 
    {
        {
            const char *sep="";
            o << "{";
            for(int i=0; i < BITS; i++) 
            {
                if(is_set(i)) { o << sep << i; sep="."; }
            }
            o << "}";
        }
        /*
        {

            const char *sep="";
            o << "(~{";
            for(int i=0; i < BITS; i++) 
            {
                if( !is_set(i)) { o << sep << i; sep="."; }
            }
            o << "})";
        }
        */
        return o;
    }

    /// clear all bits
    void clear() {
        for(int i=0; i < WORDS; i++)
            data[i] = 0;
    }

    /// true if all bits are clear
    bool is_empty() const {
        Word hash = 0;
        for(int i=0; i < WORDS; i++)
            hash |= data[i];
        return hash == 0;
    }

    int num_bits_set() const {
        int j=0;
        for(int i=0; i < BITS; i++) 
        {
            if(is_set(i)) j++;
        }
        return j;
    }

    /// true if all bits are set
    bool is_full() const {
        return num_bits_set() == BIT_COUNT;
    }

    /// copy operator
    void copy(const w_bitvector_t &other)  {
        for(int i=0; i < WORDS; i++)
            data[i] = other.data[i];
    }

#define BIT_VECTOR_PROLOGUE(idx)        \
    w_assert1(idx < BITS);                \
    Word wdex = idx / BITS_PER_WORD;        \
    Word bdex = idx % BITS_PER_WORD
    
    /// Should use is_set()
    Word get_bit(Word idx) const {
        BIT_VECTOR_PROLOGUE(idx);
        return (data[wdex] >> bdex) & 0x1;
    }
    /// true if bit at index idx is set
    bool is_set(Word idx) const {
        return (get_bit(idx) == 0x1);
    }
    /// set bit at index idx 
    void set_bit(Word idx) {
        BIT_VECTOR_PROLOGUE(idx);
        data[wdex] |= (1ul << bdex);
    }
    /// clear bit at index idx 
    void clear_bit(Word idx) {
        BIT_VECTOR_PROLOGUE(idx);
        data[wdex] &= ~(1ul << bdex);
    }
#undef BIT_VECTOR_PROLOGUE
};

template <int BIT_COUNT> ostream &operator<<(ostream &o, const w_bitvector_t <BIT_COUNT> &t)
{
    const char *sep="";
    o << "{";
    for(int i=0; i < BIT_COUNT; i++) 
    {
        if(t.is_set(i)) { o << sep << i; sep="."; }
    }
    o << "}";
    return o;
}
