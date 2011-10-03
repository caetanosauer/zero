/*<std-header orig-src='shore'>

 $Id: w_input.cpp,v 1.15 2010/12/08 17:37:37 nhall Exp $

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

#include <w_base.h>
#include <cctype>
#include <iostream>

/**\cond skip */
enum states {   start, sgned, leadz,  
        new_hex, new_oct, new_dec,
        is_hex, is_oct, is_dec, 
        end, error, no_hex, no_state };

    // NB: these first 16 MUST have the values given
enum charclass { zero=0, one=1, two=2, three=3, four=4, 
        five=5, six=6, seven=7, eight=8, 
        nine=9, ten=10, eleven=11,
        twelve=12, thirteen=13, fourteen=14, fifteen=15,
        exx, JJJJ, eofile, white, sign, no_charclass
        };

static int equiv[127] = {
    /* 10 per line */
    /* 0 */
    JJJJ, JJJJ, JJJJ, JJJJ, JJJJ,     JJJJ, JJJJ, JJJJ, JJJJ, white, 
    /* 10 */
    JJJJ, JJJJ, JJJJ, JJJJ, JJJJ,     JJJJ, JJJJ, JJJJ, JJJJ, JJJJ, 
    /* 20 */
    JJJJ, JJJJ, JJJJ, JJJJ, JJJJ,     JJJJ, JJJJ, JJJJ, JJJJ, JJJJ, 
    /* 30 */
    JJJJ, JJJJ, white, JJJJ, JJJJ,    JJJJ, JJJJ, JJJJ, JJJJ, JJJJ, 
    /* 40 */
    JJJJ, JJJJ, JJJJ, sign, JJJJ,     sign, JJJJ, JJJJ, zero, one, 
    /* 50 */
    two , three, four, five, six,     seven, eight, nine, JJJJ, JJJJ,
    /* 60 */
    JJJJ, JJJJ, JJJJ, JJJJ, JJJJ,     ten, eleven, twelve, thirteen, fourteen, 
    /* 70 */
    fifteen, JJJJ, JJJJ, JJJJ, JJJJ,  JJJJ, JJJJ, JJJJ, JJJJ, JJJJ, 
    /* 80 */
    JJJJ, JJJJ, JJJJ, JJJJ, JJJJ,     JJJJ, JJJJ, JJJJ, exx, JJJJ, 
    /* 90 */
    JJJJ, JJJJ, JJJJ, JJJJ, JJJJ,     JJJJ, JJJJ, ten, eleven, twelve, 
    /* 100 */
    thirteen, fourteen, fifteen, JJJJ, JJJJ,   JJJJ, JJJJ, JJJJ, JJJJ, JJJJ, 
    /* 110 */
    JJJJ, JJJJ, JJJJ, JJJJ, JJJJ,     JJJJ, JJJJ, JJJJ, JJJJ, JJJJ, 
    /* 120 */
    exx, JJJJ, JJJJ, JJJJ, JJJJ,      JJJJ, JJJJ
};

typedef enum states XTABLE[no_charclass][no_state];
static enum states table_unknown[no_charclass][no_state] =
{
    /*  start, sgned,  leadz,  new_hex,new_oct,new_dec,is_hex, is_oct, is_dec, end */
/* zero */
    {  leadz, leadz,   leadz,  is_hex, is_oct, is_dec, is_hex, is_oct, is_dec, error },
/* c1_7 */
    {  new_dec,new_dec,new_oct,is_hex, is_oct, is_dec, is_hex, is_oct, is_dec, error},
    {  new_dec,new_dec,new_oct,is_hex, is_oct, is_dec, is_hex, is_oct, is_dec, error},
    {  new_dec,new_dec,new_oct,is_hex, is_oct, is_dec, is_hex, is_oct, is_dec, error},
    {  new_dec,new_dec,new_oct,is_hex, is_oct, is_dec, is_hex, is_oct, is_dec, error},
    {  new_dec,new_dec,new_oct,is_hex, is_oct, is_dec, is_hex, is_oct, is_dec, error},
    {  new_dec,new_dec,new_oct,is_hex, is_oct, is_dec, is_hex, is_oct, is_dec, error},
    {  new_dec,new_dec,new_oct,is_hex, is_oct, is_dec, is_hex, is_oct, is_dec, error},
/* c8_9 */
    {  new_dec,new_dec,new_dec,is_hex, end,    is_dec, is_hex, is_oct, is_dec,error },
    {  new_dec,new_dec,new_dec,is_hex, end,    is_dec, is_hex, is_oct, is_dec,error },
/* ca-f */
    {  error,error,new_hex,is_hex, end,    end,    is_hex, end,    end,   error },
    {  error,error,new_hex,is_hex, end,    end,    is_hex, end,    end,   error },
    {  error,error,new_hex,is_hex, end,    end,    is_hex, end,    end,   error },
    {  error,error,new_hex,is_hex, end,    end,    is_hex, end,    end,   error },
    {  error,error,new_hex,is_hex, end,    end,    is_hex, end,    end,   error },
    {  error,error,new_hex,is_hex, end,    end,    is_hex, end,    end,   error },
/* exx */
    {  error,  error,  new_hex,end,    end,    end,    end,    end,    end,   error },
/* JJJJ */
    {  error,  error,  end,    no_hex, end,    end,    end,    end,    end,   error },
/* EOF, eofile */
    {  error,  error,  end,    no_hex, end,    end,    end,    end,    end,   error },
/* white */
    {  start,  error,  end,    end,    end,    end,    end,    end,    end,   error },
/* sign */
    {  sgned,  error,  end,    no_hex, end,    end,    end,    end,    end,   error }, 
};

static enum states table_base16[no_charclass][no_state] =
{
    /*  start, sgned,  leadz, new_hex,new_oct,new_dec,is_hex, is_oct, is_dec, end */
/* zero */
    {  leadz, leadz,   leadz,  is_hex, error, error, is_hex, error, error, error },
/* c1_7 */
    {  is_hex,is_hex, is_hex, is_hex, error, error, is_hex, error, error,  error },
    {  is_hex,is_hex, is_hex, is_hex, error, error, is_hex, error, error,  error },
    {  is_hex,is_hex, is_hex, is_hex, error, error, is_hex, error, error,  error },
    {  is_hex,is_hex, is_hex, is_hex, error, error, is_hex, error, error,  error },
    {  is_hex,is_hex, is_hex, is_hex, error, error, is_hex, error, error,  error },
    {  is_hex,is_hex, is_hex, is_hex, error, error, is_hex, error, error,  error },
    {  is_hex,is_hex, is_hex, is_hex, error, error, is_hex, error, error,  error },
/* c8_9 */
    {  is_hex, is_hex, is_hex, is_hex, error,error, is_hex, error, error,  error },
    {  is_hex, is_hex, is_hex, is_hex, error,error, is_hex, error, error,  error },
/* ca-f */
    {  is_hex, is_hex, is_hex, is_hex, error,error, is_hex, error, error, error },
    {  is_hex, is_hex, is_hex, is_hex, error,error, is_hex, error, error, error },
    {  is_hex, is_hex, is_hex, is_hex, error,error, is_hex, error, error, error },
    {  is_hex, is_hex, is_hex, is_hex, error,error, is_hex, error, error, error },
    {  is_hex,    is_hex, is_hex, is_hex, error,error,is_hex, error, error, error },
    {  is_hex,    is_hex, is_hex, is_hex, error,error,is_hex, error, error, error },
/* exx */
    {  error,  error, new_hex, end,    error,error,  end,   error, error, error },
/* JJJJ */
    {  error,  error,  end,    no_hex, error,error,  end,   error, error, error }, 
/* EOF, eofile */
    {  error,  error,  end,    no_hex, error,error,  end,   error, error, error }, 
/* white */
    {  start,  error,  end,    no_hex, error,error,  end,   error, error, error }, 
/* sign */
    {  sgned,  error,  end,    no_hex, error,error,  end,   error, error, error }, 
};

static enum states table_base8[no_charclass][no_state] =
{
    /* start, sgned,  leadz,  new_hex, new_oct,new_dec, is_hex, is_oct,is_dec, end */
/* zero */
    {  leadz, leadz,  leadz,  error, error, error, error, is_oct, error, error },
/* c1_7 */
    {  is_oct,is_oct, is_oct, error, error, error, error, is_oct, error,error },
    {  is_oct,is_oct, is_oct, error, error, error, error, is_oct, error,error },
    {  is_oct,is_oct, is_oct, error, error, error, error, is_oct, error,error },
    {  is_oct,is_oct, is_oct, error, error, error, error, is_oct, error,error },
    {  is_oct,is_oct, is_oct, error, error, error, error, is_oct, error,error },
    {  is_oct,is_oct, is_oct, error, error, error, error, is_oct, error,error },
    {  is_oct,is_oct, is_oct, error, error, error, error, is_oct, error,error },
/* c8_9 */
    {  end,   error,  end,    error, error, error, error, end,   error,    error },
    {  end,   error,  end,    error, error, error, error, end,   error,    error },
/* ca-f */
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
/* exx */
    {  error, error,  new_hex,end,   error,error,  end,   error, error, error },
/* JJJJ */
    {  error, error, end,     error, error,error,  error, end,      error, error }, 
/* EOF, eofile */
    {  error, error, end,     error, error,error,  error, end,      error, error }, 
/* white */
    {  start, error, end,     error, error,error,  error, end,      error, error }, 
/* sign */
    {  sgned, error, end,     error, error,error,  error, end,      error, error }, 
};

static enum states table_base10[no_charclass][no_state] =
{
    /* start, sgned,  leadz,  new_hex,
                     new_oct,new_dec,
                          is_hex, is_oct,is_dec, end */
/* zero */
    {  leadz, leadz,  leadz,  error, error, error, error, is_oct, is_dec, error },
/* c1_7 */
    {  is_dec,is_dec, is_dec, error, error, error, error, error, is_dec,error },
    {  is_dec,is_dec, is_dec, error, error, error, error, error, is_dec,error },
    {  is_dec,is_dec, is_dec, error, error, error, error, error, is_dec,error },
    {  is_dec,is_dec, is_dec, error, error, error, error, error, is_dec,error },
    {  is_dec,is_dec, is_dec, error, error, error, error, error, is_dec,error },
    {  is_dec,is_dec, is_dec, error, error, error, error, error, is_dec,error },
    {  is_dec,is_dec, is_dec, error, error, error, error, error, is_dec,error },
/* c8_9 */
    {  is_dec,is_dec, end,    error, error, error, error, end,   is_dec,error },
    {  is_dec,is_dec, end,    error, error, error, error, end,   is_dec,error },
/* ca-f */
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
    {  end,   error,  end,    error, error,error,  error, end,   error, error },
/* exx */
    {  error, error, new_hex, end,   error,error,  end,   error, error, error },
/* JJJJ */
    {  error, error, end,     error, error,error,  error, error, end,   error }, 
/* EOF, eofile */
    {  error, error, end,     error, error,error,  error, error, end,   error }, 
/* white */
    {  start, error, end,     error, error,error,  error, error, end,   error }, 
/* sign */
    {  sgned, error, end,     error, error,error,  error, error, end,   error }, 
};


/* Seek breaks on non-seekable streams, but putback will always?
   work.  However NT will actually try to putback characters into
   a read-only string, which can cause problems. */
#ifdef __GNUG__
#define    IOS_BACK(stream,ch)    (void) stream.unget()
#else
#define    IOS_BACK(stream,ch)    (void) stream.seekg(-1, ios::cur)
#endif

#if defined( __GNUG__) 
#define    IOS_FAIL(stream)    stream.setstate(ios::failbit)
#else
#define    IOS_FAIL(stream)    stream.setstate(ios_base::failbit)
#endif

/* XXX shouldn't replicate this from somewhere else */
typedef ios::fmtflags  ios_fmtflags;

/*
 *  expect string [+-][0[x]][0-9a-fA-F]+
 */
#    define LONGLONGCONSTANT(i) i##LL
#    define ULONGLONGCONSTANT(i) i##ULL

/* These are masks */

uint64_t    thresh_hex_unsigned =  
    ULONGLONGCONSTANT(0xf000000000000000);
uint64_t    thresh_hex_signed =  
     LONGLONGCONSTANT(0xf800000000000000);
uint64_t    thresh_dec_unsigned =  
    ULONGLONGCONSTANT(1844674407370955161) ;
uint64_t    thresh_dec_signed =  
     LONGLONGCONSTANT(922337203685477580);
uint64_t    thresh2_dec_unsigned =  
    ULONGLONGCONSTANT(18446744073709551610) ;
uint64_t    thresh2_dec_signed =  
     LONGLONGCONSTANT(9223372036854775800);
uint64_t    thresh_oct_unsigned =  
    ULONGLONGCONSTANT(0xe000000000000000);
uint64_t    thresh_oct_signed =  
     LONGLONGCONSTANT(0xf000000000000000);

/*
 * This is *way* slower than strtoull and strtoll, as it is written.
 */

istream &
w_base_t::_scan_uint8(
    istream& i, 
    uint64_t &u8, 
    bool chew_white, // true if coming from istream operator
    bool is_signed, // if true, we have to return an error for overflow
    bool&    range_err // set to true if range error occurred  
) 
{
    uint64_t    thresh=0, 
    thresh2=0 /* thresh2, thresh3, thresh4 for decimal only */, 
        thresh3=0, thresh4=0;
    uint64_t     value = 0;

    bool     negate = false;
    int        e=0;
    int        base=0;
    bool     skip_white = true;
    states     s = start;
    streampos   tell_start = i.tellg();
    int        chewamt = chew_white? 1 : 0; 
    XTABLE     *table=0;

    range_err = false;
    {
    // Get the base from the stream
    ios_fmtflags old = i.flags();
    skip_white = ((old & ios::skipws) != 0);
    switch((int)(old & ios::basefield)) {
        case 0:
        base = 0;
        table = &table_unknown;
        break;

        case ios::hex:
        base = 4; // shift by this
        table = &table_base16;
        thresh = is_signed?  thresh_hex_signed : thresh_hex_unsigned;
        break;

        case ios::oct:
        base = 3; // shift by this
        table = &table_base8;
        thresh = is_signed?  thresh_oct_signed : thresh_oct_unsigned;
        break;

        case ios::dec:
        base = 10; // multiply by this
        table = &table_base10;
        thresh = is_signed?  thresh_dec_signed : thresh_dec_unsigned;
        thresh2 = is_signed?  thresh2_dec_signed : thresh2_dec_unsigned;
        thresh3 = is_signed?  (negate? 8: 7) : 5;
        thresh4 = is_signed? thresh_hex_signed : thresh_hex_unsigned;
        break;
        default:
        W_FATAL(fcINTERNAL);
        break;
    }
    }

    int ich;
    char ch;
    while (s < end) {
    ch = 0;
    // if (i) {
        ich = i.get();
        if (ich != EOF) {
           ch = char(ich);
           /* By using isspace() we get locale-dependent behavior */
           if(isspace(ch)) {
           e = white;
           } else {
           e = equiv[unsigned(ch)];
        }
        }
        else
           e = eofile;
    // } else {
        // e = eofile;
    // }

    /* transition table */
    s = (*table)[e][s];

    switch(s) {
        case start:
        /* Have seen leading white space */
        if(!skip_white) {
            s = end;
        }
        tell_start += chewamt; 
        break;

        case sgned:
        if(ch == '-') {
            negate = true;
            if(thresh3!=0) thresh3 = is_signed?  (negate? 8: 7) : 5;
        }
        break;

        case leadz:
        /* Have seen 1 or more leading zeroes 
         * if base is 0 (unstated), 0 or 0x will
         * determine the base.
         */
        break;

        case new_hex:
        /* State means we've seen [0][a-f] or 0[xX] */
        if(base && (base != 4)) {
            /* consider this the end of the string */
            IOS_BACK(i, ch);
            s = end;
            break;
        }
        w_assert9(base == 0 || base == 4);
        if((base == 0) && (e != exx)) {
            /* consider this the end of the string */
            IOS_BACK(i, ch);
            s = end;
            break;
        }
            /* at this point, in the 0[xX] case, 
         * we WILL make a conversion,
         * if nothing else, it will be to 0. In event
         * of error (the char after the [Xx] is not
         * a legit hex digit) we have to be able to 
         * seek back to where the [Xx] was, rather than
         * leave the endptr at the offending digit.
         */
        base = 4; // 2 ** base, i.e., shift amt
        if(e != exx) {
           IOS_BACK(i, ch);
        } else {
            /* XXX used to be tellg()-1, but no streampos
               arith allowed that way. */
            tell_start = i.tellg();
            tell_start -= 1;    // for possible error-handling
        }
        thresh = is_signed?  thresh_hex_signed : thresh_hex_unsigned;
        break;

        case new_oct:
        /* State means we've seen 0 followed by [1-7] */
        if(base==0 || base == 3) {
            /* treat as oct # */
            base = 3; // shift amt
            thresh = is_signed?  thresh_oct_signed : thresh_oct_unsigned;
        } else if(base == 10) {
            s = new_dec;
            thresh = is_signed? thresh_dec_signed : thresh_dec_unsigned;
            thresh2= is_signed?thresh2_dec_signed : thresh2_dec_unsigned;
            thresh3 = is_signed?  (negate? 8: 7) : 5;
            thresh4 = is_signed? thresh_hex_signed : thresh_hex_unsigned;
        } else {
            w_assert9(base == 4);
            s = new_hex;
            thresh = is_signed?  thresh_hex_signed : thresh_hex_unsigned;
        }
        IOS_BACK(i, ch);
        break;

        case new_dec:
        /* State means we've seen [1-9] in start/sgned state 
        *  or 0 followed by [8-9]
        */
        if(e == eight || e == nine) {
            if(base && base != 10) {
            /* consider this the end of the string */
            IOS_BACK(i, ch);
            s = end;
            break;
            }
        }
        if(base==0 || base == 10) {
            /* treat as dec # */
            base = 10; // multiply amt
            thresh = is_signed?  thresh_dec_signed : thresh_dec_unsigned;
            thresh2= is_signed?thresh2_dec_signed : thresh2_dec_unsigned;
            thresh3 = is_signed?  (negate? 8: 7) : 5;
            thresh4 = is_signed? thresh_hex_signed : thresh_hex_unsigned;
        } else if(base == 3) {
            s = new_oct;
            thresh = is_signed?  thresh_oct_signed : thresh_oct_unsigned;
        } else {
            w_assert9(base == 4);
            thresh = is_signed?  thresh_hex_signed : thresh_hex_unsigned;
            s = new_hex;
        }
        IOS_BACK(i, ch);
        break;

        case is_hex:
        w_assert9(base == 4);
        /* drop down */

        case is_oct:
        if(value & thresh) {
           range_err = true;
           // keep parsing
           // s = end;
           break;
        }
        /* shift */
        value <<= base;
        value += int(e);
        break;

        case is_dec:
        w_assert9(base == 10);
        if(value & thresh4) {
            if(value > thresh2) {
               /* will overflow on multiply */
               range_err = true;
               // keep parsing
               // s = end;
               break;
            } 
            value *= base;
            if((value - thresh2) + unsigned(e) > thresh3) {
            /* overflow adding in e */
               range_err = true;
               // keep parsing
               // s = end;
               break;
            }
        } else {
            /* multiply */
            value *= base;
        }
        value += unsigned(e);
        break;

        case error:
        IOS_FAIL(i);
        i.seekg(tell_start);
        s = end;
        break;

        case no_hex:
        i.seekg(tell_start);
        s = end;
        break;

        case end:
            IOS_BACK(i, ch);
        break;

        case no_state:
        W_FATAL(fcINTERNAL);
        break;
    }
    }
    if(range_err) {
       // don't seek to start
        u8 = negate ? 
       ( is_signed? w_base_t::int8_min : w_base_t::uint8_max) :
       ( is_signed? w_base_t::int8_max : w_base_t::uint8_max);
       IOS_FAIL(i);
    } else { 
    u8 = negate ?  (0 - value) : value;
    }

    return i;
}

/**\endcond skip */
