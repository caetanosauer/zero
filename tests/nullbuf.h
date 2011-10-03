#ifndef TEST_NULLBUF_H
#define TEST_NULLBUF_H

// dummy stream object to ignore outputs.
// used to partially suppress verbose outputs in testcases.
#include <iostream>

template<typename Ch, typename Traits = std::char_traits<Ch> >
struct basic_nullbuf : std::basic_streambuf<Ch, Traits> {
     typedef std::basic_streambuf<Ch, Traits> base_type;
     typedef typename base_type::int_type int_type;
     typedef typename base_type::traits_type traits_type;

     virtual int_type overflow(int_type c) {
         return traits_type::not_eof(c);
     }
};

// convenient typedefs
typedef basic_nullbuf<char> nullbuf;
typedef basic_nullbuf<wchar_t> wnullbuf;


#endif // TEST_NULLBUF_H
