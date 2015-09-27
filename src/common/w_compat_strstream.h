/*
 * This stl "compatability" strstream implementation is
 * included with shore for use with newer c++ compilers whic
 * do not provide the strstream functionality.
 *
 * stringstreams are not usable for the functions shore needs,
 * since they provide no way of writing to and reading from
 * memory objects.
 *
 * This file should not be changed, except to incorporate bug
 * fixes from the SGI STL code.
 */

/*
 * Copyright (c) 1998
 * Silicon Graphics Computer Systems, Inc.
 *
 * Permission to use, copy, modify, distribute and sell this software
 * and its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appear in all copies and
 * that both that copyright notice and this permission notice appear
 * in supporting documentation.  Silicon Graphics makes no
 * representations about the suitability of this software for any
 * purpose.  It is provided "as is" without express or implied warranty.
 */ 

// WARNING: The classes defined in this header are DEPRECATED.  This
// header is defined in section D.7.1 of the C++ standard, and it
// MAY BE REMOVED in a future standard revision.  You should use the
// header <sstream> instead.


#ifndef __SGI_STL_STRSTREAM_W_COMPAT_
#define __SGI_STL_STRSTREAM_W_COMPAT_

#if defined(__sgi) && !defined(__GNUC__) && !defined(_STANDARD_C_PLUS_PLUS)
#error This header file requires the -LANG:std option
#endif

#include <istream>              // Includes <ostream>, <ios>, <iosfwd>
#include <streambuf>
#include <ostream>
#include <iostream>
#include <ios>

/** \brief  A namespace for implementations of old-style streambufs.
 */
namespace shore_compat {

using namespace std;


/**\brief Streambuf class that manages an array of char.
 *
 * Note that this class is not a template.
 */
class strstreambuf : public basic_streambuf<char, char_traits<char> >
{
public:                         // Types.
  typedef char_traits<char>              _Traits;
  typedef basic_streambuf<char, _Traits> _Base;

public:                         // Constructor, destructor
  explicit strstreambuf(streamsize __initial_capacity = 0);
  strstreambuf(void* (*__alloc)(size_t), void (*__free)(void*));

  strstreambuf(char* __get, streamsize __n, char* __put = 0);
  strstreambuf(signed char* __get, streamsize __n, signed char* __put = 0);
  strstreambuf(unsigned char* __get, streamsize __n, unsigned char* __put=0);

  strstreambuf(const char* __get, streamsize __n);
  strstreambuf(const signed char* __get, streamsize __n);
  strstreambuf(const unsigned char* __get, streamsize __n);

  virtual ~strstreambuf();

public:                         // strstreambuf operations.
  char* str();
  int pcount() const;

protected:                      // Overridden virtual member functions.
  virtual int_type overflow(int_type __c  = _Traits::eof());
  virtual int_type pbackfail(int_type __c = _Traits::eof());
  virtual int_type underflow();
  virtual _Base* setbuf(char* __buf, streamsize __n);
  virtual pos_type seekoff(off_type __off, ios_base::seekdir __dir,
                           ios_base::openmode __mode 
                                      = ios_base::in | ios_base::out);
  virtual pos_type seekpos(pos_type __pos, ios_base::openmode __mode 
                                      = ios_base::in | ios_base::out);

private:                        // Helper functions.
  // Dynamic allocation, possibly using _M_alloc_fun and _M_free_fun.
  char* _M_alloc(size_t);
  void  _M_free(char*);

  // Helper function used in constructors.
  void _M_setup(char* __get, char* __put, streamsize __n);

private:                        // Data members.
  void* (*_M_alloc_fun)(size_t);
  void  (*_M_free_fun)(void*);

  bool _M_dynamic  : 1;
  bool _M_constant : 1;
};

/// Class istrstream, an istream that manages a strstreambuf.
class istrstream : public std::basic_istream<char, std::char_traits<char> >
{
public:
  explicit istrstream(char*);
  explicit istrstream(const char*);
  istrstream(char* , streamsize);
  istrstream(const char*, streamsize);
  virtual ~istrstream();
  
  strstreambuf* rdbuf() const;
  char* str();

private:
  strstreambuf _M_buf;
};


/// Class ostrstream, an ostream that manages a strstreambuf.
class ostrstream : public std::basic_ostream<char, std::char_traits<char> >
{
public:
  ostrstream();
  ostrstream(char*, int, ios_base::openmode = ios_base::out);
  virtual ~ostrstream();

  strstreambuf* rdbuf() const;
  char* str();
  int pcount() const;

private:
  strstreambuf _M_buf;
};


} /* namespace shore compat */

#endif /* __SGI_STL_STRSTREAM_W_COMPAT */
