// -*- mode:C++; c-basic-offset:4 -*-
#ifndef __EXCEPTION_H
#define __EXCEPTION_H

#include <exception>
#include <cstring>
#include <cerrno>
#include <cassert>
#include <cstdlib>
#include <cstdio>

#include "util/c_str.h"



/**
 * @brief Using VA_ARGS...
 */
#if 0
#define EXCEPTION(type,__VA_ARGS__) \
   type(__FILE__, __LINE__, __PRETTY_FUNCTION__, c_str(__VA_ARGS__))

#define THROW(type, __VA_ARGS__) \
      throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, c_str(__VA_ARGS__)

// tests an error code and throws the specified exception if nonzero
#define THROW_IF(Exception, err) \
    do { if(err) throw EXCEPTION(Exception, errno_to_str(err)); } while (0)

#else



/**
 * @brief Without VA_ARGS
 */
#define EXCEPTION(type) \
   type(__FILE__, __LINE__, __PRETTY_FUNCTION__)
#define EXCEPTION1(type, arg) \
   type(__FILE__, __LINE__, __PRETTY_FUNCTION__, c_str(arg))
#define EXCEPTION2(type, arg1, arg2) \
   type(__FILE__, __LINE__, __PRETTY_FUNCTION__, c_str(arg1, arg2))


#define THROW(type) \
   do { \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, ""); \
   } while (false)
#define THROW1(type, arg) \
   do { \
     fprintf(stderr, arg); \
     fflush(stderr); \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, c_str(arg)); \
   } while (false)

#define THROW2(type, arg1, arg2) \
   do { \
     fprintf(stderr, arg1, arg2); \
     fflush(stderr); \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, c_str(arg1, arg2)); \
   } while (false)

// these two only used by cpu_set.cpp
#define THROW3(type, arg1, arg2, arg3) \
   do { \
     fprintf(stderr, arg1, arg2, arg3); \
     fflush(stderr); \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, c_str(arg1, arg2, arg3)); \
   } while (false)

#define THROW4(type, arg1, arg2, arg3, arg4) \
   do { \
     fprintf(stderr, arg1, arg2, arg3, arg4); \
     fflush(stderr); \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, c_str(arg1, arg2, arg3, arg4)); \
   } while (false)

// tests an error code and throws the specified exception if nonzero
#define THROW_IF(Exception, err) \
   do { \
     assert(!err); \
     if(err) { \
        THROW1(Exception, errno_to_str(err)); \
     } \
   } while (false)

#endif



// optional printf-like message allowed here
class QPipeException : public std::exception
{

private:

  c_str _message;

public:

  QPipeException(const char* filename, int line_num, const char* function_name,
                 c_str const &m)
    : _message(c_str("%s:%d (%s):%s", filename, line_num, function_name, m.data()))
  {
  }

  virtual const char* what() const throw() {
    return _message.data();
  }
 
  virtual ~QPipeException() throw() { }
};

#define DEFINE_EXCEPTION(Name) \
    class Name : public QPipeException { \
    public: \
        Name(const char* filename, int line_num, const char* function_name, \
             const char* m) \
            : QPipeException(filename, line_num, function_name, m) \
            { \
            } \
    }

inline c_str errno_to_str(int err=errno) {
    return strerror(err);
}

DEFINE_EXCEPTION(Unreachable);
DEFINE_EXCEPTION(BadAlloc);
DEFINE_EXCEPTION(OutOfRange);
DEFINE_EXCEPTION(FileException);
DEFINE_EXCEPTION(BdbException);

#ifdef __GCC
inline void unreachable() ATTRIBUTE(noreturn);
inline void unreachable() {
    assert(false);
    exit(-1);
}
#else
#define unreachable() THROW(Unreachable)
#endif

#endif
