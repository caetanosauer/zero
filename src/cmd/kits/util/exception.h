// -*- mode:C++; c-basic-offset:4 -*-
#ifndef __EXCEPTION_H
#define __EXCEPTION_H

#include <exception>
#include <cstring>
#include <cerrno>
#include <cassert>
#include <cstdlib>
#include <cstdio>
#include <sstream>


/**
 * @brief Without VA_ARGS
 */
#define EXCEPTION(type) \
   type(__FILE__, __LINE__, __PRETTY_FUNCTION__)
#define EXCEPTION1(type, arg) \
   type(__FILE__, __LINE__, __PRETTY_FUNCTION__, std::string(arg))
#define EXCEPTION2(type, arg1, arg2) \
   type(__FILE__, __LINE__, __PRETTY_FUNCTION__, std::string(arg1, arg2))


#define THROW(type) \
   do { \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, ""); \
   } while (false)
#define THROW1(type, arg) \
   do { \
     fprintf(stderr, arg.c_str()); \
     fflush(stderr); \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, arg.c_str()); \
   } while (false)

#define THROW2(type, arg1, arg2) \
   do { \
     fprintf(stderr, arg1, arg2); \
     fflush(stderr); \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, std::string(arg1, arg2)); \
   } while (false)

// these two only used by cpu_set.cpp
#define THROW3(type, arg1, arg2, arg3) \
   do { \
     fprintf(stderr, arg1, arg2, arg3); \
     fflush(stderr); \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, std::string(arg1, arg2, arg3)); \
   } while (false)

#define THROW4(type, arg1, arg2, arg3, arg4) \
   do { \
     fprintf(stderr, arg1, arg2, arg3, arg4); \
     fflush(stderr); \
     assert(false); \
     throw type(__FILE__, __LINE__, __PRETTY_FUNCTION__, std::string(arg1, arg2, arg3, arg4)); \
   } while (false)

// tests an error code and throws the specified exception if nonzero
#define THROW_IF(Exception, err) \
   do { \
     assert(!err); \
     if(err) { \
        THROW1(Exception, errno_to_str(err)); \
     } \
   } while (false)


class ZappsException : public std::exception
{

private:

  std::string _message;

public:
  ZappsException(const char* filename, int line_num, const char* function_name,
                   std::string const &m) : exception()
  {
	  std::stringstream ss;
      ss << filename << ":" << line_num
            << "(" << function_name << "): "
            << m;
      _message = ss.str();
  }
  virtual const char* what() const throw()
  {
      return _message.data();
  }
};

class QPipeException : public ZappsException
{
public:
  QPipeException(const char* filename, int line_num, const char* function_name,
                 std::string const &m) : ZappsException(filename, line_num, function_name, m)
  {

  }
};




class ThreadException : public ZappsException {
public:
	ThreadException(const char* filename, int line_num, const char* function_name,
			std::string const &m) : ZappsException(filename, line_num, function_name, m)
	{
	}
};


#define DEFINE_EXCEPTION(Name) \
    class Name : public ZappsException { \
    public: \
        Name(const char* filename, int line_num, const char* function_name, \
             const char* m) \
            : ZappsException(filename, line_num, function_name, m) \
            { \
            } \
    }

inline std::string errno_to_str(int err=errno) {
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
