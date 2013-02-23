/* -*- mode:C++; c-basic-offset:4 -*-
   Shore-kits -- Benchmark implementations for Shore-MT
   
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

/** @file fileops.cpp
 *
 *  @brief Implements common file checking operations. All exported
 *  functions return 0 on success (if the file exists or is
 *  accessible, etc).
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @bug fileops_parse_parent_directory handles few corner cases
 *  incorrectly.
 */

#include "util/fileops.h" /* for prototypes */

#include <stdio.h>   /* for vsnprintf() */
#include <stdlib.h>  /* for NULL, malloc, free() */
#include <string.h>  /* for memcpy() */
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "util/trace.h"
#include "util/guard.h"



/* constants */

#define MAX_PATH_SIZE 1024



/* helper macros */

#define GET_USER_ID()  getuid()
#define GET_GROUP_ID() getgid()



/* helper functions */

static int _is_directory(const struct stat* buf);
static int _is_readable(const struct stat* buf, uid_t uid, uid_t gid);
static int _is_writeable(const struct stat* buf, uid_t uid, uid_t gid);
static int _is_executable(const struct stat* buf, uid_t uid, uid_t gid);
static int _ends_with(const char* str, const char* ending);
static int _store(char* dst, int dst_size, const char* src);



/* debugging */

static const int debug_trace_type = TRACE_DEBUG;
#define DEBUG_TRACE(format, arg) TRACE(debug_trace_type, format, arg)



/* definitions of exported functions */


/**
 *  @brief Check whether the specified file exists.
 *
 *  @param path The path of the file to check.
 *
 *  @return 0 on success. Non-zero otherwise.
 */
int fileops_check_file_exists(const char* path)
{
  struct stat buf;
  int stat_ret = stat(path, &buf);
  if (stat_ret != 0) {
    DEBUG_TRACE("stat(%s, ...) failed\n", path);
    return FILEOPS_ERROR_NOT_FOUND;
  }

  return 0;
}



/**
 *  @brief Check whether the specified file exists and is a directory.
 *
 *  @param path The path of the file to check.
 *
 *  @return 0 on success. Non-zero otherwise.
 */
int fileops_check_file_directory(const char* path)
{
  struct stat buf;
  int stat_ret = stat(path, &buf);
  if (stat_ret != 0) {
    DEBUG_TRACE("stat(%s, ...) failed\n", path);
    return FILEOPS_ERROR_NOT_FOUND;
  }

  if (_is_directory(&buf))
    return 0;
  else
    return FILEOPS_ERROR_NOT_DIRECTORY;
}



/**
 *  @brief Check whether the file is readable.
 *
 *  @param path The path of the file to check.
 *
 *  @return 0 on success. Non-zero otherwise.
 */
int fileops_check_file_readable(const char* path)
{
  struct stat buf;
  int stat_ret = stat(path, &buf);
  if (stat_ret != 0) {
    DEBUG_TRACE("stat(%s, ...) failed\n", path);
    return FILEOPS_ERROR_NOT_FOUND;
  }

  uid_t uid = GET_USER_ID();
  uid_t gid = GET_GROUP_ID();
  if (_is_readable(&buf, uid, gid))
    return 0;
  else
    return FILEOPS_ERROR_PERMISSION_DENIED;
}



/**
 *  @brief Check whether the file is writeable.
 *
 *  @param path The path of the file to check.
 *
 *  @return 0 on success. Non-zero otherwise.
 */
int fileops_check_file_writeable(const char* path)
{
  struct stat buf;
  int stat_ret = stat(path, &buf);
  if (stat_ret != 0) {
    DEBUG_TRACE("stat(%s, ...) failed\n", path);
    return FILEOPS_ERROR_NOT_FOUND;
  }

  uid_t uid = GET_USER_ID();
  uid_t gid = GET_GROUP_ID();
  if (_is_writeable(&buf, uid, gid))
    return 0;
  else
    return FILEOPS_ERROR_PERMISSION_DENIED;
}



/**
 *  @brief Check whether the file is executable.
 *
 *  @param path The path of the file to check.
 *
 *  @return 0 on success. Non-zero otherwise.
 */
int fileops_check_file_executable(const char* path)
{
  struct stat buf;
  int stat_ret = stat(path, &buf);
  if (stat_ret != 0) {
    DEBUG_TRACE("stat(%s, ...) failed\n", path);
    return FILEOPS_ERROR_NOT_FOUND;
  }

  uid_t uid = GET_USER_ID();
  uid_t gid = GET_GROUP_ID();
  if (_is_executable(&buf, uid, gid))
    return 0;
  else
    return FILEOPS_ERROR_PERMISSION_DENIED;
}



/**
 *  @brief Check whether directory is readable and "executable".
 *
 *  @param path The path of the file to check.
 *
 *  @return 0 on success. Non-zero otherwise.
 */
int fileops_check_directory_accessible(const char* path)
{
  struct stat buf;
  int stat_ret = stat(path, &buf);
  if (stat_ret != 0) {
    DEBUG_TRACE("stat(%s, ...) failed\n", path);
    return FILEOPS_ERROR_NOT_FOUND;
  }

  if (!_is_directory(&buf))
    return FILEOPS_ERROR_NOT_DIRECTORY;
  
  uid_t uid = GET_USER_ID();
  uid_t gid = GET_GROUP_ID();
  if (_is_readable(&buf, uid, gid) && _is_executable(&buf, uid, gid))
    return 0;
  else
    return FILEOPS_ERROR_PERMISSION_DENIED;
}



/**
 *  @brief Check whether a file of the given name can be
 *  created. Basically, this comes down to either checking if the file
 *  exists and is writeable or checking that the file does not exist,
 *  but the parent exists and is writable.
 *
 *  @param path The path of the file to check.
 *
 *  @return 0 on success. Non-zero otherwise.
 */
int fileops_check_file_creatable(const char* path)
{
  struct stat fbuf;
  int stat_ret_f = stat(path, &fbuf);
  if (stat_ret_f == 0)
  {
    /* file exists! */
    uid_t uid = GET_USER_ID();
    uid_t gid = GET_GROUP_ID();
    if (_is_writeable(&fbuf, uid, gid))
      return 0;
    else
      return FILEOPS_ERROR_PERMISSION_DENIED;
  }
  else
  {
    /* file does not exist */
    int size = strlen(path)+1;
    if (size > MAX_PATH_SIZE)
      return FILEOPS_ERROR_PATH_TOO_LONG;
    
    /* construct parent directory */
    array_guard_t<char> parent_dir = new char[size];
    int ret = fileops_parse_parent_directory(parent_dir, size, path);
    assert((ret == 0) || (ret == FILEOPS_ERROR_PATH_TOO_LONG));
    if (ret == FILEOPS_ERROR_PATH_TOO_LONG)
      return FILEOPS_ERROR_PATH_TOO_LONG;

    /* check parent exists */
    struct stat pbuf;
    int stat_ret_p = stat(parent_dir, &pbuf);
    if (stat_ret_p != 0)
      /* parent directory does not exist */
      return FILEOPS_ERROR_PERMISSION_DENIED;

    /* parent exists... check permissions */
    uid_t uid = GET_USER_ID();
    uid_t gid = GET_GROUP_ID();
    if (_is_writeable(&pbuf, uid, gid))
      return 0;
    else
      return FILEOPS_ERROR_PERMISSION_DENIED;
  }
}



/**
 *  @brief Get the parent directory of the 'src' file.
 *
 *  @param dst Buffer to write the parent directory into.
 *
 *  @param dst_size The number of bytes in 'dst'. These bytes are used
 *  to store the trailing '\0'.
 *
 *  @param src The file to find the parent of.
 *
 *  @return 0 on success. Non-zero otherwise.
 */
int fileops_parse_parent_directory(char* dst, int dst_size, const char* src)
{

  /* MAYBE WE SHOULD JUST USE MALLOC AND FORGET ABOUT MAX_PATH_SIZE */
  /* THE ONLY REASON WE USE MAX_PATH_SIZE IS TO AVOID STACK
     OVERFLOW */

  int src_len = strlen(src);
  int size = src_len+1;
  if (size > dst_size)
    return FILEOPS_ERROR_PATH_TOO_LONG;
  

  /* TODO Copy 'src' into 'dst' and compress 'dst' to remove "/./" and
     trailing '/' and "/.". */
  if (0)
    _ends_with(src, src);


  /* Now search for last '/' character */
  int slash_pos;
  for (slash_pos = src_len-1; slash_pos >= 0; slash_pos--)
    if (src[slash_pos] == '/')
      break;
  
  if (slash_pos >= 0)
  {

    /* TODO Deal with "../../../.." cases. Here, we construct the
       parent by adding another ".." */


    /* The characters from 0 to 'slash_pos' make up the parent
       directory. */
    strncpy(dst, src, slash_pos+1);
    dst[slash_pos+1] = '\0';
    return 0;
  }


  /* This part seems to be correct. If we have no '/' characters, we
     are looking at ".", "..", or a file in the current directory. */
  if (!strcmp(src, "."))
    return _store(dst, dst_size, "../");
  
  if (!strcmp(src, ".."))
    return _store(dst, dst_size, "../../");
  
  return _store(dst, dst_size, "./");
}




/* definitions of helper functions */


static int _is_directory(const struct stat* buf)
{
  return S_ISDIR(buf->st_mode);
}


static int _is_readable(const struct stat* buf, uid_t uid, uid_t gid)
{
  return
  (buf->st_mode & S_IROTH)
       || ((buf->st_mode & S_IRGRP) && (buf->st_gid == gid))
       || ((buf->st_mode & S_IRUSR) && (buf->st_uid == uid));
}


static int _is_writeable(const struct stat* buf, uid_t uid, uid_t gid)
{
  return
  (buf->st_mode & S_IWOTH)
       || ((buf->st_mode & S_IWGRP) && (buf->st_gid == gid))
       || ((buf->st_mode & S_IWUSR) && (buf->st_uid == uid));
}


static int _is_executable(const struct stat* buf, uid_t uid, uid_t gid)
{
  return
  (buf->st_mode & S_IXOTH)
       || ((buf->st_mode & S_IXGRP) && (buf->st_gid == gid))
       || ((buf->st_mode & S_IXUSR) && (buf->st_uid == uid));
}


static int _ends_with(const char* str, const char* ending)
{
  int str_len = strlen(str);
  int ending_len = strlen(ending);

  /* check sizes */
  if (ending_len > str_len)
    return 0;

  int str_pos = str_len-1;
  int ending_pos = ending_len-1;

  while (ending_pos >= 0)
  {
    if (ending[ending_pos] != str[str_pos])
      return 0;
    str_pos--;
    ending_pos--;
  }

  /* 'ending_pos' == 0. All characters found. */
  return 1;
}


static int _store(char* dst, int dst_size, const char* src)
{
  int src_len = strlen(src);
  if ((src_len+1) > dst_size)
    return FILEOPS_ERROR_PATH_TOO_LONG;
  strncpy(dst, src, src_len);
  dst[src_len] = '\0';
  return 0;
}
