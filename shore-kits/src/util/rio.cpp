
/** @file rio.cpp
 *
 *  @brief Implements RIO (Robust I/O) package, a collction of
 *  read/write wrappers used to access files (and sockets). This code
 *  is adapted from the CSAPP package developed at Carnegie Mellon
 *  University.
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @bug None known.
 */
#include "util/rio.h"  /* for prototypes */



#include <string.h>    /* for memcpy() */
#include <sys/types.h> /* for size_t */
#include <unistd.h>    /* for read()/write() */
#include <errno.h>     /* for errno */





/* internal helper functions */

static ssize_t rio_read(rio_t* rp, char* usrbuf, ssize_t n);





/* exported functions */


/**
 *  @brief Robustly read from a file. That is, read n bytes (or until
 *  we hit EOF). This function is unbuffered.
 *
 *  @param fd The file to read from.
 *
 *  @param userbuf The buffer to read into.
 *
 *  @param n The number of bytes to robustly read.
 *
 *  @return -1 on error. Check errno for error type. The number of
 *  bytes read on success.
 */
ssize_t rio_readn(int fd, void* usrbuf, size_t n) 
{
  size_t  nleft = n;
  ssize_t nread;
  char* bufp = (char*)usrbuf;

  while (nleft > 0)
  {
    if ((nread = read(fd, bufp, nleft)) < 0)
    {
      if (errno == EINTR) /* Interrupted by signal. No bytes read. */
	nread = 0;        /* Call read again. */
      else
	return -1;
    } 
    else if (nread == 0)  /* EOF */
      break;
    nleft -= nread;
    bufp += nread;
  }

  return (n - nleft);     /* return number of bytes read (>= 0) */
}





/**
 *  @brief Robustly write to a file. That is, write n bytes (or until
 *  we get a file close error). This function is unbuffered.
 *
 *  @param fd The file to read from.
 *
 *  @param userbuf The buffer to write.
 *
 *  @param n The number of bytes to robustly write.
 *
 *  @return -1 on error. Check errno for error type. The number of
 *  bytes written on success. This should always be n.
 */
ssize_t rio_writen(int fd, const void* usrbuf, size_t n) 
{
    size_t nleft = n;
    ssize_t nwritten;
    char* bufp = (char*)usrbuf;

    while (nleft > 0)
    {
      if ((nwritten = write(fd, bufp, nleft)) <= 0)
      {
	if (errno == EINTR) /* Interrupted by signal. No bytes written. */
	  nwritten = 0;     /* Call write again. */
	else
	  return -1;
      }
      nleft -= nwritten;
      bufp += nwritten;
    }

    return n;
}





/**
 *  @brief Associate a descriptor with a read buffer and reset buffer.
 *
 *  @param rp This rio_t will be initialized.
 *
 *  @param fd The file descriptor to associate with rp.
 *
 *  @return void
 */
void rio_readinitb(rio_t *rp, int fd) 
{
  rp->rio_fd = fd;  
  rp->rio_cnt = 0;  
  rp->rio_bufptr = rp->rio_buf;
}





/**
 *  @brief Robustly read from a file. That is, read n bytes (or until
 *  we hit EOF). This function is buffered.
 *
 *  @param rp The file to read from.
 *
 *  @param userbuf The buffer to read into.
 *
 *  @param n The number of bytes to robustly read.
 *
 *  @return -1 on error. Check errno for error type. The number of
 *  bytes read on success.
 */
ssize_t rio_readnb(rio_t *rp, void *usrbuf, size_t n) 
{
  size_t nleft = n;
  ssize_t nread;
  char* bufp = (char*)usrbuf;
  
  while (nleft > 0)
  {
    if ((nread = rio_read(rp, bufp, nleft)) < 0)
    {
      if (errno == EINTR) /* Interrupted by signal. No bytes read. */
	nread = 0;        /* Call read again. */
      else
	return -1;
    } 
    else if (nread == 0)
      break;              /* EOF */
    nleft -= nread;
    bufp += nread;
  }

  return (n - nleft);     /* return number of bytes read (>= 0) */
}





/**
 *  @brief Robustly read a line of text from a file. That is, read
 *  until we hit a '\n' (or until we hit EOF, or until we fill our
 *  read buffer). This function is buffered.
 *
 *  @param rp The file to read from.
 *
 *  @param userbuf The buffer to read into.
 *
 *  @param maxlen The number of bytes to robustly read.
 *
 *  @return -1 on error. Check errno for error type. The number of
 *  bytes read on success.
 */
ssize_t rio_readlineb(rio_t *rp, void *usrbuf, size_t maxlen)
{
  int rc;
  size_t n;
  char c, *bufp = (char*)usrbuf;


  for (n = 1; n < maxlen; n++)
    { 
    if ((rc = rio_read(rp, &c, 1)) == 1)
    {
      *bufp++ = c;
      if (c == '\n')
	break;
    }
    else if (rc == 0)
    {
      if (n == 1)
	return 0; /* EOF, no data read */
      else
	break;    /* EOF, some data was read */
    }
    else
      return -1;  /* error */
  }


  *bufp = 0;
  return n;
}





/* definitions of internal helper functions */


/**
 *  @brief Read from buffer. Refill buffer if empty.
 *
 *  @param fd The file to read from.
 *
 *  @param userbuf The buffer to read into.
 *
 *  @param n The number of bytes to robustly read.
 *
 *  @return -1 on error. Check errno for error type. The number of
 *  bytes read on success.
 *
 *  This is a wrapper for the Unix read() function that transfers
 *  min(n, rio_cnt) bytes from an internal buffer to a user buffer,
 *  where n is the number of bytes requested by the user and rio_cnt
 *  is the number of unread bytes in the internal buffer. On entry,
 *  rio_read() refills the internal buffer via a call to read() if the
 *  internal buffer is empty.
 */
static ssize_t rio_read(rio_t* rp, char* usrbuf, ssize_t n)
{

  int cnt;

  
  /* refill if buf is empty */
  while (rp->rio_cnt <= 0)
  {
    rp->rio_cnt = read(rp->rio_fd, rp->rio_buf, sizeof(rp->rio_buf));
    if (rp->rio_cnt < 0)
    {
      if (errno != EINTR)  /* Interrupted by signal. No bytes read. */
	return -1;
    }
    else if (rp->rio_cnt == 0)  /* EOF */
      return 0;
    else 
      rp->rio_bufptr = rp->rio_buf; /* reset buffer ptr */
  }

  
  /* copy min(n, rp->rio_cnt) bytes from internal buf to user buf */
  cnt = n;          
  if (rp->rio_cnt < n)
    cnt = rp->rio_cnt;
  memcpy(usrbuf, rp->rio_bufptr, cnt);
  rp->rio_bufptr += cnt;
  rp->rio_cnt -= cnt;


  return cnt;
}
