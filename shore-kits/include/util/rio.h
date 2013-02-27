
/** @file rio.c
 *
 *  @brief Exports RIO (Robust I/O) package, a collction of read/write
 *  wrappers used to access files (and sockets).
 *
 *  @author Naju Mancheril (ngm)
 *
 *  @bug None known
 */
#ifndef _RIO_H
#define _RIO_H

#include <sys/types.h>




/* exported constants */

/** @def RIO_BUFSIZE
 *
 *  @brief The size of a RIO buffer.
 */
#define RIO_BUFSIZE 8192




/* exported types */

/** @typedef rio_t
 *
 *  @brief Persistent state for the robust I/O (Rio) package.
 */
typedef struct
{
  int     rio_fd;                /* descriptor for this internal buf */
  ssize_t rio_cnt;               /* unread bytes in internal buf */
  char   *rio_bufptr;            /* next unread byte in internal buf */
  char    rio_buf[RIO_BUFSIZE];  /* internal buffer */

} rio_t;




/* exported functions */

ssize_t rio_readn (int fd, void* usrbuf, size_t n);
ssize_t rio_writen(int fd, const void* usrbuf, size_t n);
void rio_readinitb(rio_t *rp, int fd);
ssize_t rio_readnb(rio_t *rp, void *usrbuf, size_t n);
ssize_t rio_readlineb(rio_t *rp, void *usrbuf, size_t maxlen);



#endif
