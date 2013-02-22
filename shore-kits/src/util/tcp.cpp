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

/** @file:   tcp.cpp
 *
 *  @brief:  Implements simple functions to set up TCP client and server
 *           connections. This code is adapted from the CSAPP package 
 *           developed at Carnegie Mellon University. That in turn is 
 *           adapted from Stevens.
 *
 *  @author: Naju Mancheril (ngm)
 */

#include "util/tcp.h"   /* for prototypes */

#include <stdlib.h>     /* for NULL */
#include <string.h>     /* for memcpy() */
#include <sys/types.h>  /* for size_t */
#include <sys/socket.h> /* for socket(), PF_INET */
#include <netinet/in.h> /* for htons() */
#include <netdb.h>      /* for gethostbyname() */


// internal constants

const int LISTENQ = 1024;
const int GETHOSTBYNAME_R_INITIAL_BUFFER_SIZE = 16;
const int GETHOSTBYNAME_R_MAX_BUFFER_SIZE     = 128;


/**
 *  @brief Open a listening (server) socket. This function is
 *  thread-safe.
 *
 *  @param port The port to listen on.
 *
 *  @return An opened listening socket on the specified port. -1 on
 *  error (check errno for error type).
 */

int 
open_listenfd(int port) 
{
  int listenfd, optval=1;
  struct sockaddr_in serveraddr;
  
  // 1. Create a socket descriptor
  if ((listenfd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
    return (-1);
  }
    
  // 2. Eliminates "Address already in use" error from bind
  if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, 
		 (const void *)&optval , sizeof(int)) < 0) {
    return (-1);
  }

    
  // 3. Listenfd will be an endpoint for all requests to port
  //   on any IP address for this host
  memset((char *) &serveraddr, 0, sizeof(serveraddr));
  serveraddr.sin_family = PF_INET; 
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY); 
  serveraddr.sin_port = htons((unsigned short)port); 

  if (bind(listenfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0) {
    return (-1);
  }
    
  // 4. Make it a listening socket ready to accept connection requests
  if (listen(listenfd, LISTENQ) < 0) {
    return -1;
  }

  return (listenfd);
}


/**
 *  @brief Open a TCP connection to a server. This function uses
 *  gethostbyname() and is not thread-safe.
 *
 *  @param hostname The name of the server to connect to.
 *
 *  @param port The port to connect to on the server.
 *
 *  @return A socket descriptor ready for reading and writing on
 *  success. -1 on Unix error (check errno for error type). -2 on DNS
 *  error (check h_errno for error type).
 */

int 
open_clientfd(const char* hostname, int port) 
{
  int clientfd;
  struct hostent* hp;
  struct sockaddr_in serveraddr;

  // 1. Accept socket connection
  if ((clientfd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
    return (-1); /* check errno for cause of error */
  }


  // 2. Fill in the server's IP address and port
  if ((hp = gethostbyname(hostname)) == NULL) {
    return (-2); /* check h_errno for cause of error */
  }

  memset((char*) &serveraddr, 0, sizeof(serveraddr));
  serveraddr.sin_family = PF_INET;
  memcpy((char *)&serveraddr.sin_addr.s_addr,
	 (char *)hp->h_addr, hp->h_length);
  serveraddr.sin_port = htons(port);
  
  // 3. Establish a connection with the server
  if (connect(clientfd, (struct sockaddr *) &serveraddr, sizeof(serveraddr)) < 0) {
    return (-1);
  }

  return (clientfd);
}
