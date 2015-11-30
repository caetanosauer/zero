/*<std-header orig-src='shore' incl-file-exclusion='SM_VAS_H'>

 $Id: sm_vas.h,v 1.32 2010/09/21 14:26:17 nhall Exp $

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

#ifndef SM_VAS_H
#define SM_VAS_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/* DOXYGEN documentation : */

/**\addtogroup SSMAPI
 *
 * \section COMPILE Compiling and Linking Server Code
 * \addtogroup SSMAPI
 *
 * The compiler invocation requires certain flags to ensure the
 * use of pthreads and the LP64 data model; it also has to
 * include the storage manager's libraries.
 *
 * When the storage manager  is built,
 * its "make" flags are written to a file called
 * - "makeflags"
 * in the source root directory (or the installed include/ directory).
 * These flags usually include (subject to platform):
 * - -DARCH_LP64 -m64
 * - -D_POSIX_THREAD_SEMANTICS -D_REENTRANT -pthread (Linux)
 * - -library=stlport4 -features=extensions,zla -DSOLARIS2 -mt -D_POSIX_THREAD_SEMANTICS -D_REENTRANT -lpthread (Solaris)
 *
 * The list of libraries must include -lrt and possibly -lnsl.
 *
 * \section EXAMPLES Examples
 * \addtogroup SSMAPI
 *
 * Any code that uses the SHORE Storage Manager requires
 * \code
 * #include <sm_vas.h>
 * \endcode
 * This header file encapsulates all the storage manager header files needed
 * by a value-added server.
 *
 * \subsection EXMIN A Minimal Example
 * For a simple example, see \ref startstop.cpp
 *
 * \subsection EXINIT_CONFIG_OPTIONS Setting Up Run-Time Options
 * The example \ref init_config_options.cpp
 * demonstrates a more extensive handling of run-time options, and
 * is used in other examples, below.
 *
 * \subsection EXCREATE_REC Creating a file of Records
 * The example \ref create_rec.cpp
 * shows a server that creates a file of records.
 * Thus, it also contains code to initialize a volume and
 * create a file.
 *
 * \subsection EXLOG_EXCEED  Use of ss_m::ss_m Arguments
 * The example \ref log_exceed.cpp
 * demonstrates the use of the arguments to the
 * storage manager constructor (ss_m::ss_m).
 * It is an extension of the above example that
 * generates enough log to run out of log space.
 */

 /**\example startstop.cpp
  * This is an example of using \<sm_vas.h\>. It shows a minimal
  * storage manager server, which does nothing but start up (recover) and
  * shut down.
  */
 /**\example init_config_options.cpp
  * This example demonstrates the use of run-time options.
  * This code is used for other examples.
  */
 /**\example create_rec.cpp
  * This example demonstrates creating a file of records.
  * It also demonstrates scanning the file of records,
  * creating a device and volume, and use of the root index.
  * It must also contain, of course, the creation of options, starting up
  * and shutting down a storage manager.
  */
 /**\example log_exceed.cpp
  * This example demonstrates the use of the ss_m::ss_m arguments.
  * It is an extension of the \ref create_rec.cpp example that
  * generates enough log to run out of log space.
  */
#include "w.h"
#include <cstddef>
#include <w_stream.h>

#include "basics.h"
#include "vec_t.h"
#include "tid_t.h"

#undef SM_SOURCE
#include "sm.h"
#include "kvl_t.h" // define kvl_t for lock_base_t
#include "lock_s.h" // define lock_base_t

/*<std-footer incl-file-exclusion='SM_VAS_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
