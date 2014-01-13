/* (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP */

/*<std-header orig-src='shore' incl-file-exclusion='MAINPAGE_H'>

 $Id: mainpage.h,v 1.14 2010/10/27 17:04:20 nhall Exp $

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

/* this file contains only Doxygen documentation */


/**\page COPYRIGHT Copyrights and Licenses 
 *

Major parts of the code have been modified or developed by HP, and are
subject to the HPDC copyright, below.

The main code base of the storage manager is also subject to the SHORE/UW 
copyright (given below) and most of it is also subject to 
the SHORE-MT/DIAS copyright (also given below).
Both copyrights are hereby extended to 2010.

The strstream compatibility code found in src/fc/w_compat_strstream.h and
src/fc/w_compat_strstream.cpp
is subject to the Silicon Graphics copyright, below.

The regex code found in the src/common/ library is subject to the 
Henry Spencer/ATT copyright and license, contained in src/common/regex2.h, and
included below.

What little remains of the old SHORE sthreads library is subject to
copyright given in those source files (src/sthread/sthread.h) as well as to the SHORE/UW and
SHORE-MT/DIAS copyrights.

 - \b HPDC \b Copyright:

(c) Copyright 2011-2013, Hewlett-Packard Development Company, LP


 -       \b SHORE/UW \b Copyright:

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-2010 Computer Sciences Department, University of
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

 -       \b SHORE-MT/DIAS \b Copyright:

Shore-MT -- Multi-threaded port of the SHORE storage manager

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


 -       \b Silicon \b Graphic \b Copyright \b and \b License:

 Copyright (c) 1998
 Silicon Graphics Computer Systems, Inc.

 Permission to use, copy, modify, distribute and sell this software
 and its documentation for any purpose is hereby granted without fee,
 provided that the above copyright notice appear in all copies and
 that both that copyright notice and this permission notice appear
 in supporting documentation.  Silicon Graphics makes no
 representations about the suitability of this software for any
 purpose.  It is provided "as is" without express or implied warranty.

 -       \b Henry \b Spencer/ATT \b Copyright \b and \b License:

Copyright 1992, 1993, 1994, 1997 Henry Spencer.  All rights reserved.
This software is not subject to any license of the American Telephone
and Telegraph Company or of the Regents of the University of California.

Permission is granted to anyone to use this software for any purpose on
any computer system, and to alter it and redistribute it, subject
to the following restrictions:

1. The author is not responsible for the consequences of use of this
   software, no matter how awful, even if they arise from flaws in it.

2. The origin of this software must not be misrepresented, either by
   explicit claim or by omission.  Since few users ever read sources,
   credits must appear in the documentation.

3. Altered versions must be plainly marked as such, and must not be
   misrepresented as being the original software.  Since few users
   ever read sources, credits must appear in the documentation.

4. This notice may not be removed or altered.

 * */
/** \mainpage Zero Storage Manager
 * \section Brief Description
 *
 * This is an experiment test-bed library for use by researchers who wish to
 * write multi-threaded software that manages persistent data.
 *
 * This storage engine provides the following capabilities:
 *  - transactions with ACID properties, with ARIES-based logging and recovery,
 *  primitives for partial rollback,
 *  transaction chaining, and early lock release,
 *  - persistent storage structures : 
 *  B+ tree indexes and files of untyped records,
 *  - fine-grained locking for records and B+ tree indexes with deadlock detection,
 *  ptional coarse-grained locking, 
 *  - in-memory buffer management with optional prefetching, 
 *  - extensible statistics-gathering, option-processing, and error-handling 
 *  facilities.
 *
 * This software runs on Pthreads, thereby providing its client software
 * (e.g., a database server) multi-threading
 * capabilities and resulting scalability from modern SMP and NUMA 
 * architectures, and has been used on Linux/x86-64 and Solaris/Niagara
 * architectures.
 *
 * \section Background
 *
 * The system is based on SHORE-MT, as modified by HP Labs.
 *
 * The SHORE (Scalable Heterogeneous Object REpository) project
 * at the University of Wisconsin - Madison Department of Computer Sciences
 * produced the first release
 * of this storage manager as part of the full SHORE release in 1996.
 * The storage manager portion of the SHORE project was used by 
 * other projects at the UW and elsewhere, and was intermittently
 * maintained through 2008.
 *
 * The SHORE Storage Manager was originally developed on single-cpu Unix-based systems,
 * providing support for "value-added" cooperating peer servers, which were processes
 * on single-CPU machines.  One such server was the
 * SHORE Value-Added Server (http://research.cs.wisc.edu/shore), and another of which was 
 * Paradise (http://research.cs.wisc.edu/paradise) at the University of Wisconsin.
 * The
 * TIMBER (http://www.eecs.umich.edu/db/timber, and http://dx.doi.org/10.1007/s00778-002-0081-x) and 
 * Pericope (http://www.eecs.umich.edu/periscope) projects 
 * at the University of Michigan, 
 * PREDATOR (http://www.distlab.dk/predator) at Cornell,
 * Lachesis (http://www.vldb.org/conf/2003/papers/S21P03.pdf)
 * BODHI (http://dsl.serciisc.ernet.in/projects/BODHI/homepage/index.html, http://doi.acm.org/10.1145/1007568.1007717)
 * used the SHORE Storage Manager.
 * The storage manager has been used for innumerable published studies since
 * then.
 *
 * The storage manager had its own (non-preemptive) threads library, which
 * forked a per-disk cooperating process to provide non-blocking I/O. 
 * The storage manager also had a communications library for peer-server communication. 
 * Until recently, the code structure, nomenclature, 
 * and contents reflected its SHORE roots.
 *
 * In 2007, the Data Intensive Applications and Systems Labaratory (DIAS)
 * at Ecole Polytechnique Federale de Lausanne
 * began work on a port of release 5.0.1 of the storage manager to Pthreads,
 * and developed more scalable synchronization primitives, identified
 * bottlenecks in the storage manager, and improved the scalability of 
 * the code.
 * This work was on a Solaris/Niagara platform and was released as Shore-MT
 * http://diaswww.epfl.ch/shore-mt).
 * It was a partial port of the storage manager and did not include documentation.
 * Projects using Shore-MT include
 * StagedDB/CMP (http://www.cs.cmu.edu/~stageddb/),
 * DORA (http://www.cs.cmu.edu/~ipandis/resources/pvldb10pandis.pdf)
 *
 * In 2009, the University of Wisconsin - Madison (UW) took the first Shore-MT
 * release and ported the remaining code to Pthreads.
 * This work as done on a Red Hat Linux/x86-64 platform.
 * The UW and DIAS cooperated on bug fixes and some internal
 * rewrites.
 * This release is the result of that work, and includes this documentation,
 * bug fixes, and supporting test code.   
 *
 * \section Copyrights Copyrights and Licenses
 *
This distribution contains code and documentation subject to several copyrights and licenses.
See the copyrights page \ref COPYRIGHT "Copyrights and Licenses, here".

 *
 * \section START Getting Started With the Shore Storage Manager
 * A good place to start is with the programming interface, described \ref SSMAPI "here".
 *
 *\section BUILD Configuring and Building the Storage Manager
 * See \ref OPT "this page" to configure and build the storage manager.
 *
 * \section IMPLNOTES1 Implementation Notes
 * See \ref IMPLNOTES "this page" for some implementation details.
 *
 * \section BUGREPORT Reporting Bugs
 * This is unsupported software.  We hope there will be times when someone will
 * be able to respond to problem reports.
 * A problem report should include the following information:
 * - Release number/version of the storage manager.
 * - Platform (operating system, hardware/CPU type).
 * - Configuration options (command used to build the storage manager).
 * - Copy of program that demonstrates problematic behavior, if possible (if
 *   not itself part of the release).
 * - Description of problematic behavior, and how the program is run to
 *    reproduce the behavior.  If you can create a script to reproduce the
 *    problem, include that.
 *
 * Mail this to: express@groups.hp.com
 *
 * \section REFS References
 * See \ref REFERENCES "this page" for references to selected papers 
 * from which ideas are used in the Shore Storage Manager. 
 */

/**\addtogroup OPT
 *
 * <b> This Section is out of date. Use cmake. JIRA issue ZERO-148 covers fixing this </b>
 *
 * Configuring and building the storage manager consists of these steps,
 * all done at the root of the distribution directory tree.
 * - bootstrap
 * - configure
 * - build (make)
 *
 *\section OPTBOOT Bootstrapping 
 * Bootstrapping might not be necessary, but if you have the autotools
 * installed, it might save time to bootstrap the first time you try to 
 * build, particularly if you are installing on a system other than Linux.
 * To bootstrap, type ./bootstrap. You can also look at that script and
 * run selected parts of it, since all it does is run the autotools.
 *
 * Autotools run abysmally slowly on Solaris.
 *
 *\section OPTCONF Configuring 
 * There are two parts to configuring the storage manager.
 * The original configuration scheme of SHORE was encapsulated in
 * \e config/shore.def, which described all or most pertinent CPP macros. 
 * We are moving away from that scheme and replacing it with
 * autoconf options and features, but a few things still remain under
 * the control of \e shore.def.
 * These fall into three categories:
 *     - details related to autoconf-controlled options, such as pathnames
 *     - basic compile-time constants that someone extending the
 *       storage manager might want to change 
 *     - maintainer's tools
 *
 * There remaining some CPP macros not 
 * described in \e config/shore.def:
 *      - code of occasional utility for debugging purposes
 *      - code intended to be the subject of future experimentation
 *
 * Configuring amounts to running ./configure from the root of the
 * distribution directory tree, and, depending on the features you 
 * wish to use, editing \e config/shore.def.
 *
 * \remarks
 * The storage manager API contains a method ss_m::config_info (q.v.) that
 * allows a server to determine, at run time, some of the
 * compile-time limits determined by the configuration.
 *
 *\subsection CONFIGOPT Configuration Options
 * To find the configuration options, type 
 *\code ./configure --help \endcode, the output of which is reproduced here.
 *
 * \verbatim
SHORE-specific Features:
  --enable-pthread-mutex default:yes    Compile to use pthreads for mutexes for
                                        certain critical sections.
  --enable-lp64         default:yes     Compile to use LP 64 data model
                                        No other data model is supported yet.
                                        But we hope some day to port back to LP32.
  --enable-checkrc      default:no      Generate (expensive) code to verify return-code checking
                                        If a w_rc_t is set but not checked with
                                        method is_error(), upon destruction the
                                        w_rc_t will print a message to the effect
                                        "error not checked".
  --enable-trace        default:no      Include tracing code
                                        Run-time costly.  Good for debugging
                                        problems that are not timing-dependent.
                                        Use with DEBUG_FLAGS and DEBUG_FILE
                                        environment variables.  See \ref SSMTRACE.
  --enable-dbgsymbols   default:no      Turn on debugger symbols
                                        Use this to override what a given
                                        debugging level will normally do.
  --enable-explicit     default:no      Compile with explicit templates
                                        NOT TESTED. 
 \todo mainpage.h compile with or remove explicit templates

  --enable-valgrind     default:no      Enable running valgrind run-time behavior
                                        Includes some code for valgrind.

SHORE-specific Optional Packages:
  --with-page-size        1024, 2048, 4196, 8192(default), 16384, or 32768
  --with-hugetlbfs        Use the hugetlbfs for the buffer pool.
                          Depending on the target architecture, this might
                          be useful.  If you use it, you will need to set
                          a path for your hugetlbfs in config/shore.def.
                          The default is :
                          #define HUGETLBFS_PATH "/mnt/huge/SSM-BUFPOOL"
  --without-mmap          Do not use mmap for the buffer pool. Trumps
                          hugetlbfs option.
  --with-debug-level1     Include level 1 debug code, optimize.
                          This includes code in w_assert1 and 
                          #if W_DEBUG_LEVEL > 0 /#endif pairs and 
                          #if W_DEBUG_LEVEL >= 1 /#endif pairs and  and
                          W_IFDEBUG1
  --with-debug-level2     Include level 2 debug code, no optimize.
                          Equivalent to debug level 1 PLUS
                          code in w_assert2 and
                          #if W_DEBUG_LEVEL > 1 /#endif pairs and
                          #if W_DEBUG_LEVEL >= 2 /#endif pairs and
                          W_IFDEBUG2
  --with-debug-level3     Include level 3 debug code, no optimize.
                          Equivalent to debug level 2 PLUS
                          includes code in w_assert3  and
                          #if W_DEBUG_LEVEL > 2 /#endif pairs and
                          #if W_DEBUG_LEVEL >= 3 /#endif pairs and
                          W_IFDEBUG3
 \endverbatim

 \bug GNATS 136 Only 64-bit platforms are supported. The issue is that
 lsns and some other data structures need atomic methods.

 \bug GNATS 143 
 The polyphase merge-sort does not handle this case:
 More than one run is required AND 
 keys are embedded in  objects AND the keys are 
 already in lexicographic order in the
 object AND the keys are properly aligned AND the keys 
 span multiple pages.
 It returns an error indication about a "broken key" comparison not 
 being implemented. The workaround is to provide a marshal function
 or to put the sort key in the header of the object or to provide more
 memory (larger number of pages for the run size) for the sort.
 

 \bug GNATS 139  Convert w_assert9 to w_assert3 where the asserts are still reasonable and remove the rest.  Some of these are obsolete, some are racy in the new mt-context.  All the w_assert9's are what used to be w_assert3; they were turned into 9 to disable them until they could be evaluated for usefulness, and many have been converted to 2 or 3-level asserts; many remain to be addressed.

 * \subsection SHOREDEFOPT Description of Selected CPP Macros 
 * In this section we describe selected macros defined (or not) in
 * \e config/shore.def.
 *
 * - HUGETLBFS_PATH See --with-hugetlbfs in \ref CONFIGOPT;
 *   see also \ref REFHUGEPAGE1 "Huge Pages" for use of hugetlbfs with Linux.
 *
 * - USE_SSMTEST Define this if you want to include crash test hooks in your
 *   smsh.  This is for a maintainer's testing purposes and should not be
 *   defined for a release version of the storage manager.
 *
 * - COMMON_GTID_LENGTH : You can override the default length of a global
 *   transaction id. 
 *   Useful only if your server implements distributed 
 *   transactions.
 *
 * - COMMON_SERVER_HANDLE_LENGTH : You can override the default length of a 
 *   server handle, the handle by which the server identifies a
 *   coordinator of distributed transaction.
 *   Useful only if your server implements distributed 
 *   transactions.
 *
 * - SM_LOG_PARTITIONS  : You can override the default maximum number of
 *   open partitions for the log by defining this. 
 *
 *\subsection OPTTCL Tcl and smsh 
 * The storage manager test shell, smsh, uses Tcl.
 * Autoconf tries to find Tcl in a standard place; if it is found,
 * fine. But if not, you must define two paths to your Tcl 
 * library and include files in \e Makefile.local
 * at the top of the directory tree.  
 * If you Tcl installation is not built for multithreading, you must 
 * install such a copy and put its path in \e Makefile.local.
 *
 * Tcl is available from 
 * - ActiveState (http://www.activestate.com/activetcl/), or
 * - SourceForge (http://sourceforge.net/projects/tcl/files/).
 *
 *\section OPTBLD Building
 * Building the storage manager consist of running
 * \code
 make
 \endcode
 *
 * Since it is not alway easy to tell what options were used for the
 * most recent build in a directory, the compiler options used on the
 * build are put in the file \e makeflags 
 * and the rest of the options are determined in \e config/shore-config.h,
 * produced at configuration time.
 *
 * \section SHOREMKCHECK Checking the Release
 *
 * After building the storage manager, you can check it by running
 * \code
 make check
 \endcode
 * in the root of the directory tree.
 * This runs unit tests for each libary.
 *
 * \attention
 * The storage manager test shell, smsh, is run by
 * \code make check \endcode.  Smsh uses Tcl.  The path to you
 * Tcl installation is given in \e Makefile.local  at the 
 * top of the directory tree.
 *
 * \note
 * If you do not have Tcl installed and want to test the installation
 * without smsh, you may run
 * \e make \e check and ignore the fact that 
 * it chokes trying to build smsh, because
 * smsh is the last test that \e make \e check runs.
 *
 * \section SHOREMKINSTALL Installing the Release
 *
 * You may run 
 * \code
 make install
 \endcode
 * This installs:
 * - the header files  in 
 *   - \<includedir\> [default: \<prefix\>/include]
 * - the libraries     in 
 *   - \<libdir\> [default: \<exec-prefix\>/lib]
 *
 *  To change the prefixes, use one or more of these configure options:
 *  \code configure --prefix=\<path\> \endcode
 *  or
 *  \code configure --libdir=\<path\> \endcode
 *  or
 *  \code configure --includedir=\<path\> \endcode
 *
 */

/**\cond skip */
/**\page HUGETLBFS HugeTLBfs
 *
 * See \ref REFHUGEPAGE1 "Huge Pages" for assorted on-line documentation about
 * using large pages to avoid excessive load on the TLB.
 *
 * Here we do not claim to be complete for all target architectures.
 * This is meant to serve as an example for Linux targets.
 * The following steps are what we did on one RHEL5 system.
 *
 * NOTE: If you have kernel documentation installed, see:
 * /usr/share/doc/kernel-doc-\<version\>/Documentation/vm/hugetlbpage.txt
 *
 * First steps (most of this must be done by the super-user):
 * - Determine that our kernel supports hugetlbfs.
 *   - grep Hugepagesize /proc/meminfo
 * - Ensure that we have adequate huge pages (this example is
 *   for an 8GB buffer pool) on reboot:
 *   - echo "vm.nr_hugepages=4096" >> /etc/sysctl.conf
 *   To dynamically allocate pages, 
 *   - echo 4096 > /proc/sys/vm/nr_hugepages
 * - Create a group for users of the hugetlbfs using your sys admin
 *   applications. My group is called "ssm" and has gid 55555.
 * - Add users to the ssm group.
 * - Create the mount point for a hugetlbfs:
 *   - mkdir -p /mnt/huge
 * - Mount a pseudo filesystem of type hugetlbfs at that mount point; do this
 *   on reboot:
 *   - echo "none       /mnt/huge    hugetlbfs rw,gid=55555,size10g,mode=0770 0 0" > /etc/fstab
 *
 * - Reboot.
 *
 *\warning 
 * <b> If you have configured the storage manager for use with hugetlbfs
 * and have not taken the above steps to ensure that your system actually
 * has the hugetlbfs pages available for use, your storage manager (or
 * the 'make check' tests) will likely croak.  Unfortunately, we have
 * not yet figured out a way to determine ahead of time, programmatically,
 * whether things will go well or not, before we try to write to the 
 * huge pages.  </b>
 *
 *
 * Second steps (this can be done by users in the ssm group):
 * - Edit the default path for the hugetlbfs node in config/shore.def.
 * - Create a file in the hugetlbfs, owned by the ssm group, whose
 *   name matches the default path in config/shore.def, e.g.,
 *   - touch /etc/huge/SSM-BUFPOOL
 * - Configure and build the storage manager with --with-hugetlbfs.
 * - To run using the hugetlbfs is now the default; to run without it,
 *   change the sm_hugetlbfs_path run-time option to the value "NULL"; 
 *   here is an example from the .shoreconfig file in the smsh directory:
 *   - *.server.*.sm_hugetlbfs_path: NULL
 *
 *   Note that your buffer pool size will have to be set to a multiple
 *   of the huge page size for your system.  Thus, if your huge pages are 2 MB
 *   you will get an error from mmap if you use a 3 MB buffer pool.
 *
 *   On the whole, the use of mmap with the hugetlbfs is not reliable
 *   and, although at process end, all huge pages are supposed to be returned
 *   to the system, we have seen cases in which pages were "lost" and
 *   the mmap thereafter failing, repaired only on reboot, so consider this
 *   feature for performance experiments only on systems that do not
 *   require high availability. 
 *
 */
/**\endcond skip */

