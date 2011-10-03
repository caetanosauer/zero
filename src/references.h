// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='REFERENCES_H'>

 $Id: references.h,v 1.6 2010/09/15 18:35:30 nhall Exp $

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

/*  -- do not edit anything above this line --   </std-header>*/

/* This file contains doxygen documentation only */

/**\page REFERENCES References 
 *
 * These links were valid on 8/31/2010.
 *
 * \anchor REFSYNC 
 * \htmlonly
 * <b><font color=#B2222 size=5> Synchronization Primitives
 * </font> </b>
 * \endhtmlonly
 *
 * These are papers are pertinent to the synchronization primitives used
 * in the threads layer of the storage manager.
 *
 * \section JPA1 [JPA1]
 * R. Johnson, I. Pandis, A. Ailamaki.  
 * "Critical Sections: Re-emerging Scalability Concerns for Database Storage Engines" 
 * in Proceedings of the 4th. DaMoN, Vancouver, Canada, June 2008
 * (http://doi.acm.org/10.1145/1457150.1457157 http://www.db.cs.cmu.edu/db-site/Pubs/#DBMS:General)
 *
 * \section  B1 [B1]
 * H-J Boehm.
 * "Reordering Constraints for Pthread-Style Locks"
 * HP Technical Report HPL-2005-217R1, September 2006
 * (http://doi.acm.org/10.1145/1229428.1229470 http://www.hpl.hp.com/techreports/2005/HPL-2005-217R1.pdf)
 *
 * \section  MCS1 [MCS1]
 * J.M. Mellor-Crummey, M.L. Scott
 * "Algorithms for Scalable Synchronization on Shared-Memory Multiprocessors"
 * in
 * ACM Transactions on Computer Systems, Vol 9, No. 1, February, 1991, pp
 * 20-65
 * (http://doi.acm.org/10.1145/103727.103729 http://www.cs.rice.edu/~johnmc/papers/tocs91.pdf)
 *
 * \section  SS1 [SS1]
 * M.L.Scott, W.N. Scherer III
 * "Scalable Queue-Based Spin Locks with Timeout"
 * in PPOPP '01, June 18-20, 2001, Snowbird, Utah, USA
 * (http://doi.acm.org/10.1145/379539.379566 http://www.cs.rochester.edu/u/scott/papers/2001_PPoPP_Timeout.pdf)
 *
 * \section  HSS1 [HSS1]
 * B. He, M.L.Scott, W.N. Scherer III
 * "Preemption Adaptivity in Time-Published Queue-Based Spin Locks"
 * in Proceedings of HiPC 2005: 12th International Conference, Goa, India, December 18-21,
 * (http://www.springer.com/computer/swe/book/978-3-540-30936-9 and
 * http://www.cs.rice.edu/~wns1/papers/2005-HiPC-TPlocks.pdf)
 *
 * \anchor REFSMT 
 * \htmlonly
 * <b><font color=#B2222 size=5> SHORE-MT
 * </font> </b>
 * \endhtmlonly
 *
 * This paper describes the DIAS Shore-MT release. 
 *
 * \section JPHAF1 [JPHAF1]
 * R. Johnson, I. Pandis, N. Hardavellas, A. Ailamaki, B. Falsaff
 * "Shore-MT: A Scalable Storage Manager for the MultiCore Era"
 * in Proceedings of the 12th EDBT, St. Petersburg, Russia, 2009
 * (http://doi.acm.org/10.1145/1516360.1516365 http://diaswww.epfl.ch/shore-mt/papers/edbt09johnson.pdf)
 *
 * \anchor REFDLD 
 * \htmlonly
 * <b><font color=#B2222 size=5> Deadlock Detection
 * </font> </b>
 * \endhtmlonly
 *
 * This paper is the basis for the deadlock detection used by
 * the storage manager's lock manager.
 *
 * \section KH1 [KH1]
 * E. Koskinen, M. Herlihy
 * "Dreadlocks: Efficient Deadlock Detection"
 * in SPAA '08, June 14-16, 2008, Munich, Germany
 * (http://doi.acm.org/10.1145/1378533.1378585 http://www.cl.cam.ac.uk/~ejk39/papers/dreadlocks-spaa08.pdf)
 *
 * \anchor REFHASH 
 * \htmlonly
 * <b><font color=#B2222 size=5> Cuckoo Hashing
 * </font> </b>
 * \endhtmlonly
 *
 * This paper describes cuckoo hashing, a variation of which
 * is used by the storage manager's buffer manager.
 * \section P1 [P1]
 * R. Pagh 
 * "Cuckoo Hashing for Undergraduates",
 * Lecture note, IT University of Copenhagen, 2006
 * (http://www.it-c.dk/people/pagh/papers/cuckoo-undergrad.pdf)
 *
 * See also
 * R. Pagh, F. F. Rodler
 * "Cuckoo Hashing" 
 * in Procedings of the 9th Annual European Symposium on Algorithms,
* p. 121-133, August 28-31, 2001, ISBN 3-540-42493-8
 *
 *
 * \anchor REFARIES 
 * \htmlonly
 * <b><font color=#B2222 size=5> ARIES Recovery
 * </font> </b>
 * \endhtmlonly
 *
 * Many papers fall under the topic "ARIES"; this is an early
 * paper that describes the logging and recovery.
 *
 *\section MHLPS [MHLPS]
 * C. Mohan, D. Haderle, B. Lindsay, H. Parahesh, P. Schwarz,
 * "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking
 * and Partial Rollbacks Using Write-Ahead Logging"
 * IBM Almaden Reserch Center Technical Report RJ6649, Revised 11/2/90
 * Also in
 * ACM Transactions on Database Systems (TODS) Volume 17, Issue 1 (March 1992), pp. 94-162
 * (http://doi.acm.org/10.1145/128765.128770)
 *
 *\section BKSS [BKSS]
 * N. Beckmenn, H.P. Kriegel, R. Schneider, B. Seeger, 
 * "The R*-Tree: An Efficient and Robust Access Method for Points and Rectangles"
 * in Proc. ACM SIGMOD International Conference on Management of Data, 1990, pp. 322-331.
 * (http://doi.acm.org/10.1145/93597.98741)
 *
 * \anchor REFBTREE 
 * \htmlonly
 * <b><font color=#B2222 size=5> B+ - Tree Indexes
 * </font> </b>
 * \endhtmlonly
 *
 * This describes the key-value locking and index-management locking,
 * as well as the details of how logging and recovery are handled for
 * B+-Trees in ARIES.
 *
 *\section MOH1 [MOH1]
 * C. Mohan
 * "Concurrency Control and Recovery Methods for B+-Tree Indexes: ARIES/KVL and ARIES/IM"
 * IBM Almaden Reserch Center Technical Report RJ9715, March 1, 1994
 *
 * \anchor REFHUGEPAGE1 
 * \htmlonly
 * <b><font color=#B2222 size=5> Huge Pages (hugetlbfs) </font> </b>
 * \endhtmlonly
 *
 * If you have RHEL kernel documentation installed, see:
 * /usr/share/doc/kernel-doc-\<version\>/Documentation/vm/hugetlbpage.txt
 *
 *\section RHEL1 [RHEL1]
 *   http://linux.web.cern.ch/linux/scientific5/docs/rhel/RHELTuningandOptimizationforOracleV11.pdf for information about using huge pages with
 *   Linux (this is for Red Hat Linux).
 *
 *\section LINSYB1 [LINSYB1]
 * http://www.cyberciti.biz/tips/linux-hugetlbfs-and-mysql-performance.html for information about using hugetlbfs for SyBase.
 *
 *\section LINKER1 [LINKER1]
 * http://lxr.linux.no/source/Documentation/vm/hugetlbpage.txt  for information about Linux kernel support for hugetlbfs.
 *

 * \anchor REFMISCSHOREPAPERS 
 * \htmlonly
 * <b><font color=#B2222 size=5> Other Work Using the SHORE Storage Manager
 * </font> </b>
 * \endhtmlonly
 *
 * This section contains a small subset of the published papers whose work used the SHORE storage manager. 
 *
 * \section JPA2 [JPA2]
 * R. Johnson, I. Pandis, A. Ailamaki
 * "Improving OLTP Scalability Using Speculative Lock Inheritance"
 * in Proceedings of the VLDB Endowment, Volume 2, Issue 1 August 2009
 * (http://www.cs.cmu.edu/~ipandis/resources/vldb09johnson.pdf)
 *
 * \section JPSAA1 [JPSAA1]
 * R. Johnson, I. Pandis, R. Stoica, M. Athanassoulis, A. Ailamaki
 * "Aether: A Scalable Approach to Logging"
 * in Proceedings of the 36th International Conference on Very Large Data Bases,
 * Singapore,
 * Sept 13-17, 2010.
 * and in Proceedings of the VLDB Endowment, Volume 3, Issue 1. 
 * (http://www.cs.cmu.edu/~ipandis/resources/vldb10aether.pdf)
 *
 * \section JASA [JASA1]
 * R. Johnson, M. Athanassoulis, R. Stoica, A. Ailamaki
 * "A New Look at the Roles of Spinning and Blocking"
 * in Proceedings of the 5th International Workshop on Data Management on New Hardware,
 * Providence, Rhode Island, June 28, 2009, pp. 21-26
 * (http://doi.acm.org/10.1145/1565694.1565700)
 *
 * \section HAMS [HAMS1]
 * S. Harizopoulos, D. Abadi, S. Madden, M. Stonebraker
 * "OLTP Through the Looking Glass, and What We Found There"
 * in Proceedings of SIGMOD '08, Vancouver, BC, Canada, June 9-12, 2008
 * (http://doi.acm.org/10.1145/11376616.1376713)
 *
 * \section RDS [RDS1]
 * R. Ramamurthy, D. DeWitt, Q. Su
 * "A Case for Fractured Mirrors"
 * in VLDB Journal, Volume 12, Issue 1, pp. 89-101, August 2003
 * (http://dx.doi.org/10.1007/s00778-003-0093-1)
 *
 * \section KFDA [KFDA1]
 * D. Kossmann, M.J. Franklin, G. Drasch, Wig Ag
 * "Cache Investment: Integrating Query Optimization and Distributed Data Placement"
 * in ACM Transactions on Database Systems (TODS), Volume 25, Issue 4, December 2000
 * (http://doi.acm.org/10.1145/377673.377677)
 *
 * \section KFDA [KFDA1]
 * Y. Chen, J.M. Patel
 * "Design and Evaluation of Trajectory Join Algorithms"
 * in ACM GIS '09, Seattle, Washington, November 4-6, 2009
 * (http://doi.acm.org/10.1145/1653771.1653809)
 *
 *
 * \section CZZSS1 [CZZSS1]
 * Z. Chen, Y. Zhang, Y. Zhou, H. Scott, B. Shiefer
 * "Empirical Evaluation of Multi-level Buffer Cache Collaboration for Storage Systems"
 * in Proceedings of the 2005 ACM SIGMETRICS International Conference on Measurement and Modeling of Computer Systems,
 * Banff, Alberta, Canada,
 * June 6-10 2005,
 * pp. 145-156
 * (http://doi.acm.org/10.1145/1064212.1064230)
 *
 * \section MWSZG1 [MWSZG1]
 * M.P. Mesnier, M. Wachs, R.R. Sambasivan, A.Z. Zheng, G.R. Ganger
 * "Modeling the Relative Fitness of Storage",
 * in Proceedings of the 2007 ACM SIGMETRICS International Conference on Measurement and Modeling of Computer Systems,
 * Best Paper Award,
 * San Diego, CA,
 * June 12-16 2007,
 * pp 37-48
 * (http://doi.acm.org/10.1145/1254882.1254887)
 *
 * \section PCC1 [PCC1]
 * J.M. Patel, Y. Chen, V.P. Chakka,
 * "STRIPES: An Efficient Index for Predicted Trajectories"
 * in Proceedings of the 2004 ACM SIGMOD International Conference on Management of Data,
 * Paris, France,
 * June 13-18 2004,
 * (http://doi.acm.org/10.1145/1007568.1007639)
 * pp. 635-646
 *
 * \section ADH1 [ADH1]
 * A. Ailamaki, D.J.DeWitt, M.D.Hill,
 * "Data Page Layouts For Relations Databases on Deep Memory Hierarchies",
 * in VLDB Journal, Volume 11, Issue 3, November 2002, pp. 198-215
 * (http://dx.doi.org/10.1007/s00778-002-0074-9)
 */
