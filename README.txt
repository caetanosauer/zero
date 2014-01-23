/*
 * (c) Copyright 2011-, Hewlett-Packard Development Company, LP.
 * HP Express (Zero) source code.
 */
README Last Updated: Jan 2014

==== Steps to build/test
https://twiki.hpl.hp.com/bin/view/SIMPL/DevelopmentHowTo

Repeat after me: WE ALL DO FULL BUILD AND RUN ALL TESTCASES BEFORE CHECKING IN.

==== Code Review
http://codereview.hpl.hp.com/dashboard/

==== Coding Convention
TODO

 
==== Module owners
Include module owners in code reviews whenever you touch them.
Even when your change is just one line, pay reasonable effort to communicate with
the owners BEFORE checking it in. If they are not available within reasonable time/effort,
you may commit and push, but you have to revert the change if the owners later insist to do so.
In that case, or when you have major changes that are appropriate for synchronous reviews,
follow the official design/code review steps.

== CMake scripts and general build mechanism
Example files: CMakelists.txt, folder structure, etc
Primary: Joe
Secondary: Hideaki

== Testcases and any test infrastructure we will have in future
Example files: Everything under tests.
Primary: ???
Secondary: All of us (for individual testcases)

== Bufferpool
Example files: bf_*.h/cpp
Primary: Haris
Secondary: Stan

== Latch and physical implementation of synchronization (mutex/spinlock/etc)
Example files: latches folder, latch.cpp/h, relevant code in common/fc/sthread (e.g., mcs_rwlock)
Primary: Joe
Secondary: Oleg

== Lock manager (including deadlock handling)
Example files: lock.h/cpp, lock_*.h/cpp
Primary: Hideaki
Secondary: Mark

== Logging – outputting logs (existing)
Example files: log*.h/cpp and relevant code in Btree, also the perl script for logdef.dat
Primary: Harumi and Charlie
Secondary: Haris and Goetz

== Logging – outputting logs (improved, modularized, API)
Example files: log*.h/cpp and relevant code in Btree
Primary: Haris and Charlie
Secondary: Stan, Harumi and Goetz

Note: This part is quite complex and currently messy. More eyes are needed.

== Logging – checkpoint and restart
Example files: restart*.h/cpp, log*.h/cpp and relevant code in Btree (recovery, undo, redo, restart etc)
Primary: Charlie and Stan
Secondary: Wey and Goetz

Note: Same as above. Obviously the owners of the two logging aspects must be in close sync.

== Transaction classes
Example files: xct.cpp/xct.h
Primary: Hideaki
Secondary: Charlie

== Page formats, core Btree storage classes
Example files: *_page.h/cpp
Primary: Mark
Secondary: Joe

== BTree verification
Example files: btree_impl_verify.cpp, btree_verify.h, etc
Primary: Harumi
Secondary: Stan

== All other Btree codes
Example files: btree_impl_search and btcursor.h/cpp
Primary: Hideaki 
Secondary: Mark

Example files: split/ grow / merge / shrink
Primary: Hideaki
Secondary: Charlie

Example files: defrag.cpp and btree utilities
Primary: Hideaki
Secondary: first person to modify

== Standard benchmarks
Example files: TPC-B, TPC-C, experiments/*
Primary: Hideaki
Secondary: Stan

== All other Shore-MT remnants, including thread managements
Example files: many under common/fc/sthread
Primary: Joe
Secondary: Hideaki


