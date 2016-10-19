/*
 * (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
 */

This folder contains implementation of TPC-C benchmark for Foster B-trees.


== Quick Start

If you just want to run TPCC on the Foster B-trees, compile and run:

   mkdir -p /dev/shm/`whoami`/foster/log
  ./tpcc_load
  ./tpcc_full

tpcc_load is the data load program that creates TPCC databases and load data.
tpcc_full is the query workload program that randomly runs TPCC transactions.

  
== Making your own TPCC variant

    In many cases, you'd like to make your own query workload for your
specific focus, using just the TPCC data.  In that case, include the
tpcc.h from your experiments and also link to libtpcc.a in this folder.

    See tpcc_example.cpp/h in this folder for instance.  Another (more
complex) example is under ../okvl.


== For more details

    Use the "-h" option or read the comments.
