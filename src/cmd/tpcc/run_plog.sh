#!/bin/bash

##
## (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
##


cores=24
xct_per_core=20000

echo "Using cores: $cores"
echo "Transactions per core: $xct_per_core"


run_expr() {
    resfile="tpcc_results/res.$2"
    param="--workers $cores --transactions $xct_per_core --pin_numa --verbose_level=0"
    for rep in `seq 0 0`
    do
        rm -rf /dev/shm/$usr/foster/*
        cp -r /dev/shm/$usr/backup/* /dev/shm/$usr/foster/
        if [[ "$2" == "plog" ]]; then
            rm -rf /dev/shm/$usr/foster/clog
            mv /dev/shm/$usr/foster/log /dev/shm/$usr/foster/clog
        fi
        #numactl --interleave=all ./tpcc_full$1 $param  > "$resfile.$rep"
        ./tpcc_full$1 $param  > "$resfile.$rep"
        #gdb --args ./tpcc_full$1 $param
    done
}


usr=`whoami`
mkdir -p "/dev/shm/$usr/foster/log"
mkdir -p "/dev/shm/$usr/foster/clog"

# load data once then save disk files for fast reuse:
echo "Creating TPC-C database image..."
./tpcc_load --nolock --warehouses $cores
mkdir -p "/dev/shm/$usr/backup/"
rm -rf /dev/shm/$usr/backup/*
cp -r /dev/shm/$usr/foster/data /dev/shm/$usr/foster/log /dev/shm/$usr/foster/clog /dev/shm/$usr/backup/

echo "Running experiments..."
mkdir -p tpcc_results
rm -f tpcc_results/*
mkdir -p tpcc_results

echo "  running tpcc_full"
run_expr "" "baseline"

echo "  running tpcc_full_plog"
run_expr "_plog" "plog"


ruby summarize_results.rb $cores $xct_per_core > tpcc_results.csv
