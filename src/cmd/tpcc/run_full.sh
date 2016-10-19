#!/bin/sh

## 
## (c) Copyright 2013-2014, Hewlett-Packard Development Company, LP
## 


cores=12
xct_per_core=20000

echo "Using cores: $cores"
echo "Transactions per core: $xct_per_core"


run_expr() {
    resfile="tpcc_results/res.$1.$2.$3"
    param="--workers $cores --transactions $xct_per_core --pin_numa --verbose_level=0"
    if [ -n "$1" ]; then
        param="$param --$1"
    fi
    if [ -n "$2" ]; then
        param="$param --$2"
    fi
    if [ "$3" = "_NOSWIZZLING" ]; then
        param="$param --noswizzling"
    fi
    for rep in 0 1 2 3 4 5 6 7 8 9
    do
        rm -rf /dev/shm/$usr/foster/data /dev/shm/$usr/foster/log
        cp -r /dev/shm/$usr/backup/data /dev/shm/$usr/backup/log /dev/shm/$usr/foster/
        numactl --interleave=all ./tpcc_full$3 $param  > "$resfile.$rep"
    done
}


usr=`whoami`
mkdir -p "/dev/shm/$usr/foster/log"

# load data once then save disk files for fast reuse:
echo "Creating TPC-C database image..."
./tpcc_load --nolock
mkdir -p "/dev/shm/$usr/backup/"
rm -rf /dev/shm/$usr/backup/*
cp -r /dev/shm/$usr/foster/data /dev/shm/$usr/foster/log /dev/shm/$usr/backup/

echo "Running experiments..."
mkdir -p tpcc_results
rm -f tpcc_results/*
mkdir -p tpcc_results
for nolog in "" "nolog"
do
    for nolock in "" "nolock"
    do
        for suffix in "" "_MAINMEMORY" "_NOSWIZZLING"
        do
	    echo "  running:  log:$nolog lock:$nolock type:$suffix"
            run_expr "$nolog" "$nolock" "$suffix"
        done
    done
done


ruby summarize_results.rb $cores $xct_per_core > tpcc_results.csv
