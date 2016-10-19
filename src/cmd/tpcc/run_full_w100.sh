#!/bin/sh

## 
## (c) Copyright 2014, Hewlett-Packard Development Company, LP
## 

# Same as run_full, but this uses 100 warehouses AND sizes up a few parameters accordingly.
# DO NOT RUN THIS SCRIPT IN A SMALL MACHINE. This eats up a lot of RAM and CPUs.

cores=18
xct_per_core=20000

echo "Using cores: $cores"
echo "Transactions per core: $xct_per_core"


run_expr() {
    resfile="tpcc_results/res.$1.$2.$3"
    param="--workers $cores --transactions $xct_per_core --pin_numa --verbose_level=0 --bufferpool_mb 13000 --max_log_mb 26000"
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
./tpcc_load --nolock --warehouses 100 --bufferpool_mb 13000 --max_log_mb 26000
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
