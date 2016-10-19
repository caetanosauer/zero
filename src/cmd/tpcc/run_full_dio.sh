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
    for rep in 0 1 2 3 4 
#5 6 7 8 9
    do
        rm -rf /var/tmp/$usr/foster/data /tmp/$usr/foster/log
        cp -r /dev/shm/$usr/backup/data /var/tmp/$usr/foster/
        mkdir -p /tmp/$usr/foster/log
        numactl --interleave=all ./tpcc_full$3 $param --log-folder /tmp/$usr/foster/log --data-device /var/tmp/$usr/foster/data  > "$resfile.$rep" 
    done
}


usr=`whoami`
mkdir -p "/tmp/$usr/foster/log"
mkdir -p "/var/tmp/$usr/foster"
mkdir -p "/dev/shm/$usr/backup"


# load data once then save disk files for fast reuse:
echo "Creating TPC-C database image..."
./tpcc_load --nolock --log-folder /tmp/$usr/foster/log --data-device /var/tmp/$usr/foster/data
rm -rf /dev/shm/$usr/backup/*
cp -r /var/tmp/$usr/foster/data /dev/shm/$usr/backup/

echo "Running experiments..."
mkdir -p tpcc_results
rm -f tpcc_results/*
mkdir -p tpcc_results
for nolog in "" 
#"nolog"
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
