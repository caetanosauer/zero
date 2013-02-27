#!/bin/bash

#@file:   scripts/post_process_dbx.sh 
#@brief:  Does all the post processing for a DBX run
#@author: Ippokratis Pandis

# Usage example:
# for Baseline:  ../scripts/post_process_dbx.sh run-base-tm1-.. base-tm1 0 base-tm1-..all
# for DORA:      ../scripts/post_process_dbx.sh run-dora-tm1-.. dora-tm1 1 dora-tm1-..all

# args: $0 <DBX-DUMP-FILE> <PROFILER-DATA-DIR> <IS_DORA>
if [ $# -lt 3 ]; then
    echo "Usage: $0 <profiler-data-dir> <workload> <is_dora> <dbx-dump-file> "
    echo "          <is_dora>: 0/1"
    echo " "
    echo "Example:"
    echo "For Baseline:  ../scripts/post_process_dbx.sh run-base-tm1-.. base-tm1 0 base-tm1-..all"
    echo "For DORA:      ../scripts/post_process_dbx.sh run-dora-tm1-.. dora-tm1 1 dora-tm1-..all"
    echo " "
    exit 1
fi

PROFILERDATADIR=$1
WORKLOAD=$2
ISDORA=$3
DBXDUMPFILE=$4

echo 
echo PROFILERDATADIR: $PROFILERDATADIR
echo WORKLOAD:        $WORKLOAD
echo ISDORA:          $ISDORA
echo DBXDUMPFILE:     $DBXDUMPFILE
echo 

# ## Runs the filters and prints out everything to dbx-dumpfile ($DBXDUMPFILE)

# #for Baseline: find . -name "*.er" | grep -v startup | sort | xargs -n 1 ../scripts/er_filter_dora.sh 0

# echo "find $PROFILERDATADIR -name "*.er" | grep -v startup | sort |xargs -n 1 ../scripts/er_filter_dora.sh $ISDORA | tee $DBXDUMPFILE"

# find $PROFILERDATADIR -name "*.er" | grep -v startup | sort | tee $DBXDUMPFILE


## Greps only the Totals for each filter to ($ONLYTOTALS)

ONLYTOTALS=$DBXDUMPFILE.onlytotal

cat $DBXDUMPFILE | ggrep -e "Total" -e $WORKLOAD | tee $ONLYTOTALS



# # Prints Throughput to .power

POWER=$DBXDUMPFILE.power

echo "Throughput" | tee $POWER

echo "find $PROFILERDATADIR -name \"run.out\" | xargs -n 1 cat | ggrep -e \"MQTh\" -e \"TPS\" | sed 's/^.*(//' | sed -e 's/)$//' | tee -a $POWER"
find $PROFILERDATADIR -name "run.out" | xargs -n 1 cat | ggrep -e "MQTh" -e "TPS" | sed 's/^.*(//' | sed -e 's/)$//' | tee -a $POWER


# # Prints AvgCPU to .power

echo "AvgCPU" | tee -a $POWER

echo "find $PROFILERDATADIR -name \"run.out\" | xargs -n 1 cat | ggrep -e \"AvgCPU\"  | sed 's/^.*(//' | sed -e 's/.$//' | sed -e 's/%$//' | tee -a $POWER"
find $PROFILERDATADIR -name "run.out" | xargs -n 1 cat | ggrep -e "AvgCPU"  | sed 's/^.*(//' | sed -e 's/.$//' | sed -e 's/%$//' | tee -a $POWER



# # Prints CpuLoad to .power

echo "CpuLoad" | tee -a $POWER

echo "find $PROFILERDATADIR -name \"run.out\" | xargs -n 1 cat | ggrep -e \"CpuLoad\"  | sed 's/^.*(//' | sed -e 's/.$//' | sed -e 's/%$//' | tee -a $POWER"
find $PROFILERDATADIR -name "run.out" | xargs -n 1 cat | ggrep -e "CpuLoad"  | sed 's/^.*(//' | sed -e 's/.$//' | sed -e 's/%$//' | tee -a $POWER

