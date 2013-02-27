#!/bin/bash

#@file:   scripts/after_er_filter.sh
#@brief:  Gets as an input a filtered output (er_filter_dora <.er> <1/0> | tee run.all)
#@author: Ippokratis Pandis

# args: $0 <PROFILER-DATA-DIR>
if [ $# -lt 1 ]; then
    echo "Usage: $0 <PROFILER-DATA-DIR>"
    exit 1
fi

PROFILERDATADIR=$1

echo "Throughput"
find $PROFILERDATADIR -name "run.out" | xargs -n 1 cat | ggrep -e "MQTh" -e "TPS"  | sed 's/^.*(//' | awk 'NR % 3 != 1' | sed -e 's/)$//'

echo "AvgCPU"
find $PROFILERDATADIR -name "run.out" | xargs -n 1 cat | ggrep -e "AvgCPU"  | sed 's/^.*(//' | sed -e 's/.$//' | sed -e 's/%$//' | awk 'NR % 3 != 1'

echo "CpuLoad"
find $PROFILERDATADIR -name "run.out" | xargs -n 1 cat | ggrep -e "CpuL"  | sed 's/^.*(//' | sed -e 's/.$//' | sed -e 's/%$//' | awk 'NR % 3 != 1'

