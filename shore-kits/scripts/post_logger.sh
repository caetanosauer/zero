#!/bin/bash

#@file:   scripts/post_logger.sh 
#@brief:  Post-processing output of powerwrapper for the logger-related runs
#@author: Ippokratis Pandis

# args: <file>
if [ $# -lt 1 ]; then
    echo "Usage: $0 <file>" >&2
    exit 1
fi

EXPFILE=$1; shift

FILTERFILE=filter.`basename $EXPFILE`
echo $FILTERFILE

### TPCC/TPCB have TPS, while TM1 has MQTh
echo "++ Throughput" | tee $FILTERFILE
cat $EXPFILE | ggrep -e "TPS" -e "MQTh" -e "measure" | grep -v "measurement" | sed 's/^.*(//' | sed -e 's/.$//' | tee -a $FILTERFILE

echo "++ AvgCPU" | tee -a $FILTERFILE
cat $EXPFILE | ggrep -e "AvgCPU" -e "measure" | grep -v "measurement" | sed 's/^.*(//' | sed -e 's/..$//' | tee -a $FILTERFILE

echo "++ CpuLoad" | tee -a $FILTERFILE
cat $EXPFILE | ggrep -e "CpuLoad" -e "measure" | grep -v "measurement" | sed 's/^.*(//' | sed -e 's/.$//' | tee -a $FILTERFILE

echo "++ VoluntaryCtx" | tee -a $FILTERFILE
cat $EXPFILE | ggrep -e "Voluntary" -e "measure" | grep -v "measurement" | sed 's/^.*\. //' | tee -a $FILTERFILE

echo "++ InvoluntaryCtx" | tee -a $FILTERFILE
cat $EXPFILE | ggrep -e "Involuntary" -e "measure" | grep -v "measurement" | sed 's/^.*\. //' | tee -a $FILTERFILE

echo "++ TotalUser" | tee -a $FILTERFILE
cat $EXPFILE | ggrep -e "Total User" -e "measure" | grep -v "measurement" | sed 's/^.*\. //' | tee -a $FILTERFILE

echo "++ TotalSystem" | tee -a $FILTERFILE
cat $EXPFILE | ggrep -e "Total System" -e "measure" | grep -v "measurement" | sed 's/^.*\. //' | tee -a $FILTERFILE

echo "++ OtherSystem" | tee -a $FILTERFILE
cat $EXPFILE | ggrep -e "Other System" -e "measure" | grep -v "measurement" | sed 's/^.*\. //' | tee -a $FILTERFILE


ROWIZEPERL="scripts/rowize.pl"
perl $ROWIZEPERL -f $FILTERFILE

rm $FILTERFILE
exit

