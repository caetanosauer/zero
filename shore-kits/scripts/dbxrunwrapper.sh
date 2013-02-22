#!/bin/bash

#@file:   dbxrunwrapper.sh 
#@brief:  Wrapper for the dbx runs. Creates/opens directories/files, pipes commands.
#@author: Ryan Johnson
#@author: Ippokratis Pandis

# typical usage: 
#
# SLI-TM1
# for ((trx=20; trx <= 26; trx++)); do ./scripts/dbxrunwrapper.sh sli tm1 $trx ./scripts/slirun.sh 10000 10 3 $trx 300 large; done
#
# DORA-TM1
# for ((trx=220; trx <= 226; trx++)); do ./scripts/dbxrunwrapper.sh exp tm1dora $trx ./scripts/dorarun.sh 100 30 3 $trx 300 large; done
# !! Note for DORA-TM1
# Cannot distinguish between DORA and BASELINE. Therefore, 
# (1) Edit shore.conf to point to the correct configuration (DORA or BASELINE)
# (2) Fire up test with different selected database (doratm1, basetm1)
#
# LOGGER-TM1
# ./scripts/dbxrunwrapper.sh EXPDBX/FLUSHER tm1-100 25 ./scripts/loggerrun.sh 100 30 3 360 large 0


# args: <output-dir> <selecteddb> <xctid> <run-script> [<run-script-args>]
#                                                      <sf> <duration> <iteration> <warmuptrx> <sleeptime> <clientset>
if [ $# -lt 4 ]; then
    echo "Usage: $0 <base-dir> <selecteddb> <xctid> <run-script> [args for run-script...]" >&2
    echo "                                                       <sf> <duration> <iterations> <warmuptrx> <sleeptime> <clientset>" >&2
    echo "The run-script will be passed the <base-dir> <selecteddb> <xctid> and any remaining args" >&2
    echo " " >&2
    echo "Examples:" >&2
    echo "BASE-TM1: SEQ=(24 26 20) ; for trx in \${SEQ[@]}; do ./scripts/dbxrunwrapper.sh EXPDBX tm1-100 \$trx ./scripts/dorarun.sh 100 30 3 \$trx 300 large ; done"
    echo "DORA-TM1: for ((trx=220; trx <= 226; trx++)); do ./scripts/dbxrunwrapper.sh EXPDBX tm1-100-nl \$trx ./scripts/dorarun.sh 100 30 3 \$trx 300 large ; done"
    echo " " >&2
    echo "!!! Make sure that shore.conf has the correct <selecteddb> for your experiment !!!" >&2
    echo " " >&2
    exit 1
fi

BASE_DIR=$1; shift
SELECTEDDB=$1; shift
XCT=$1; shift
SCRIPT=$1; shift

STAMP=$(date +"%F-%Hh%Mm%Ss")
DIR=$BASE_DIR/run-$SELECTEDDB-$XCT.$STAMP
OUTFILE=$DIR/run.out

TRXSHELL="shore_kits"
DBX="dbx"

mkdir -p $DIR

# remove log 
echo rm -rf log/*
rm -rf log/*

# # remove databases !
rm databases/*


CMD="source $SCRIPT $DIR $SELECTEDDB $XCT $* | $DBX $TRXSHELL 2>&1 | tee $OUTFILE"
echo "$CMD" | tee $OUTFILE
($SCRIPT $DIR $SELECTEDDB $XCT $* | $DBX $TRXSHELL) 2>&1 | tee -a $OUTFILE
