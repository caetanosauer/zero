#!/bin/bash

#@file:   scripts/powerwrapper.sh 
#@brief:  Ryns a single experiment, varying the number of clients. The user
#         specifies the space of the client values, from a set of predefined values.
#@author: Ippokratis Pandis

# typical usage: 
#
# DORA-TM1
# for ((trx=220; trx <= 226; trx++)); do ./scripts/powerwrapper.sh EXP tm1-100-nl 200 3 30 $trx 1 80 large cdme; done
# or
# SEQ=(224 226 200) ; for i in ${SEQ[@]}; do ./scripts/powerwrapper.sh EXP tm1-100-nl 200 3 30 $trx 1 80 large cdme; done


EXPSHELL="scripts/trx_powerrun.sh"
TRXSHELL="shore_kits"
SHORECONF="shore.conf"

# args: <base-dir> <selecteddb> <sleeptime> <iter> <time> <xctid> <low> <high> <clientset> <logtype>
if [ $# -lt 10 ]; then
    echo "Usage: $0 <base-dir> <selecteddb> <sleeptime> <iter> <time> <xctid> <low-cl> <high-cl> <clientset> <logtype>" >&2
    echo " " >&2
    echo "Examples:" >&2
    echo "BASE-TM1: for ((trx=20; trx <= 26; trx++)); do ./scripts/powerwrapper.sh EXP tm1-100 200 3 30 \$trx 1 80 large cdme; done" >&2
    echo "DORA-TM1: for ((trx=220; trx <= 226; trx++)); do ./scripts/powerwrapper.sh EXP tm1-100-nl 200 3 30 \$trx 1 80 large cdme; done" >&2
    echo "SEQ=(224 226 200) ; for trx in \${SEQ[@]}; do ./scripts/powerwrapper.sh EXP tm1-100 200 3 30 \$trx 1 80 large cdme; done" >&2
    echo " " >&2
    exit 1
fi

# The directory where the .out file will be written
BASE_DIR=$1; shift

# The sleeping time until the database is loaded depends on the selected database
SELECTEDDB=$1; shift
SLEEPTIME=$1; shift

# Number of iteration and duration of measurement
ITER=$1; shift
TIME=$1; shift

# The number of clients used and the clientset depend on the workload (xctid) 
XCT=$1; shift
LOW=$1; shift
HIGH=$1; shift
CLIENTSET=$1; shift


# Log type (for more info check src/sm/shore/shore_shell.cpp:922)
LOGTYPE=$1; shift 

# Set filename of the output
STAMP=$(date +"%F-%Hh%Mm%Ss")
OUTFILE=$BASE_DIR/perf-$SELECTEDDB-$XCT.$LOGTYPE.$STAMP.out
mkdir -p $BASE_DIR

# Remove old log
#echo rm -rf log/*
#rm -rf log/*
echo rm log/*
rm log/*

# Delete all databases !
echo rm databases/*
rm databases/*

# Run !
CMD="source $EXPSHELL $LOW $HIGH $XCT $TIME $ITER $SLEEPTIME $CLIENTSET $LOGTYPE | $TRXSHELL -c $SELECTEDDB 2>&1 | tee $OUTFILE"
echo "$CMD" | tee $OUTFILE
echo "Configuration" | tee -a $OUTFILE
cat shore.conf | grep "^$SELECTEDDB" | tee -a $OUTFILE 

## Clobbers the db
($EXPSHELL $LOW $HIGH $XCT $TIME $ITER $SLEEPTIME $CLIENTSET $LOGTYPE | $TRXSHELL -c $SELECTEDDB -d) 2>&1 | tee -a $OUTFILE

