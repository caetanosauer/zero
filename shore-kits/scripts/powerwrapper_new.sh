#!/bin/bash

#@file:   scripts/powerwrapper_new.sh 
#@brief:  Runs a single experiment, varying the number of clients. The user
#         specifies the space of the client values, from a set of predefined values.
#@author: Ippokratis Pandis
#@author: Pinar Tozun




EXPSHELL="scripts/trx_powerrun_new.sh"
TRXSHELL="./shore_kits"
SHORECONF="shore.conf"

# args: <base-dir> <selecteddb> <sleeptime> <iter> <time> <xctid> <low> <high> <clientset> <logtype> <design> <type> <sli> <elr> <hack> 
if [ $# -lt 15 ]; then
    echo "Usage: $0 <base-dir> <selecteddb> <sleeptime> <iter> <time> <xctid> <low-cl> <high-cl> <clientset> <logtype> <design> <type> <sli> <elr> <hack>" >&2
    echo " " >&2
    echo "Examples:" >&2
    echo "BASE-TM1: for ((trx=20; trx <= 26; trx++)); do ./scripts/powerwrapper_new.sh EXP tm1-100 200 3 30 \$trx 1 80 large cdme baseline normal sli elr p; done" >&2
    echo "DORA-TM1: for ((trx=220; trx <= 226; trx++)); do ./scripts/powerwrapper_new.sh EXP tm1-100 200 3 30 \$trx 1 80 large cdme dora normal p elr p; done" >&2
    echo "SEQ=(224 226 200) ; for trx in \${SEQ[@]}; do ./scripts/powerwrapper_new.sh EXP tm1-100 200 3 30 \$trx 1 80 large cdme plp mrbtnorm p elr p; done" >&2
    echo " " >&2
    exit 1
fi

# <design> -> baseline/dora/plp
# <type> -> normal/mrbtnorm/mrbtleaf/mrbtpart
# <sli>,<elr>,<hack> -> sli,elr,x respectively (if you want sli,elr,hacks to be enabled, otherwise write something dummy that is not a command for the database promt) 

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

# 
DESIGN=$1; shift
TYPE=$1; shift
SLI=$1; shift
ELR=$1; shift
HACK=$1; shift

# Set filename of the output
STAMP=$(date +"%F-%Hh%Mm%Ss")
OUTFILE=$BASE_DIR/perf-$SELECTEDDB-$DESIGN-$SLI-$ELR-$HACK-$XCT.$LOGTYPE.$STAMP.out
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
CMD="source $EXPSHELL $LOW $HIGH $XCT $TIME $ITER $SLEEPTIME $CLIENTSET $LOGTYPE $SLI $ELR | $TRXSHELL -c $SELECTEDDB 2>&1 | tee $OUTFILE"
echo "$CMD" | tee $OUTFILE
echo "Configuration" | tee -a $OUTFILE
cat shore.conf | grep "^$SELECTEDDB" | tee -a $OUTFILE 

## Clobbers the db
if [ "$HACK" = "x" ]; then
    echo "KITS CMD: $TRXSHELL -c $SELECTEDDB -s $DESIGN -d $TYPE -x -r" | tee -a OUTFILE
    ($EXPSHELL $LOW $HIGH $XCT $TIME $ITER $SLEEPTIME $CLIENTSET $LOGTYPE $SLI $ELR | $TRXSHELL -c $SELECTEDDB -s $DESIGN -d $TYPE -x -r) 2>&1 | tee -a $OUTFILE
else
    echo "KITS CMD: $TRXSHELL -c $SELECTEDDB -s $DESIGN -d $TYPE -r" | tee -a OUTFILE
    ($EXPSHELL $LOW $HIGH $XCT $TIME $ITER $SLEEPTIME $CLIENTSET $LOGTYPE $SLI $ELR | $TRXSHELL -c $SELECTEDDB -s $DESIGN -d $TYPE -r) 2>&1 | tee -a $OUTFILE
fi

