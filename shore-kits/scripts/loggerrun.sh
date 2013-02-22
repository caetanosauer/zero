#!/bin/bash

#@file:   loggerrun.sh 
#
#@brief:  Does a dbx run for the LOGGER
#
#@author: Ryan Johnson
#@author: Ippokratis Pandis

# Modified for Logger.
# - It does not spread threads (SF==CLIENTS)
# - It gets the logger mode as input

# args: <dir> <dbname> <xctid> <sf> <length> <iterations> <warmup_mix> <sleeptime> <clientset> <logtype>
#
# example:
# ./scripts/loggerrun.sh EXPDBX/FLUSHER tm1-100 25 100 30 3 25 360 large 0

if [ $# -lt 10 ]; then
    echo "Invalid args: $0 $*" >&2
    echo "Usage: $0 <dir> <dbname> <xctid> <sf> <measure-time> <measure-iters> <warmup-trx> <sleeptime> <clientset> <logtype>" >&2
    echo " " >&2
    echo "Wrapper script does not pass: <sf> <measure-time> <measure-iters> <warmup-trx>" >&2
    exit 1
fi


DIR=$1; shift
SELECTEDDB=$1; shift
XCT=$1; shift
SF=$1; shift
TIME=$1; shift
ITER=$1; shift
WARMUP_MIX=$1; shift
SLEEPTIME=$1; shift
CLIENTSET=$1; shift
LOGTYPE=$1; shift


command()
{
    echo echo $@
    echo $@
}

### dbx
START_EXP=startup.100.er
command collector archive copy
command collector store directory $DIR
command collector store experiment $START_EXP
command collector enable
command stop in main
command run -c $SELECTEDDB
command collector disable
command collector archive off # we'll manually link in later...

### kit

### dbx
#command assign signal_debugger_when_done=1
#command assign show_sli_lock_info=0
command cont

# instead of warmup we set clobberdev = 1 (load a new database each time) 
# and sleep for the:
# TM1 : 6mins  (100sf)
# sleep 360
# TPCC: 20mins (100wh)
#sleep 1200
# TPCC: 10mins (50wh)
# sleep 600
# TPCB: 4mins  (100sf)
#sleep 250

sleep $SLEEPTIME


# ### kit
# command elr
# command sli
# command restart

#command measure $SF 0 10 30 $WARMUP_MIX 1
command break
command collector disable

run_one ()
{
    # <clients>>
    CLIENTS=$1
    
    EXPNAME=$SELECTEDDB-$XCT-$(printf "%02d" $CLIENTS)cl.$LOGTYPE.100.er

    ### dbx
    command collector store experiment $EXPNAME
    command collector enable
    command cont
    # make sure to get all the measurements before continuing!
    sleep $((60+TIME*ITER))
    sleep $CLIENTS
    
    ### kit
    command log $LOGTYPE

    ## NO-SPREAD
    #command measure $CLIENTS 0 $CLIENTS $TIME $XCT $ITER
    ## SPREAD
    command measure $CLIENTS 1 $CLIENTS $TIME $XCT $ITER

    command break
    command collector disable
    # link in the archive from the base experiment
    (cd $DIR/$EXPNAME; rm -r archives; ln -s ../$START_EXP/archives)
}


## Default
CLIENT_SEQ=(1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 62 64 68)


## Special
if [ "$CLIENTSET" = "special" ]; then
    CLIENT_SEQ=(44 48 52 56 60 62 64)
fi

## Small
if [ "$CLIENTSET" = "small" ]; then
    CLIENT_SEQ=(1 4 8 12 16 20 24 28 32 36 40 44)
fi

## Medium
if [ "$CLIENTSET" = "medium" ]; then
    CLIENT_SEQ=(1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60)
fi

## Large
if [ "$CLIENTSET" = "large" ]; then
    CLIENT_SEQ=(1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 62 64 68)
fi

## ExtraLarge
if [ "$CLIENTSET" = "extralarge" ]; then
    CLIENT_SEQ=(1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64 68 74 78 82 86 90 94 98)
fi


for i in ${CLIENT_SEQ[@]}; do
    run_one $i
done

### dbx
command cont
sleep 1

### kit
command quit


### dbx
command kill
command exit
exit

