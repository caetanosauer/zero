#!/bin/bash

#@file:   slirun.sh 
#
#@brief:  Does a dbx run for SLI
#@note:   Assumes that the db is already populated
#
#@author: Ryan Johnson

# args: <dir> <name> <xctid> <sf> <length> <iterations> <warmup_mix>

if [ $# -lt 7 ]; then
    echo "Invalid args: $0 $*" >&2
    echo "Usage: pass this script as a parameter to the run wrapper" >&2
    echo -e "\twith extra arguments <sf> <measure-time> <measure-iters> <warmup_mix>" >&2
    exit 1
fi


DIR=$1; shift
NAME=$1; shift
XCT=$1; shift
SF=$1; shift
TIME=$1; shift
ITER=$1; shift
WARMUP_MIX=$1; shift

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
command run
command collector disable
command collector archive off # we'll manually link in later...

### kit

### dbx
#command assign signal_debugger_when_done=1
command assign show_sli_lock_info=1
command cont

# this sleep needs to be long enough to load the db!
#sleep 60
# (ip) - no db loading
sleep 5

### kit
command measure $SF 0 10 10 $WARMUP_MIX 3
command break
command collector disable

run_one ()
{
    # <clients> <sli:[0|1]>
    CLIENTS=$1
    SLI=$2
    if [ "$SLI" -eq 0 ]; then TYPE=base; else TYPE=sli; fi
    
    EXPNAME=$TYPE-$NAME-$XCT-$(printf "%02d" $CLIENTS)cl.100.er
### dbx
    command collector store experiment $EXPNAME
    command collector enable
    command cont
    # make sure to get all the measurements before continuing!
    sleep $((30+TIME*ITER))
    
    ### kit
    command sli-enable $SLI
    command measure $SF 0 $CLIENTS $TIME $XCT $ITER
    command break
    command collector disable
    # link in the archive from the base experiment
    (cd $DIR/$EXPNAME; rm -r archives; ln -s ../$START_EXP/archives)
}


# tm1-sli sequence
CLIENT_SEQ=(1 3 7 15 23 31 35 39 43 47 51 55 59 63)

# test
#CLIENT_SEQ=(1 3)

for i in ${CLIENT_SEQ[@]} do
    run_one $i 0
    run_one $i 1
done

### dbx
command kill
command exit
exit

#while read value; do
#    command $value
#done



    