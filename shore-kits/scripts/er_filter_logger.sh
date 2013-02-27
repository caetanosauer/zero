#!/bin/bash

#@file:   er_filter_logger.sh 
#@brief:  Post-processing script for the output of dbx, for Logger 
#
#@author: Ryan Johnson
#@author: Ippokratis Pandis
#
#@note:   Typical usage to a directory with .er files from dbxwrapper:
#         find . -name "*.er" | grep -v startup | sort | xargs -n 1 ../scripts/er_filter_logger.sh 0 
#

# args: <is_dora> <er file>
if [ $# -lt 2 ]; then
    echo "Usage: $0 <is_dora> <er file>" >&2
    echo "       if is_dora==1 parses also for DORA component" >&2
    exit 1
fi

in_stack()
{
    echo '(fname("'"$1"'") some in stack)'
}
is_leaf()
{
    echo '(leaf in fname("'"$1"'"))'
}
parenthize()
{
    echo '('"$*"')'
}

# The population
POPULATE=$(in_stack ".*populate.*")

# All server side + begin_xct
BEGIN=$(in_stack ".*begin_xct.*")
##SERVE=$(in_stack ".*_serve_action.*")
SERVE=$(in_stack ".*_work_ACTIVE_impl.*")

BASE=$(parenthize "$BEGIN || $SERVE")


# Components in graphs
LM=$(in_stack ".*lock_m::.*")
LOGM=$(in_stack ".*log.*")


# Contention
ATOMIC=$(is_leaf "atomic[^:]*")
PPMCS=$(is_leaf "ppmcs.*acquire.*")
OCC_RWLOCK=$(is_leaf "occ_rwlock.*")
SPIN=$(is_leaf ".*lock.*spin.*")
MUTEX=$(is_leaf "mutex.*lock.*")


POLYLOCK=$(in_stack ".*poly_lock.*")
CAWAIT=$(in_stack ".*ringbuf.*wait_for_.*")
CAJOIN=$(in_stack ".*ringbuf.*join_slot.*")
TRUEMCS=$(is_leaf "true_mcs::.*")

CONTENTION=$(parenthize "$ATOMIC || $PPMCS || $SPIN || $OCC_RWLOCK || $MUTEX || $POLYLOCK || $CAWAIT || $CAJOIN || $TRUEMCS")

# DORA
DORA=$(is_leaf ".*dora.*")
# when looking for Dora's component we have to filter out the dora*trx_exec 
# because this is part of the (actual) transaction execution
DORA_TRX_EXEC=$(is_leaf ".*dora.*trx_exec.*")


echo " "
echo " "
echo " "
echo " "

date +"%r"
echo "$2"

if [ $1 -eq "1" ]; then
    echo "DORA PARSING" >&2
else
    echo "BASELINE PARSING" >&2
fi

echo " "
echo " "
echo " "
echo " "


# echo "$BASE"
# echo "$BASE && $CONTENTION"
# echo "$BASE && $LM"
# echo "$BASE && $LM && $CONTENTION"
# echo "$BASE && $LOGM"
# echo "$BASE && $LOGM && $CONTENTION"
# echo "$BASE && $DORA && !$DORA_TRX_EXEC"

if [ $1 -eq "1" ]; then
#     er_print -limit 5 \
#         -filters "$BASE" -functions \
#         -filters "$BASE && $LM" -functions \
#         -filters "$BASE && $DORA && !$DORA_TRX_EXEC" -functions \
#         $2
    er_print -limit 5 \
        -filters "$BASE" -functions \
        -filters "$BASE && $CONTENTION" -functions \
        -filters "$BASE && $LOGM" -functions \
        -filters "$BASE && $LOGM && $CONTENTION" -functions \
        -filters "$BASE && $DORA && !$DORA_TRX_EXEC" -functions \
        $2
else
    er_print -limit 5 \
        -filters "$BASE" -functions \
        -filters "$BASE && $CONTENTION" -functions \
        -filters "$BASE && $LOGM" -functions \
        -filters "$BASE && $LOGM && $CONTENTION" -functions \
        $2
fi

