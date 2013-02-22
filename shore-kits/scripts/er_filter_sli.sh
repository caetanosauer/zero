#!/bin/bash

#@file:   er_filter_sli.sh 
#@brief:  Post-processing script for the output of dbx, for SLI 
#@author: Ryan Johnson

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
BASE=$(in_stack ".*run_one.*")
LM=$(in_stack "lock_m.*")

ATOMIC=$(is_leaf "atomic[^:]*")
MEMBAR=$(is_leaf "membar.*")
PPMCS=$(is_leaf "ppmcs.*acquire.*")
OCC_RWLOCK=$(is_leaf "occ_rwlock.*")
SPIN=$(is_leaf ".*lock.*spin.*")
MUTEX=$(is_leaf "mutex.*lock.*")
SLI=$(in_stack ".*sli_.*")

CONTENTION=$(parenthize "$ATOMIC || $PPMCS || $SPIN || $OCC_RWLOCK || $MUTEX")

er_print -limit 5 \
    -filters "$BASE" -functions \
    -filters "$BASE && $LM" -functions \
    -filters "$BASE && $CONTENTION" -functions \
    -filters "$BASE && $LM && $CONTENTION" -functions \
    -filters "$BASE && $SLI" -functions \
    -filters "$BASE && $SLI && $CONTENTION" -functions \
    $@