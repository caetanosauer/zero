#!/bin/bash

DATFILE=$1

# latching
NOLIST=_1cHsdesc
COUNT=0
WHAT[$COUNT]=Latching
YES[$COUNT]=__1cHlatch_t
NO[$COUNT]=


# locking
NOLIST="$NOLIST ${YES[$COUNT]}"
((COUNT++))
WHAT[$COUNT]=Locking
YES[$COUNT]=__1cGlock_m
NO[$COUNT]=$NOLIST

# logging
NOLIST="$NOLIST ${YES[$COUNT]}"
((COUNT++))
WHAT[$COUNT]=Logging
YES[$COUNT]="__1cIlog_core __1cFlog_m"
NO[$COUNT]=$NOLIST

# xct
NOLIST="$NOLIST ${YES[$COUNT]}"
((COUNT++))
WHAT[$COUNT]=TxMgt
YES[$COUNT]=__1cFxct_t
NO[$COUNT]=$NOLIST

# bpool
NOLIST="$NOLIST ${YES[$COUNT]}"
((COUNT++))
WHAT[$COUNT]=BPool
YES[$COUNT]=__1cEbf_m
NO[$COUNT]=$NOLIST

# btree
NOLIST="$NOLIST ${YES[$COUNT]}"
((COUNT++))
WHAT[$COUNT]=B+Tree
YES[$COUNT]=__1cHbtree_m
NO[$COUNT]=$NOLIST

# SM
NOLIST="$NOLIST ${YES[$COUNT]}"
((COUNT++))
WHAT[$COUNT]=SM
YES[$COUNT]=__1cEss_m
NO[$COUNT]=$NOLIST

# DORA
NOLIST="$NOLIST ${YES[$COUNT]}"
((COUNT++))
WHAT[$COUNT]=DORA
YES[$COUNT]=__1cEdora
NO[$COUNT]=$NOLIST

# kits
NOLIST="$NOLIST ${YES[$COUNT]}"
((COUNT++))
WHAT[$COUNT]=Kits
YES[$COUNT]=
NO[$COUNT]=$NOLIST

# grand total
NOLIST="$NOLIST ${YES[$COUNT]}"
((COUNT++))
WHAT[$COUNT]=TOTAL
YES[$COUNT]=
NO[$COUNT]="sdesc"

function filter() {
    flag=$1
    shift
    val=$(echo $* | sed 's; [ ]*;\\|;g')
    if [ -n "$val" ]; then
	echo "ggrep $flag ' \($val\)'"
    else
	echo cat
    fi
}

for ((i=0; i <= COUNT; i++)); do
    yes=$(filter -e ${YES[$i]})
    no=$(filter -v ${NO[$i]})
    CMD="cat $DATFILE | grep ACTIVE | $yes | $no | dtopk | head -n1 | awk '{print \$1}'"
    echo "$CMD" >&2
    echo -n "${WHAT[$i]} "
    bash -c "$CMD"
done