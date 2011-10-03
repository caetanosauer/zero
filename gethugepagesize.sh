#!/bin/sh

dir=/proc
meminfo=$dir/meminfo

if test -d $dir; then
    if test -f $meminfo; then
        x=`grep "Hugepagesize" $meminfo  | sed -e "s/[^0123456789]//g"`
	if test -z $x; then 
	    echo 0
        else
	    echo $x
        fi
    else
	echo 0
    fi
else
    echo 0
fi
