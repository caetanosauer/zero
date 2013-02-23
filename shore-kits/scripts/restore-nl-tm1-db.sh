#!/bin/sh
#
# @file:  restore-nl-tm1-db
# 
# @brief: Deletes the log file and the database of the TM1 NL <db> database

if [ $# -ne 1 ]; then
    echo "Usage: $0 <DB-SF>"
    exit 127
fi

sf="$1-nl"
logname="log-tm1-$1-nl/*"
dbname="databases/tm1-db-$1sf-nl"
gtarname="/export/home/ipandis/RAID/TARS/tm1db$1sf-nl.tar"


echo "+ Deleting ($logname)"
rm $logname


echo "+ Deleting ($dbname)"
rm $dbname

echo "+ Restoring from tar ($gtarname)"
gtar -xvf $gtarname

echo "+ DB ($sf) Restored"
