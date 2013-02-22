#!/bin/sh
#
# @file:  restore-base-db
# 
# @brief: Deletes the log file and the database of the TPC-C BASELINE <db> database

if [ $# -ne 1 ]; then
    echo "Usage: $0 <DB-SF>"
    exit 127
fi


sf="$1"
logname="log$1/*"
dbname="databases/shore$1wh"
gtarname="/export/home/ipandis/RAID/TARS/shore$1wh.tar"


echo "+ Deleting ($logname)"
rm $logname


echo "+ Deleting ($dbname)"
rm $dbname

echo "+ Restoring from tar ($gtarname)"
gtar -xvf $gtarname

echo "+ DB ($sf) Restored"
