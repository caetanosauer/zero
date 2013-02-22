#!/bin/sh
#
# @file:  restore-nl-db
# 
# @brief: Deletes the log file and the database of the TPC-C NL <db> database

if [ $# -ne 1 ]; then
    echo "Usage: $0 <DB-SF>"
    exit 127
fi

sf="$1-nl"
logname="log$1-nl/*"
dbname="databases/shore$1wh-nl"
gtarname="/export/home/ipandis/RAID/TARS/shore$1wh-nl.tar"


echo "+ Deleting ($logname)"
rm $logname


echo "+ Deleting ($dbname)"
rm $dbname

echo "+ Restoring from tar ($gtarname)"
gtar -xvf $gtarname

echo "+ DB ($sf) Restored"
