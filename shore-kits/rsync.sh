#!/bin/zsh

USAGE="Usage: $0 [DST_DIR]"

if [ $# -lt 1 ]
then
  if [ -z "${BUILD_OPT}" ]
  then
    echo "Error: BUILD_OPT not defined"
    exit
  fi 
  DST_DIR=${BUILD_OPT}/Zero
else
  DST_DIR=$1
fi

SRC_DIR=/home/`whoami`/projects/Zero

echo "Syncing ${SRC_DIR}/shore-kits to ${DST_DIR}"

rsync -r ${SRC_DIR}/shore-kits ${DST_DIR}
