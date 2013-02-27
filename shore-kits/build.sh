#!/bin/zsh

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

echo "Syncing shore-kits at ${DST_DIR}"

rsync -r ${SRC_DIR}/shore-kits ${DST_DIR}

cd ${DST_DIR}/shore-kits

if [ ! -f "${DST_DIR}/shore-kits/configure" ]
then
  ./autogen.sh
  ./configure --enable-shore6 --enable-debug SHORE_HOME=${BUILD_OPT}/Zero
fi

echo "Building shore-kits at ${DST_DIR}"
pwd

#make
