#!/bin/zsh

USAGE="Usage: 
\t% $0 <command>
\t  deploy
\t  sync
\t  diff"

if [ -z "${BUILD_OPT}" ]
then
  echo "Error: BUILD_OPT not defined"
  exit
fi 

if [ $# -lt 1 ]
then
  echo ${USAGE}
  exit
fi

CMD=$1

BUILD_DIR=${BUILD_OPT}/Zero
SRC_DIR=/home/`whoami`/projects/Zero

if [ "${CMD}" = "deploy" ]
then
  cp -a ${SRC_DIR}/shore-kits ${BUILD_DIR}
fi

if [ "${CMD}" = "sync" ]
then
  rsync -r ${BUILD_DIR}/shore-kits ${SRC_DIR}
fi

if [ "${CMD}" = "diff" ]
then
  diff -r ${BUILD_DIR}/shore-kits ${SRC_DIR}/shore-kits
fi
