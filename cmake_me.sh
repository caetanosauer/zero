#!/bin/sh
# This is the only script written in native bash.
# All other stuffs should be implemented as CMake scripts.
# In non-bash environments (e.g., Windows), just do it manually.
echo "What's the build type? [d(Debug)/r(Release)/w(RelWithDebInfo)/t(Trace)]"
read TYPE

case $TYPE in
d)
  BUILD_ARG="Debug"
  BUILD_DIR="debug_build"
  ;;
w)
  BUILD_ARG="RelWithDebInfo"
  BUILD_DIR="relwithdebinfo_build"
  ;;
t)
  BUILD_ARG="Trace"
  BUILD_DIR="trace_build"
  ;;
r)
  BUILD_ARG="Release"
  BUILD_DIR="release_build"
  ;;
*)
  echo "Unexpected build type."
  exit
  ;;
esac

echo "Are you going to use SunCC on Sun Solaris? [y/*n] (*)default"
read SOLARIS

case $SOLARIS in
y)
  CC_ARG="-DCMAKE_CXX_COMPILER=/usr/bin/CC"
  ;;
*)
  CC_ARG=""
  ;;
esac

echo "running cmake on $BUILD_DIR. command=cmake ../ -DCMAKE_BUILD_TYPE=$BUILD_ARG $CC_ARG"
mkdir $BUILD_DIR; cd $BUILD_DIR
cmake ../ -DCMAKE_BUILD_TYPE=$BUILD_ARG $CC_ARG
cd ../
