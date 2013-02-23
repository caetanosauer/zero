#!/bin/sh

automake -a -f
libtoolize -f
aclocal
autoconf -f
automake --add-missing --force

## Configuration options
echo ------------------------------------------------------------
echo
echo Run
echo     ./configure [CONFIGURATIONS] [COMPILATION] SHORE_HOME=shore-dir [READLINE_HOME=readline-dir]
echo
echo     ./configure --help 
echo            to see the options
echo    
echo Supported configuration options
echo --enable-shore6  -  If compiling against shore-sm-6.X.X
echo --enable-dora    -  Includes DORA files, defines CFG_DORA
echo --enable-flusher -  Defines CFG_FLUSHER
echo --enable-qpipe   -  Includes QPipe files, defines CFG_QPIPE
echo --enable-bt      -  Enables backtracing facility. defines CFG_BT
echo --enable-simics  -  Adds the simics MAGIC instructions. defines CFG_SIMICS
echo --enable-hack    -  Enables physical design haks. Padding padding TPC-B tables, and partitioning indexes, such as OL_IDX
echo   
echo   
echo There are 3 supported compilation options
echo --enable-debug      -  Compile for debugging, e.g., -g
echo --enable-profile    -  Compile for profiling, e.g., -pg for oprofile
echo --enable-dbgsymbols -  Compile with debug symbols, e.g., -ggdb
echo
echo If none of them is enabled then the default compilation will be with
echo the maximum optimizations possible, e.g., -O3 or -xO4
echo
echo For SOLARIS, we suggest that you use --enable-dependendency-tracking and CC
echo     ./configure  CXX=CC --enable-dependency-tracking 
echo
echo After you configure, run
echo     make
echo
echo ------------------------------------------------------------
