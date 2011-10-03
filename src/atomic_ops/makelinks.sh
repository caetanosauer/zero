#!/bin/sh
if test -h ia32; then
   echo ./asm_linkage/usr/src/uts/intel/ia32 exists
else
   echo ln -s ./asm_linkage/usr/src/uts/intel/ia32
   ln -s ./asm_linkage/usr/src/uts/intel/ia32
fi

if test -h intel; then
    echo ./asm_linkage/usr/src/uts/intel exists
else
    echo ln -s ./asm_linkage/usr/src/uts/intel
    ln -s ./asm_linkage/usr/src/uts/intel
fi

if test -h sparc; then
    echo ./asm_linkage/usr/src/uts/sparc exists
else
    echo ln -s ./asm_linkage/usr/src/uts/sparc
    ln -s ./asm_linkage/usr/src/uts/sparc
fi

