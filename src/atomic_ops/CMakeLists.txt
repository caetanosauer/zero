
SET(CMAKE_VERBOSE_MAKEFILE ON)

add_subdirectory(asm_linkage)
add_subdirectory(atomic_ops)


########### next target ###############

# atomic_ops.S is assembler, which is a bit tricky to
# handle in cmake.

IF (COMPILER_IS_SUNCC)
  SET (atomic_ops_cmpl CC -I${CMAKE_SOURCE_DIR}/config -I${CMAKE_CURRENT_SOURCE_DIR} -m64 -DARCH_LP64 -g -c ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops.S)
ELSE (COMPILER_IS_SUNCC)
  IF (CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "Trace")
    SET (atomic_ops_cmpl ${CMAKE_C_COMPILER} -DHAVE_CONFIG_H -I${CMAKE_SOURCE_DIR}/config -I${CMAKE_CURRENT_SOURCE_DIR} -D__GNUC_AS__ -D_ASM -m64 -DARCH_LP64 -g -c -o atomic_ops.o ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops.S)
  ELSE (CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "Trace")
    SET (atomic_ops_cmpl ${CMAKE_C_COMPILER} -DHAVE_CONFIG_H -I${CMAKE_SOURCE_DIR}/config -I${CMAKE_CURRENT_SOURCE_DIR} -D__GNUC_AS__ -D_ASM -m64 -DARCH_LP64 -O2 -c -o atomic_ops.o ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops.S)
  ENDIF (CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "Trace")
ENDIF (COMPILER_IS_SUNCC)

add_custom_command(OUTPUT atomic_ops.o
    COMMAND ${atomic_ops_cmpl}
    VERBATIM)

add_library(atomic_ops STATIC atomic_ops.o)
SET_TARGET_PROPERTIES(atomic_ops PROPERTIES LINKER_LANGUAGE C)
########### install files ###############

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops.h ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops_impl.h DESTINATION include)



#original Makefile.am contents follow:

### Process this file with automake to produce Makefile.in
#
#include $(top_srcdir)/Makefile.generic
#
#EXTRA_DIST =  makelinks.sh
#SUBDIRS =  asm_linkage atomic_ops
#DIST_SUBDIRS =  $(SUBDIRS)
#
#lib_LIBRARIES     = libatomic_ops.a
#
#include_HEADERS = atomic_ops.h atomic_ops_impl.h
#
#libatomic_ops_a_SOURCES      = atomic_ops.S
#
#atomic_ops.o:  links
#
#links: 
#	chmod +x ./makelinks.sh
#	./makelinks.sh
