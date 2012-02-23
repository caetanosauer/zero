
SET(CMAKE_VERBOSE_MAKEFILE ON)

add_subdirectory(asm_linkage)
add_subdirectory(atomic_ops)


########### next target ###############

# atomic_ops.S is assembler, which is a bit tricky to
# handle in cmake.

IF (COMPILER_IS_SUNCC)
  # Seems like SunCC in assembly mode ignores -I options! crap.
  # however, shore-config-env.h is in build folder.. so, we give SHORE_CONFIG_ENV_H_PATH to do it.
  # Be careful on quoting escapes. Google "computed includes for C" if in doubt.
  # The resulting command line must be like:
  # /usr/bin/CC "-DSHORE_CONFIG_ENV_H_PATH=</export/home/hideaki/zero_brown/trunk/debug_build/config/shore-config-env.h>" -m64 -g -c /export/home/hideaki/zero_brown/trunk/src/atomic_ops/atomic_ops.S
  SET (atomic_ops_cmpl ${CMAKE_CXX_COMPILER} -DSHORE_CONFIG_ENV_H_PATH=<${CMAKE_BINARY_DIR}/config/shore-config-env.h> -m64 -g -c ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops.S)
  # I know this is ugly, but couldn't find out a better way.
ELSE (COMPILER_IS_SUNCC)
  SET (atomic_ops_cmpl ${CMAKE_C_COMPILER} -I${CMAKE_SOURCE_DIR}/config -I${CMAKE_BINARY_DIR}/config -I${CMAKE_CURRENT_SOURCE_DIR} -D__GNUC_AS__ -D_ASM -m64 -O2 -c -o atomic_ops.o ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops.S)
ENDIF (COMPILER_IS_SUNCC)

add_custom_command(OUTPUT atomic_ops.o
    COMMAND ${atomic_ops_cmpl}
    VERBATIM)

add_library(atomic_ops STATIC atomic_ops.o)
SET_TARGET_PROPERTIES(atomic_ops PROPERTIES LINKER_LANGUAGE C)
########### install files ###############

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops.h ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops_impl.h DESTINATION include)

