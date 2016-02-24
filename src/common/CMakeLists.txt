INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

set(common_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/errlog.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread_core_pthread.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/srwlock.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/io.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sdisk_unix.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sdisk.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_debug.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/rand48.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/block_alloc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/mem_block.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/gethrtime.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_compat_strstream.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/stime.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_base.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_listm.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tls.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_findprime.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/basics.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/bitmap.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/latch.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/kvl_t.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/lsn.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/tid_t.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/vec_mkchunk.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/vec_t.cpp)

add_library(common STATIC ${common_SRCS})
target_link_libraries (common pthread rt)

# generate stat header files with tools/stats.pl
# again, this has to be a part of source tree.
set(COMMON_GENFILES_H sthread_stats_collect_enum_gen.h sthread_stats_msg_gen.h sthread_stats_struct_gen.h)
set(COMMON_GENFILES_CPP sthread_stats_collect_gen.cpp sthread_stats_dec_gen.cpp sthread_stats_inc_gen.cpp sthread_stats_out_gen.cpp)
set(COMMON_GENFILES_FILES ${COMMON_GENFILES_H} ${COMMON_GENFILES_CPP})
foreach(_file ${COMMON_GENFILES_FILES})
    add_custom_command(OUTPUT ${_file}
       COMMAND perl ${CMAKE_SOURCE_DIR}/tools/stats.pl ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.dat
       DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.dat
    )
    set(COMMON_GENFILES_FILES_B ${COMMON_GENFILES_FILES_B} ${CMAKE_CURRENT_BINARY_DIR}/${_file})
endforeach()
add_custom_target(COMMON_GENFILES DEPENDS ${COMMON_GENFILES_FILES})
add_dependencies(common COMMON_GENFILES)

SET (GENFILES_H ${COMMON_GENFILES_H})
foreach(_file ${GENFILES_H})
    set(GENFILES_B_H ${GENFILES_B_H} ${CMAKE_CURRENT_BINARY_DIR}/${_file})
endforeach()

