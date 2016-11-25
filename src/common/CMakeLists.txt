INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

set(common_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.cpp # new stats infrastructure
  ${CMAKE_CURRENT_SOURCE_DIR}/worker_thread.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_debug.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/latches.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/rand48.cpp # used only in tls_rng, clean & throw away
  ${CMAKE_CURRENT_SOURCE_DIR}/block_alloc.cpp # unifiy all mem mgmt code (2 below too)
  ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/mem_block.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/gethrtime.cpp # used in vol.cpp fake disk latency -- clean up & throw away
  ${CMAKE_CURRENT_SOURCE_DIR}/w_base.cpp # cleanup and remove if possible (lots of std numeric stuff)
  ${CMAKE_CURRENT_SOURCE_DIR}/w_listm.cpp # remove all uses and throw away
  ${CMAKE_CURRENT_SOURCE_DIR}/tls.cpp # cleanup (at least macros) and leave only memory manager stuff
  ${CMAKE_CURRENT_SOURCE_DIR}/w_findprime.cpp # used in bf tree hashtable -> move or replace with std
  ${CMAKE_CURRENT_SOURCE_DIR}/basics.cpp # cleanup unused parts; move used parts elsewhere
  ${CMAKE_CURRENT_SOURCE_DIR}/latch.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/lsn.cpp # unify with FL's new lsn_t
  ${CMAKE_CURRENT_SOURCE_DIR}/tid_t.cpp # throw away and use simple int
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

