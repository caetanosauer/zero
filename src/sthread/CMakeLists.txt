
set(sthread_STAT_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/errlog.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread_core_pthread.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/srwlock.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/io.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sdisk_unix.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sdisk.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/vtable_sthread.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_debug.cpp)

add_library(sthread STATIC ${sthread_STAT_SRCS})
target_link_libraries (sthread fc pthread rt)

# generate error header files with tools/errors.pl
set (ST_ERROR_GENFILES_H st_errmsg_gen.h st_einfo_gen.h st_error_def_gen.h st_error_enum_gen.h st_einfo_bakw_gen.h)
foreach(_file ${ST_ERROR_GENFILES_H})
    add_custom_command(OUTPUT ${_file}
        COMMAND perl ${CMAKE_SOURCE_DIR}/tools/errors.pl -d -e ${CMAKE_CURRENT_SOURCE_DIR}/st_error.dat
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/st_error.dat
    )
    set(GENFILES_B_H ${GENFILES_B_H} ${CMAKE_CURRENT_BINARY_DIR}/${_file})
endforeach()
add_custom_target(ST_ERROR_GENFILES DEPENDS ${ST_ERROR_GENFILES_H})
add_dependencies(sthread ST_ERROR_GENFILES)

# generate stat header files with tools/stats.pl
# again, this has to be a part of source tree.
set(STHREAD_GENFILES_H sthread_stats_collect_enum_gen.h sthread_stats_msg_gen.h sthread_stats_struct_gen.h)
set(STHREAD_GENFILES_CPP sthread_stats_collect_gen.cpp sthread_stats_dec_gen.cpp sthread_stats_inc_gen.cpp sthread_stats_out_gen.cpp)
set(STHREAD_GENFILES_FILES ${STHREAD_GENFILES_H} ${STHREAD_GENFILES_CPP})
foreach(_file ${STHREAD_GENFILES_FILES})
    add_custom_command(OUTPUT ${_file}
        COMMAND perl ${CMAKE_SOURCE_DIR}/tools/stats.pl ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.dat
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.dat
    )
    set(GENFILES_B_H ${GENFILES_B_H} ${CMAKE_CURRENT_BINARY_DIR}/${_file})
endforeach()
add_custom_target(STHREAD_GENFILES DEPENDS ${STHREAD_GENFILES_FILES})
add_dependencies(sthread STHREAD_GENFILES)

SET (GENFILES_H ${ST_ERROR_GENFILES_H} ${STHREAD_GENFILES_H})

########### install files ###############

install(FILES ${GENFILES_B_H}
  ${CMAKE_CURRENT_SOURCE_DIR}/auto_release.h
  ${CMAKE_CURRENT_SOURCE_DIR}/errlog.h
  ${CMAKE_CURRENT_SOURCE_DIR}/critical_section.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_pthread.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mcs_lock.h
  ${CMAKE_CURRENT_SOURCE_DIR}/os_fcntl.h
  ${CMAKE_CURRENT_SOURCE_DIR}/os_interface.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sdisk.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sdisk_unix.h
  ${CMAKE_CURRENT_SOURCE_DIR}/stcore_pthread.h
  ${CMAKE_CURRENT_SOURCE_DIR}/srwlock.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sthread_vtable_enum.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_debug.h
  DESTINATION include)

