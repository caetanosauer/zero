
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

IF(${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
    SET (COPY_GENERATED_FILES)
ELSE(${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
    SET (COPY_GENERATED_FILES && cp -f *_gen.* ${CMAKE_CURRENT_SOURCE_DIR})
ENDIF(${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})

# generate error header files with tools/errors.pl
# again, this has to be a part of source tree.
set (ST_ERROR_GENFILES_H st_errmsg_gen.h st_einfo_gen.h st_error_def_gen.h st_error_enum_gen.h st_einfo_bakw_gen.h)
foreach(_file ${ST_ERROR_GENFILES_H})
    add_custom_command(OUTPUT ${_file}
        COMMAND perl ${CMAKE_SOURCE_DIR}/tools/errors.pl -d -e ${CMAKE_CURRENT_SOURCE_DIR}/st_error.dat ${COPY_GENERATED_FILES}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/st_error.dat
    )
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
        COMMAND perl ${CMAKE_SOURCE_DIR}/tools/stats.pl ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.dat ${COPY_GENERATED_FILES}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/sthread_stats.dat
    )
endforeach()
add_custom_target(STHREAD_GENFILES DEPENDS ${STHREAD_GENFILES_FILES})
add_dependencies(sthread STHREAD_GENFILES)

SET (GENFILES_H ${ST_ERROR_GENFILES_H} ${STHREAD_GENFILES_H})

########### install files ###############

install(FILES ${GENFILES_H}
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



#original Makefile.am contents follow:

### Process this file with automake to produce Makefile.in
#SUBDIRS = tests
#include $(top_srcdir)/Makefile.generic
#EXTRA_DIST = st_error.dat sthread_stats.dat 
#
#AM_CXXFLAGS          += -I. \
#		       -I$(top_srcdir)/src/fc
#
#
#lib_LIBRARIES     = libsthread.a
#
#ST_ERROR_GENFILES  = st_errmsg_gen.h\
#		     st_einfo_gen.h\
#		     st_error_def_gen.h\
#		     st_error_enum_gen.h\
#		     st_einfo_bakw_gen.h
#
#STHREAD_GENFILES_H  = \
#    sthread_stats_collect_enum_gen.h \
#	sthread_stats_msg_gen.h \
#	sthread_stats_struct_gen.h
#
#STHREAD_GENFILES_CPP = \
#	sthread_stats_collect_gen.cpp \
#	sthread_stats_dec_gen.cpp \
#	sthread_stats_inc_gen.cpp \
#	sthread_stats_out_gen.cpp 
#
#
#GENFILES_H = $(SHMC_GENFILES_H)  $(STHREAD_GENFILES_H) $(ST_ERROR_GENFILES)
#
#GENFILES_CPP = $(SHMC_GENFILES_CPP)  $(STHREAD_GENFILES_CPP) 
#
#GENFILES = $(GENFILES_H) $(GENFILES_CPP)
#
#include_HEADERS = \
#	$(GENFILES_H) \
#	auto_release.h \
#	errlog.h critical_section.h w_pthread.h \
#	mcs_lock.h \
#	os_fcntl.h os_interface.h \
#	sdisk.h \
#	sdisk_unix.h \
#	stcore_pthread.h \
#	srwlock.h \
#	sthread.h \
#	sthread_stats.h sthread_vtable_enum.h  w_debug.h
#
#libsthread_a_SOURCES      = \
#	errlog.cpp \
#	sthread.cpp \
#	sthread_core_pthread.cpp \
#	sthread_stats.cpp \
#	srwlock.cpp \
#	io.cpp \
#	sdisk_unix.cpp \
#	sdisk.cpp \
#	vtable_sthread.cpp w_debug.cpp
#
#
## libsthread_a_LIBADD       = @LIBOBJS@
#
#MOSTLYCLEANFILES = $(GENFILES_H) $(GENFILES_CPP)
#
### sthread.h: st_error_enum_gen.h
### sthread_stats.h: sthread_stats_struct_gen.h
#
#$(ST_ERROR_GENFILES): st_error.dat
#	$(PERL) $(top_srcdir)/tools/errors.pl --d --e $?
#
#
#event.$(OBJEXT): $(STHREAD_GENFILES_H)
#sthread_stats.$(OBJEXT): $(STHREAD_GENFILES_CPP)
### vtable_sthread.$(OBJEXT): $(STHREAD_GENFILES_H)
#$(STHREAD_GENFILES_CPP) $(STHREAD_GENFILES_H): sthread_stats.dat
#	$(PERL) $(top_srcdir)/tools/stats.pl $?
#
