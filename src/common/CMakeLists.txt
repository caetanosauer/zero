
set(common_STAT_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/basics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bitmap.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/devid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/latch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kvl_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lsn.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/option.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regerror.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regcomp.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regexec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regfree.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/regex_posix.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vec_mkchunk.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vec_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vid_t.cpp)

add_library(common STATIC ${common_STAT_SRCS})
#target_link_libraries (common atomic_ops)

IF(${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
    SET (COPY_GENERATED_FILES)
ELSE(${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
    SET (COPY_GENERATED_FILES && cp -f *_gen.* ${CMAKE_CURRENT_SOURCE_DIR})
ENDIF(${CMAKE_CURRENT_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})

# generate error header files with tools/errors.pl
# again, this has to be a part of source tree.
set (OPT_ERROR_GENFILES_H opt_errmsg_gen.h opt_einfo_gen.h opt_error_def_gen.h opt_einfo_bakw_gen.h)
foreach(_file ${OPT_ERROR_GENFILES_H})
    add_custom_command(OUTPUT ${_file}
        COMMAND perl ${CMAKE_SOURCE_DIR}/tools/errors.pl -d ${CMAKE_CURRENT_SOURCE_DIR}/opt_error.dat ${COPY_GENERATED_FILES}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/opt_error.dat
    )
endforeach()
add_custom_target(OPT_ERROR_GENFILES DEPENDS ${OPT_ERROR_GENFILES_H})
add_dependencies(common OPT_ERROR_GENFILES)


set (GENFILES ${OPT_ERROR_GENFILES_H})
########### install files ###############

install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/basics.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bitmap.h
  ${CMAKE_CURRENT_SOURCE_DIR}/devid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lsn.h
  ${CMAKE_CURRENT_SOURCE_DIR}/hostname.h
  ${CMAKE_CURRENT_SOURCE_DIR}/kvl_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/latch.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/option.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regcomp_i.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regerror_i.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex2.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_cclass.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_cname.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_engine_i.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_posix.h
  ${CMAKE_CURRENT_SOURCE_DIR}/regex_utils.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_vas.h
  ${CMAKE_CURRENT_SOURCE_DIR}/stid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/tid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/umemcmp.h
  ${CMAKE_CURRENT_SOURCE_DIR}/vec_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/vid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_opaque.h
  ${GENFILES} DESTINATION include)



#original Makefile.am contents follow:

### Process this file with automake to produce Makefile.in
#SUBDIRS = tests
#include $(top_srcdir)/Makefile.generic
#EXTRA_DIST = opt_error.dat regex_engine.cpp
#
#AM_CXXFLAGS          += -I. \
#		       -I$(top_srcdir)/src/fc \
#		       -I$(top_srcdir)/src/sthread
#
#lib_LIBRARIES     = libcommon.a
#
#OPT_ERROR_GENFILES_H  = opt_errmsg_gen.h opt_einfo_gen.h \
#		       	opt_error_def_gen.h \
#			    opt_einfo_bakw_gen.h
#
#GENFILES = $(OPT_ERROR_GENFILES_H)
#
#include_HEADERS    = \
#	basics.h bitmap.h \
#	devid_t.h \
#	lsn.h \
#	hostname.h kvl_t.h latch.h \
#	lid_t.h option.h regcomp_i.h \
#	regerror_i.h regex.h regex2.h \
#	regex_cclass.h regex_cname.h regex_engine_i.h \
#	regex_posix.h regex_utils.h  \
#	store_latch_manager.h \
#	sm_vas.h stid_t.h \
#	tid_t.h umemcmp.h vec_t.h \
#	vid_t.h w_opaque.h \
#	$(GENFILES)
#
#libcommon_a_SOURCES      = \
#	basics.cpp bitmap.cpp devid_t.cpp latch.cpp lid_t.cpp kvl_t.cpp	\
#	lsn.cpp option.cpp \
#    regerror.cpp regcomp.cpp \
#    regexec.cpp regfree.cpp regex_posix.cpp \
#	store_latch_manager.cpp \
#	stid_t.cpp tid_t.cpp vec_mkchunk.cpp		\
#	vec_t.cpp vid_t.cpp  
#
#
#MOSTLYCLEANFILES = $(GENFILES)
#
#option.o: $(OPT_ERROR_GENFILES_H)
#$(OPT_ERROR_GENFILES_H): opt_error.dat
#	$(PERL) $(top_srcdir)/tools/errors.pl -d $?
