
set(fc_STAT_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/rand48.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rusage.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/block_alloc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/mem_block.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gethrtime.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_compat_strstream.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_form.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common_tmpl.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_getopt.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stime.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vtable.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_base.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_bitmap.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_error.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/valgrind_help.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_listm.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_rc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_input.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tls.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_findprime.cpp)

add_library(fc STATIC ${fc_STAT_SRCS})
#target_link_libraries (fc atomic_ops)

# generate error header files with tools/errors.pl
# some help here : http://stackoverflow.com/questions/2354473/cmake-add-custom-command
# note: these generated files have to be a part of source, so we copy them into
#  source tree. (not fully out-of-source build because of this)
set (FC_ERROR_GENFILES_H
    fc_errmsg_gen.h
    fc_einfo_gen.h
    fc_error_def_gen.h
    fc_error_enum_gen.h
    fc_einfo_bakw_gen.h)
add_custom_command(OUTPUT ${FC_ERROR_GENFILES_H}
  COMMAND perl ${CMAKE_SOURCE_DIR}/tools/errors.pl -d -e ${CMAKE_CURRENT_SOURCE_DIR}/fc_error.dat ${COPY_GENERATED_FILES}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/fc_error.dat
)
add_custom_target(FC_ERROR_GENFILES DEPENDS ${FC_ERROR_GENFILES_H})
add_dependencies(fc FC_ERROR_GENFILES)


########### install files ###############

install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/atomic_container.h
  ${CMAKE_CURRENT_SOURCE_DIR}/atomic_templates.h
  ${CMAKE_CURRENT_SOURCE_DIR}/block_alloc.h
  ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mem_block.h
  ${CMAKE_CURRENT_SOURCE_DIR}/errlog_s.h
  ${CMAKE_CURRENT_SOURCE_DIR}/gethrtime.h
  ${CMAKE_CURRENT_SOURCE_DIR}/largefile_aware.h
  ${CMAKE_CURRENT_SOURCE_DIR}/os_types.h
  ${CMAKE_CURRENT_SOURCE_DIR}/rand48.h
  ${CMAKE_CURRENT_SOURCE_DIR}/stime.h
  ${CMAKE_CURRENT_SOURCE_DIR}/tls.h
  ${CMAKE_CURRENT_SOURCE_DIR}/vtable.h
  ${CMAKE_CURRENT_SOURCE_DIR}/valgrind_help.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_autodel.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_base.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_bitmap.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_bitvector.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_compat_strstream.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_hashing.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_defines.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_error.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_findprime.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_getopt.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_gettimeofday.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_hash.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_heap.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_list.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_minmax.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_rc.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_rusage.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_stream.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_strstream.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_stat.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_workaround.h
  DESTINATION include)

INSTALL_GENERATED_H(${FC_ERROR_GENFILES_H})


#original Makefile.am contents follow:

### Process this file with automake to produce Makefile.in
#SUBDIRS = tests
#include $(top_srcdir)/Makefile.generic
#EXTRA_DIST = fc_error.dat
#
#lib_LIBRARIES     = libfc.a
#
#FC_ERROR_GENFILES_H  = fc_errmsg_gen.h \
#             fc_einfo_gen.h \
#             fc_error_def_gen.h \
#             fc_error_enum_gen.h \
#             fc_einfo_bakw_gen.h
#
#GENFILES = $(FC_ERROR_GENFILES_H)
#
#include_HEADERS = \
#    atomic_container.h \
#    atomic_templates.h \
#    block_alloc.h \
#    dynarray.h \
#    mem_block.h \
#    errlog_s.h \
#    gethrtime.h \
#    largefile_aware.h \
#    os_types.h \
#    rand48.h \
#    stime.h \
#    tls.h \
#    vtable.h \
#    valgrind_help.h \
#    w.h w_autodel.h w_base.h w_bitmap.h w_bitvector.h \
#    w_compat_strstream.h \
#    w_hashing.h \
#    w_defines.h \
#    w_error.h \
#    w_findprime.h \
#    w_getopt.h w_gettimeofday.h \
#    w_hash.h w_heap.h w_list.h w_minmax.h \
#    w_rc.h w_rusage.h \
#    w_stream.h w_strstream.h \
#    w_stat.h \
#    w_workaround.h \
#    $(GENFILES)
#
#
#libfc_a_SOURCES      = \
#        rand48.cpp \
#        rusage.cpp \
#        block_alloc.cpp \
#        dynarray.cpp \
#        mem_block.cpp \
#        gethrtime.cpp \
#        w_compat_strstream.cpp \
#        w_form.cpp \
#        common_tmpl.cpp w_getopt.cpp \
#        stime.cpp vtable.cpp w_base.cpp w_bitmap.cpp w_error.cpp \
#        valgrind_help.cpp \
#        w_listm.cpp w_rc.cpp \
#        w_input.cpp tls.cpp w_findprime.cpp
#
#MOSTLYCLEANFILES = $(GENFILES)
#
#debug.$(OBJEXT): $(FC_ERROR_GENFILES_H)
#$(FC_ERROR_GENFILES_H): fc_error.dat
#	$(PERL) $(top_srcdir)/tools/errors.pl -d -e $?
#
