
set(sm_STAT_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/alloc_cache.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_core.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_prefetch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btcursor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_defrag.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_grow.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_lock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_merge.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_search.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_split.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_verify.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_logrec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_p.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/chkpt.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/chkpt_serial.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common_templates.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/crash.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/device.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lid.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock_core.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock_lil.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logrec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logstub.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/partition.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log_core.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/page.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pmap.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/restart.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sm.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sm_du_stats.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sm_io.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sm_s.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/smfile.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/smindex.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/smstats.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/smthread.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stnode_p.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vol.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xct.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vtable_sm.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vtable_smthread.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vtable_xct.cpp)

add_library(sm STATIC ${sm_STAT_SRCS})
target_link_libraries(sm fc sthread common atomic_ops)

set (E_ERROR_GENFILES_H e_errmsg_gen.h e_einfo_gen.h e_error_def_gen.h e_error_enum_gen.h e_einfo_bakw_gen.h)
add_custom_command(OUTPUT ${E_ERROR_GENFILES_H}
  COMMAND perl ${CMAKE_SOURCE_DIR}/tools/errors.pl --d --e ${CMAKE_CURRENT_SOURCE_DIR}/e_error.dat ${COPY_GENERATED_FILES}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/e_error.dat
)

add_custom_target(E_ERROR_GENFILES DEPENDS ${E_ERROR_GENFILES_H})
add_dependencies(sm E_ERROR_GENFILES)

# generate stat header files with tools/stats.pl (sm_stat)
set(SM_STATS_GENFILES_H sm_stats_t_collect_enum_gen.h sm_stats_t_msg_gen.h sm_stats_t_struct_gen.h )
set(SM_STATS_GENFILES_CPP sm_stats_t_collect_gen.cpp sm_stats_t_dec_gen.cpp sm_stats_t_inc_gen.cpp sm_stats_t_out_gen.cpp)
set(SM_STATS_GENFILES_FILES ${SM_STATS_GENFILES_H} ${SM_STATS_GENFILES_CPP})
add_custom_command(OUTPUT ${SM_STATS_GENFILES_FILES}
  COMMAND perl ${CMAKE_SOURCE_DIR}/tools/stats.pl ${CMAKE_CURRENT_SOURCE_DIR}/sm_stats.dat ${COPY_GENERATED_FILES}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/sm_stats.dat
)
add_custom_target(SM_STATS_GENFILES DEPENDS ${SM_STATS_GENFILES_FILES})
add_dependencies(sm SM_STATS_GENFILES)

# generate stat header files with tools/stats.pl (htab_stat)
set(BF_HTAB_STATS_GENFILES_H bf_htab_stats_t_collect_enum_gen.h bf_htab_stats_t_msg_gen.h bf_htab_stats_t_struct_gen.h )
set(BF_HTAB_STATS_GENFILES_CPP bf_htab_stats_t_collect_gen.cpp bf_htab_stats_t_dec_gen.cpp bf_htab_stats_t_inc_gen.cpp bf_htab_stats_t_out_gen.cpp)
set(BF_HTAB_STATS_GENFILES_FILES ${BF_HTAB_STATS_GENFILES_H} ${BF_HTAB_STATS_GENFILES_CPP})
add_custom_command(OUTPUT ${BF_HTAB_STATS_GENFILES_FILES}
  COMMAND perl ${CMAKE_SOURCE_DIR}/tools/stats.pl ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab_stats.dat ${COPY_GENERATED_FILES}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab_stats.dat
)
add_custom_target(BF_HTAB_STATS_GENFILES DEPENDS ${BF_HTAB_STATS_GENFILES_FILES})
add_dependencies(sm BF_HTAB_STATS_GENFILES)

# generate log header files with tools/logdef.pl
set(LOGDEF_GENFILES_H logfunc_gen.h logtype_gen.h)
set(LOGDEF_GENFILES_CPP undo_gen.cpp redo_gen.cpp logdef_gen.cpp logstr_gen.cpp logstub_gen.cpp logfudge_gen.cpp)
set(LOGDEF_GENFILES_FILES ${LOGDEF_GENFILES_H} ${LOGDEF_GENFILES_CPP})
add_custom_command(OUTPUT ${LOGDEF_GENFILES_FILES}
  COMMAND perl ${CMAKE_SOURCE_DIR}/tools/logdef.pl ${CMAKE_CURRENT_SOURCE_DIR}/logdef.dat ${COPY_GENERATED_FILES}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/logdef.dat
)
add_custom_target(LOGDEF_GENFILES DEPENDS ${LOGDEF_GENFILES_FILES})
add_dependencies(sm LOGDEF_GENFILES)


SET (GENFILES_H ${E_ERROR_GENFILES_H} ${SM_STATS_GENFILES_H} ${BF_HTAB_STATS_GENFILES_H} ${LOGDEF_GENFILES_H})


########### install files ###############

install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/alloc_cache.h
  ${CMAKE_CURRENT_SOURCE_DIR}/alloc_p.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_core.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_transit_bucket.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_prefetch.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_s.h
  ${CMAKE_CURRENT_SOURCE_DIR}/btcursor.h
  ${CMAKE_CURRENT_SOURCE_DIR}/btree.h
  ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl.h
  ${CMAKE_CURRENT_SOURCE_DIR}/btree_p.h
  ${CMAKE_CURRENT_SOURCE_DIR}/chkpt.h
  ${CMAKE_CURRENT_SOURCE_DIR}/chkpt_serial.h
  ${CMAKE_CURRENT_SOURCE_DIR}/crash.h
  ${CMAKE_CURRENT_SOURCE_DIR}/device.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lid.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lock.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lock_cache.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lock_core.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lock_lil.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lock_s.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lock_x.h
  ${CMAKE_CURRENT_SOURCE_DIR}/log.h
  ${CMAKE_CURRENT_SOURCE_DIR}/log_core.h
  ${CMAKE_CURRENT_SOURCE_DIR}/partition.h
  ${CMAKE_CURRENT_SOURCE_DIR}/logrec.h
  ${CMAKE_CURRENT_SOURCE_DIR}/page.h
  ${CMAKE_CURRENT_SOURCE_DIR}/page_s.h
  ${CMAKE_CURRENT_SOURCE_DIR}/pmap.h
  ${CMAKE_CURRENT_SOURCE_DIR}/prologue.h
  ${CMAKE_CURRENT_SOURCE_DIR}/restart.h
  ${CMAKE_CURRENT_SOURCE_DIR}/restart_s.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_base.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_du_stats.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_escalation.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_0.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_1.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_2.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_3.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_4.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_io.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_s.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_vtable_enum.h
  ${CMAKE_CURRENT_SOURCE_DIR}/smstats.h
  ${CMAKE_CURRENT_SOURCE_DIR}/smthread.h
  ${CMAKE_CURRENT_SOURCE_DIR}/stnode_p.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sysdefs.h
  ${CMAKE_CURRENT_SOURCE_DIR}/vol.h
  ${CMAKE_CURRENT_SOURCE_DIR}/xct.h
  ${CMAKE_CURRENT_SOURCE_DIR}/xct_dependent.h
  DESTINATION include)

INSTALL_GENERATED_H(${GENFILES_H})

#original Makefile.am contents follow:

### Process this file with automake to produce Makefile.in
#SUBDIRS = tests smsh
#include $(top_srcdir)/Makefile.generic
#
#lib_LIBRARIES=libsm.a
#
#EXTRA_DIST = e_error.dat\
#	 sm_stats.dat logdef.dat \
#	 bf_htab_stats.dat
#
#AM_CXXFLAGS          += -I$(top_srcdir)/src/fc \
#		       -I$(top_srcdir)/src/sthread \
#		       -I$(top_srcdir)/src/common 
#
#E_ERROR_GENFILES_H = \
#	e_errmsg_gen.h \
#	e_einfo_gen.h \
#	e_error_def_gen.h \
#	e_error_enum_gen.h \
#	e_einfo_bakw_gen.h
#
#SM_STATS_GENFILES_H = \
#	sm_stats_t_collect_enum_gen.h \
#	sm_stats_t_msg_gen.h \
#	sm_stats_t_struct_gen.h 
#
#SM_STATS_GENFILES_CPP = \
#	sm_stats_t_collect_gen.cpp \
#	sm_stats_t_dec_gen.cpp \
#	sm_stats_t_inc_gen.cpp \
#	sm_stats_t_out_gen.cpp 
#
#BF_HTAB_STATS_GENFILES_H = \
#	bf_htab_stats_t_collect_enum_gen.h \
#	bf_htab_stats_t_msg_gen.h \
#	bf_htab_stats_t_struct_gen.h 
#
#BF_HTAB_STATS_GENFILES_CPP = \
#	bf_htab_stats_t_collect_gen.cpp \
#	bf_htab_stats_t_dec_gen.cpp \
#	bf_htab_stats_t_inc_gen.cpp \
#	bf_htab_stats_t_out_gen.cpp 
#
#LOGDEF_GENFILES_H = \
#	logfunc_gen.h \
#	logtype_gen.h
#
#LOGDEF_GENFILES_CPP = \
#	undo_gen.cpp \
#	redo_gen.cpp \
#	logdef_gen.cpp \
#	logstr_gen.cpp \
#	logstub_gen.cpp \
#	logfudge_gen.cpp
#
#GENFILES_CPP = $(SM_STATS_GENFILES_CPP) \
#	   $(BF_HTAB_STATS_GENFILES_CPP) \
#	   $(LOGDEF_GENFILES_CPP)
#
#GENFILES_H = $(E_ERROR_GENFILES_H) \
#	   $(SM_STATS_GENFILES_H) \
#	   $(BF_HTAB_STATS_GENFILES_H) \
#	   $(LOGDEF_GENFILES_H)
#
# 
#GENFILES = $(GENFILES_H) $(GENFILES_CPP)
#
#include_HEADERS = \
#	$(GENFILES_H) \
#	app_support.h \
#	bf.h bf_core.h  bf_htab.h bf_transit_bucket.h\
#	bf_prefetch.h bf_s.h \
#	btcursor.h btree.h btree_impl.h btree_p.h \
#	chkpt.h chkpt_serial.h \
#	crash.h \
#	device.h dir.h \
#	extent.h \
#	file.h file_s.h \
#	histo.h lexify.h \
#	lgrec.h lid.h \
#	lock.h lock_cache.h lock_core.h lock_s.h lock_s_inline.h lock_x.h \
#	log.h log_core.h partition.h logrec.h \
#	page.h page_alias.h page_h.h page_s.h \
#	pin.h \
#	pmap.h \
#	prologue.h \
#	restart.h restart_s.h \
#	scan.h sdesc.h \
#	sm.h sm_base.h sm_du_stats.h sm_escalation.h \
#	sm_int_0.h sm_int_1.h sm_int_2.h sm_int_3.h sm_int_4.h \
#	sm_io.h sm_s.h \
#	sm_vtable_enum.h \
#	smstats.h \
#	smthread.h \
#	sort.h sort_s.h \
#	sysdefs.h \
#	vol.h \
#	xct.h xct_dependent.h \
#	zkeyed.h 
#
#
#libsm_a_SOURCES=  \
#	bf.cpp bf_core.cpp \
#	bf_htab.cpp bf_htab_test.cpp \
#	bf_prefetch.cpp \
#	btcursor.cpp btree.cpp btree_impl.cpp btree_p.cpp \
#	chkpt.cpp chkpt_serial.cpp \
#	common_templates.cpp \
#	crash.cpp \
#	device.cpp \
#	dir.cpp \
#	file.cpp \
#	histo.cpp \
#	keyed.cpp zkeyed.cpp \
#	lexify.cpp \
#	lgrec.cpp \
#	lid.cpp \
#	lock.cpp lock_core.cpp \
#	log.cpp logrec.cpp logstub.cpp \
#	partition.cpp log_core.cpp \
#	sort.cpp newsort.cpp \
#	page.cpp \
#	pin.cpp \
#	pmap.cpp \
#	restart.cpp \
#	scan.cpp \
#	sm.cpp \
#	sm_du_stats.cpp \
#	sm_io.cpp \
#	sm_s.cpp \
#	smfile.cpp smindex.cpp \
#	smstats.cpp \
#	smthread.cpp \
#	vol.cpp \
#	xct.cpp \
#	vtable_sm.cpp \
#	vtable_smthread.cpp \
#	vtable_xct.cpp
#
#MOSTLYCLEANFILES = $(GENFILES) e_error.timestamp logdef.timestamp \
#	sm_stats.timestamp
#
#$(E_ERROR_GENFILES_H): e_error.dat
#	$(PERL) $(top_srcdir)/tools/errors.pl --d --e $?
#
#$(BF_HTAB_STATS_GENFILES_CPP) $(BF_HTAB_STATS_GENFILES_H): bf_htab_stats.dat
#	$(PERL) $(top_srcdir)/tools/stats.pl $?
#
#$(SM_STATS_GENFILES_CPP) $(SM_STATS_GENFILES_H): sm_stats.dat
#	$(PERL) $(top_srcdir)/tools/stats.pl $?
#
#$(LOGDEF_GENFILES_H) $(LOGDEF_GENFILES_CPP): logdef.dat
#	$(PERL) -w $(top_srcdir)/tools/logdef.pl $?
#
