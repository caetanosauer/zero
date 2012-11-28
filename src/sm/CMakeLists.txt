
set(sm_STAT_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/alloc_cache.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_core.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_fixed.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_hashtable.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_prefetch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_tree.cpp
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
    ${CMAKE_CURRENT_SOURCE_DIR}/lock_dump.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock_vtable.cpp
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
target_link_libraries(sm fc sthread common)

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
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_fixed.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_hashtable.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_transit_bucket.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_prefetch.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_s.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bf_tree.h
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
  ${CMAKE_CURRENT_SOURCE_DIR}/lock_bucket.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lock_compt.h
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
