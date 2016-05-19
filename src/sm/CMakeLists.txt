
set(sm_STAT_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/alloc_cache.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/alloc_page.cpp
    #${CMAKE_CURRENT_SOURCE_DIR}/allocator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/backup_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_hashtable.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_tree.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_tree_cleaner.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bf_tree_evict.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btcursor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_defrag.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_grow.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_lock.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_merge.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_search.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_split.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl_verify.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_logrec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_page.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/btree_page_h.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/chkpt.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventlog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/fixable_page_h.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/generic_page.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logarchiver.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/mem_mgmt.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock_core.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock_dump.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock_lil.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock_raw.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lock_x.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log_carray.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log_core.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log_storage.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log_spr.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log_lsn_tracker.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logrec.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logstub.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/page_cleaner.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/page_cleaner_decoupled.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/partition.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/plog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/plog_xct.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/restart.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/restore.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sm.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/smindex.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/smstats.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/smthread.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stnode_page.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vol.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/xct.cpp
)

add_library(sm STATIC ${sm_STAT_SRCS})
target_link_libraries(sm
    common
    numa
    boost_program_options
    boost_system
    boost_filesystem
    boost_regex
    )

# CS: target that uses plog_xct, i.e., atomic commit protocol
add_library(sm_plog STATIC ${sm_STAT_SRCS})
target_link_libraries(sm_plog fc sthread common numa)
get_target_property (SM_PLOG_COMPILEDEF sm_plog COMPILE_DEFINITIONS)
#set(SM_PLOG_COMPILEDEF "SIMULATE_NO_SWIZZLING;USE_ATOMIC_COMMIT")
set(SM_PLOG_COMPILEDEF "USE_ATOMIC_COMMIT")
set_target_properties (sm_plog PROPERTIES
    COMPILE_DEFINITIONS "${SM_PLOG_COMPILEDEF}"
    EXCLUDE_FROM_ALL  ON
)

# generate a library that performs swizzling but no pin_cnt increments/decrements
add_library(sm_swizzling_nopincntincdec STATIC ${sm_STAT_SRCS})
target_link_libraries(sm_swizzling_nopincntincdec fc sthread common numa)
# make sure we don't override any previous value of COMPILE_DEFINITIONS
get_target_property (SM_SWIZZLING_NOPINCNTINCDEC_COMPILEDEF sm_swizzling_nopincntincdec COMPILE_DEFINITIONS)
set(SM_SWIZZLING_NOPINCNTINCDEC_COMPILEDEF "NO_PINCNT_INCDEC")
set_target_properties (sm_swizzling_nopincntincdec
    PROPERTIES COMPILE_DEFINITIONS "${SM_SWIZZLING_NOPINCNTINCDEC_COMPILEDEF}"
    EXCLUDE_FROM_ALL ON
)

# generate a library that simulates no swizzling
add_library(sm_noswizzling STATIC ${sm_STAT_SRCS})
target_link_libraries(sm_noswizzling fc sthread common numa)
# make sure we don't override any previous value of COMPILE_DEFINITIONS
get_target_property (SM_NOSWIZZLING_COMPILEDEF sm_noswizzling COMPILE_DEFINITIONS)
set(SM_NOSWIZZLING_COMPILEDEF "SIMULATE_NO_SWIZZLING")
set_target_properties (sm_noswizzling
    PROPERTIES COMPILE_DEFINITIONS "${SM_NOSWIZZLING_COMPILEDEF}"
    EXCLUDE_FROM_ALL ON
)

# generate a library that simulates an in-memory DB
add_library(sm_mainmemory STATIC ${sm_STAT_SRCS})
target_link_libraries(sm_mainmemory fc sthread common numa)
# make sure we don't override any previous value of COMPILE_DEFINITIONS
get_target_property (SM_MAINMEMORY_COMPILEDEF sm_mainmemory COMPILE_DEFINITIONS)
set(SM_MAINMEMORY_COMPILEDEF "SIMULATE_MAINMEMORYDB")
set_target_properties (sm_mainmemory
    PROPERTIES COMPILE_DEFINITIONS "${SM_MAINMEMORY_COMPILEDEF}"
    EXCLUDE_FROM_ALL ON
)

# generate stat header files with tools/stats.pl (sm_stat)
set(SM_STATS_GENFILES_H sm_stats_t_collect_enum_gen.h sm_stats_t_msg_gen.h sm_stats_t_struct_gen.h )
set(SM_STATS_GENFILES_CPP sm_stats_t_collect_gen.cpp sm_stats_t_dec_gen.cpp sm_stats_t_inc_gen.cpp sm_stats_t_out_gen.cpp)
set(SM_STATS_GENFILES_FILES ${SM_STATS_GENFILES_H} ${SM_STATS_GENFILES_CPP})
foreach(_file ${SM_STATS_GENFILES_FILES})
    add_custom_command(OUTPUT ${_file}
        COMMAND perl ${CMAKE_SOURCE_DIR}/tools/stats.pl ${CMAKE_CURRENT_SOURCE_DIR}/sm_stats.dat
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/sm_stats.dat
    )
    set(SM_STATS_GENFILES_B_H ${SM_STATS_GENFILES_B_H} ${CMAKE_CURRENT_BINARY_DIR}/${_file})
endforeach()
add_custom_target(SM_STATS_GENFILES DEPENDS ${SM_STATS_GENFILES_FILES})
add_dependencies(sm SM_STATS_GENFILES)

# generate stat header files with tools/stats.pl (htab_stat)
set(BF_HTAB_STATS_GENFILES_H bf_htab_stats_t_collect_enum_gen.h bf_htab_stats_t_msg_gen.h bf_htab_stats_t_struct_gen.h )
set(BF_HTAB_STATS_GENFILES_CPP bf_htab_stats_t_collect_gen.cpp bf_htab_stats_t_dec_gen.cpp bf_htab_stats_t_inc_gen.cpp bf_htab_stats_t_out_gen.cpp)
set(BF_HTAB_STATS_GENFILES_FILES ${BF_HTAB_STATS_GENFILES_H} ${BF_HTAB_STATS_GENFILES_CPP})
foreach(_file ${BF_HTAB_STATS_GENFILES_FILES})
    add_custom_command(OUTPUT ${_file}
        COMMAND perl ${CMAKE_SOURCE_DIR}/tools/stats.pl ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab_stats.dat
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/bf_htab_stats.dat
    )
    set (BF_HTAB_STATS_GENFILES_B_H ${BF_HTAB_STATS_GENFILES_B_H} ${CMAKE_CURRENT_BINARY_DIR}/${_file})
endforeach()
add_custom_target(BF_HTAB_STATS_GENFILES DEPENDS ${BF_HTAB_STATS_GENFILES_FILES})
add_dependencies(sm BF_HTAB_STATS_GENFILES)

# generate log header files with tools/logdef.pl
set(LOGDEF_GENFILES_H logfunc_gen.h logtype_gen.h)
set(LOGDEF_GENFILES_CPP undo_gen.cpp redo_gen.cpp logdef_gen.cpp logstr_gen.cpp logstub_gen.cpp logfudge_gen.cpp)
set(LOGDEF_GENFILES_FILES ${LOGDEF_GENFILES_H} ${LOGDEF_GENFILES_CPP})
foreach(_file ${LOGDEF_GENFILES_FILES})
    add_custom_command(OUTPUT ${_file}
        COMMAND perl ${CMAKE_SOURCE_DIR}/tools/logdef.pl ${CMAKE_CURRENT_SOURCE_DIR}/logdef.dat
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/logdef.dat
    )
    set (LOGDEF_GENFILES_B_H ${LOGDEF_GENFILES_B_H} ${CMAKE_CURRENT_BINARY_DIR}/${_file})
endforeach()
add_custom_target(LOGDEF_GENFILES DEPENDS ${LOGDEF_GENFILES_FILES})
add_dependencies(sm LOGDEF_GENFILES)


SET (GENFILES_H ${SM_STATS_GENFILES_B_H} ${BF_HTAB_STATS_GENFILES_B_H} ${LOGDEF_GENFILES_B_H})


########### install files ###############

# install(FILES
#   ${GENFILES_H}
#   ${CMAKE_CURRENT_SOURCE_DIR}/alloc_cache.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/alloc_page.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/backup.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/bf_fixed.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/bf.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/bf_hashtable.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/bf_s.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/bf_transit_bucket.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/bf_tree.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/btcursor.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/btree.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/btree_impl.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/btree_logrec.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/btree_page.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/btree_page_h.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/chkpt.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/crash.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/device.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/fixable_page_h.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/generic_page.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lid.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lock_bucket.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lock_cache.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lock_compt.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lock_core.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lock.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lock_lil.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lock_raw.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lock_s.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/lock_x.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/log_carray.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/log_core.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/log_spr.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/log_lsn_tracker.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/logrec.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/partition.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/pmap.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/prologue.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/restart.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/restart_s.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_base.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_external.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_0.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_0.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_2.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_3.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_int_4.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_options.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_s.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/smstats.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/smthread.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sm_vtable_enum.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/stnode_page.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/sysdefs.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/vol.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/xct_dependent.h
#   ${CMAKE_CURRENT_SOURCE_DIR}/xct.h
#   DESTINATION include)

