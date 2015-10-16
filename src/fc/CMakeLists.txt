
set(fc_STAT_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/rand48.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/block_alloc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/mem_block.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gethrtime.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_compat_strstream.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stime.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vtable.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_base.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/w_listm.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tls.cpp)

add_library(fc STATIC ${fc_STAT_SRCS})
target_link_libraries (fc rt)

########### install files ###############

install(FILES
#  ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops.h
#  ${CMAKE_CURRENT_SOURCE_DIR}/atomic_ops_builtin.h
#  ${CMAKE_CURRENT_SOURCE_DIR}/atomic_templates.h
  ${CMAKE_CURRENT_SOURCE_DIR}/atomic_container.h
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
  ${CMAKE_CURRENT_SOURCE_DIR}/w_endian.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_error.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_error_xmacro.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_fill.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_findprime.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_hash.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_heap.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_key.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_list.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_lockfree_list.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_markable_pointer.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_minmax.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_okvl.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_rc.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_stream.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_strstream.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_stat.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_workaround.h
  DESTINATION include)
