INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

set(common_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/worker_thread.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_debug.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/block_alloc.cpp # unifiy all mem mgmt code (2 below too)
  ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/mem_block.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/w_base.cpp # cleanup and remove if possible (lots of std numeric stuff)
  ${CMAKE_CURRENT_SOURCE_DIR}/w_listm.cpp # remove all uses and throw away
  ${CMAKE_CURRENT_SOURCE_DIR}/tls.cpp # cleanup (at least macros) and leave only memory manager stuff
  ${CMAKE_CURRENT_SOURCE_DIR}/basics.cpp # cleanup unused parts; move used parts elsewhere
  ${CMAKE_CURRENT_SOURCE_DIR}/lsn.cpp # unify with FL's new lsn_t
  ${CMAKE_CURRENT_SOURCE_DIR}/vec_t.cpp)

add_library(common STATIC ${common_SRCS})
target_link_libraries (common pthread rt)

