
set(common_STAT_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/basics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/bitmap.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/devid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/latch.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/kvl_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lsn.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tid_t.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vec_mkchunk.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/vec_t.cpp)

add_library(common STATIC ${common_STAT_SRCS})
#target_link_libraries (common)

########### install files ###############

install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/FairRWLatch.h
  ${CMAKE_CURRENT_SOURCE_DIR}/basics.h
  ${CMAKE_CURRENT_SOURCE_DIR}/bitmap.h
  ${CMAKE_CURRENT_SOURCE_DIR}/devid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/lsn.h
  ${CMAKE_CURRENT_SOURCE_DIR}/kvl_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/latch.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sm_vas.h
  ${CMAKE_CURRENT_SOURCE_DIR}/stid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/tid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/umemcmp.h
  ${CMAKE_CURRENT_SOURCE_DIR}/vec_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/vid_t.h
  ${CMAKE_CURRENT_SOURCE_DIR}/w_opaque.h
  ${GENFILES} DESTINATION include)
