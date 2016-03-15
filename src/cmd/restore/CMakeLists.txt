set(restore_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/genarchive.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/mergeruns.cpp
    )

add_library (restore ${restore_SRCS})
