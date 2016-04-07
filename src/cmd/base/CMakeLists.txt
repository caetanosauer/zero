set(zapps_base_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/basethread.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/command.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/iterator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/scanner.cpp
    )

add_library (zapps_base ${zapps_base_SRCS})
