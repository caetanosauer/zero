set(loginspect_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/logcat.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/agglog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/verifylog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/truncatelog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logstats.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logpagestats.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dbinspect.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/loganalysis.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/propstats.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dbscan.cpp
    )

add_library (loginspect ${loginspect_SRCS})
