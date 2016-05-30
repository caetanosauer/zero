set(webserver_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/http.cpp
    )

add_library (webserver ${webserver_SRCS})
