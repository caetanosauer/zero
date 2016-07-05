INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/src/cmd/base
  ${CMAKE_SOURCE_DIR}/src/cmd/restore
  ${CMAKE_SOURCE_DIR}/src/cmd/kits
  ${CMAKE_SOURCE_DIR}/src/cmd/webserver
  ${CMAKE_SOURCE_DIR}/src/cmd/loginspect
  ${CMAKE_SOURCE_DIR}/src/cmd
  ${CMAKE_SOURCE_DIR}/config
  ${CMAKE_SOURCE_DIR}/src/sm
  ${CMAKE_SOURCE_DIR}/src/common
)
# to look for generated headers
INCLUDE_DIRECTORIES (
  ${CMAKE_BINARY_DIR}/config
  ${CMAKE_BINARY_DIR}/config
  ${CMAKE_BINARY_DIR}/src/sm
  ${CMAKE_BINARY_DIR}/src/common
)

set(webserver_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/http.cpp
    )

add_library (webserverLib ${webserver_SRCS})

add_executable(webserver main.cpp)

set(webserver_LIBS
    # zapps components
    webserverLib
    kits
    zapps_base
    restore
    loginspect
    # Zero/Shore libraries
    sm
    common
    # third-party dependencies
    pthread
)

target_link_libraries(webserver ${webserver_LIBS})
