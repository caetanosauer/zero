# global include directories
INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/config
  ${CMAKE_SOURCE_DIR}/src/fc
  ${CMAKE_SOURCE_DIR}/src/common
  ${CMAKE_SOURCE_DIR}/src/sthread
  ${CMAKE_SOURCE_DIR}/src/sm

  # For auto-generated files
  ${CMAKE_BINARY_DIR}/src/fc
  ${CMAKE_BINARY_DIR}/src/common
  ${CMAKE_BINARY_DIR}/src/sthread
  ${CMAKE_BINARY_DIR}/src/sm
) 

# to look for generated headers
INCLUDE_DIRECTORIES (
  ${CMAKE_BINARY_DIR}/config
  ${CMAKE_BINARY_DIR}/src/fc
  ${CMAKE_BINARY_DIR}/src/common
  ${CMAKE_BINARY_DIR}/src/sthread
  ${CMAKE_BINARY_DIR}/src/sm
)

add_executable(example1 ${CMAKE_CURRENT_SOURCE_DIR}/example1.cpp)
target_link_libraries(example1 common fc sthread sm)

add_executable(example2 ${CMAKE_CURRENT_SOURCE_DIR}/example2.cpp)
target_link_libraries(example2 common fc sthread sm)
