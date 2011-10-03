
INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/config
  ${CMAKE_SOURCE_DIR}/src/atomic_ops
  ${CMAKE_SOURCE_DIR}/src/fc
  ${CMAKE_SOURCE_DIR}/src/common
  ${CMAKE_SOURCE_DIR}/src/sthread
  ${CMAKE_SOURCE_DIR}/src/sm
) 

add_executable(example1 ${CMAKE_CURRENT_SOURCE_DIR}/example1.cpp)
target_link_libraries(example1 common fc sthread sm atomic_ops)

add_executable(example2 ${CMAKE_CURRENT_SOURCE_DIR}/example2.cpp)
target_link_libraries(example2 common fc sthread sm atomic_ops)
