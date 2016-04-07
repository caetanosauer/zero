# global include directories
INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/config
  ${CMAKE_SOURCE_DIR}/src/common
  ${CMAKE_SOURCE_DIR}/src/sm
  ${CMAKE_SOURCE_DIR}/src/cmd
) 

# to look for generated headers
INCLUDE_DIRECTORIES (
  ${CMAKE_BINARY_DIR}/config
  ${CMAKE_BINARY_DIR}/src/common
  ${CMAKE_BINARY_DIR}/src/sm
  ${CMAKE_BINARY_DIR}/src/cmd
)

INCLUDE_DIRECTORIES (${CMAKE_SOURCE_DIR}/gtest-1.7.0/include)

IF (COMPILER_IS_GCC)
    # strip out unused data/function. this also removes -rdynamic
    SET(CMAKE_SHARED_LIBRARY_LINK_C_FLAGS "-fdata-sections -ffunction-sections")
    SET(CMAKE_SHARED_LIBRARY_LINK_CXX_FLAGS "-fdata-sections -ffunction-sections")
ENDIF (COMPILER_IS_GCC)

add_subdirectory(common)
add_subdirectory(sm)
