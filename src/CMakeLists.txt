macro(INSTALL_GENERATED_H)
  set(ig_tmp)
  foreach(tmp ${ARGV})
    list(APPEND ig_tmp ${CMAKE_CURRENT_BINARY_DIR}/${tmp})
  endforeach(tmp)
  install(FILES ${ig_tmp} DESTINATION include)
endmacro(INSTALL_GENERATED_H)

INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/config
  ${CMAKE_SOURCE_DIR}/src/atomic_ops
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

add_subdirectory(atomic_ops)
add_subdirectory(fc)
add_subdirectory(sthread)
add_subdirectory(common)
add_subdirectory(sm)

########### install files ###############




#original Makefile.am contents follow:

### Process this file with automake to produce Makefile.in
#include $(top_srcdir)/Makefile.generic
#SUBDIRS = \
#	  atomic_ops  \
#	  fc \
#	  sthread \
#	  common \
#	  sm 
#
### Doxygen documentation files. NOT included in sources.
#EXTRA_DIST =  api.h references.h internal.h mainpage.h
