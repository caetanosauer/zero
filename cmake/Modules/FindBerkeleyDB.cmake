# Copied from https://github.com/hawkinsp/ZTopo/ then modified.
# This module is distributed with BSD license.
#
# - Try to find Berkeley DB
# Once done this will define
#
#  BERKELEY_DB_FOUND - system has Berkeley DB
#  BERKELEY_DB_INCLUDE_DIR - the Berkeley DB include directory
#  BERKELEY_DB_LIBRARIES_C - Link these to use Berkeley DB for C
#  BERKELEY_DB_LIBRARIES_CXX - Link these to use Berkeley DB for C++

# Copyright (c) 2006, Alexander Dymo, <adymo@kdevelop.org>
#
# Redistribution and use is allowed according to the terms of the BSD license.

FIND_PATH(BERKELEY_DB_INCLUDE_DIR NAMES db.h
  PATHS
  /usr/include # older Linux distro puts it directly under here.
  /usr/include/db4
  /usr/include/libdb4 # newer Linux distro puts it here.
  /usr/local/include
  /usr/local/include/db4 
  /usr/local/include/libdb4
)

# retrieve version information from the header
file(READ "${BERKELEY_DB_INCLUDE_DIR}/db.h" DB_H_FILE)
string(REGEX REPLACE ".*#define[ \t]+DB_VERSION_MAJOR[ \t]+([0-9]+).*"      "\\1" BERKELEYDB_VERSION_MAJOR "${DB_H_FILE}")
string(REGEX REPLACE ".*#define[ \t]+DB_VERSION_MINOR[ \t]+([0-9]+).*"      "\\1" BERKELEYDB_VERSION_MINOR "${DB_H_FILE}")

SET(BERKELEY_DB_LIB_SEARCH_DIR /usr/lib /usr/local/lib)
if(CMAKE_SIZEOF_VOID_P EQUAL 8)
  SET(BERKELEY_DB_LIB_SEARCH_DIR /usr/lib64 /usr/local/lib64)
endif()
# C version and C++ versions are quite different.
# If you get "underfined reference" linker errors,
# check if you use the right one.
FIND_LIBRARY(BERKELEY_DB_LIBRARIES_C
  NAMES "db-${BERKELEYDB_VERSION_MAJOR}.${BERKELEYDB_VERSION_MINOR}"
  PATHS ${BERKELEY_DB_LIB_SEARCH_DIR}
  )

FIND_LIBRARY(BERKELEY_DB_LIBRARIES_CXX
  NAMES "db_cxx-${BERKELEYDB_VERSION_MAJOR}.${BERKELEYDB_VERSION_MINOR}"
  PATHS ${BERKELEY_DB_LIB_SEARCH_DIR}
  )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(BerkeleyDB "Could not find Berkeley DB" BERKELEY_DB_INCLUDE_DIR BERKELEY_DB_LIBRARIES_C BERKELEY_DB_LIBRARIES_CXX)
MARK_AS_ADVANCED(BERKELEY_DB_INCLUDE_DIR BERKELEY_DB_LIBRARIES_C BERKELEY_DB_LIBRARIES_CXX)
