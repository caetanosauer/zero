# Find the google tcmalloc library.
# Output variables:
#  TCMALLOC_INCLUDE_DIR : e.g., /usr/include/.
#  TCMALLOC_LIBRARY     : Library path of tcmalloc.
#  TCMALLOC_FOUND       : True if found.
FIND_PATH(TCMALLOC_INCLUDE_DIR NAME google/tcmalloc.h
  PATHS /opt/local/include /usr/local/include /usr/include)

FIND_LIBRARY(TCMALLOC_LIBRARY NAME tcmalloc
  PATHS /usr/lib64 /usr/local/lib64 /opt/local/lib64 /usr/lib /usr/local/lib /opt/local/lib
)

IF (TCMALLOC_INCLUDE_DIR AND TCMALLOC_LIBRARY)
    SET(TCMALLOC_FOUND TRUE)
    MESSAGE(STATUS "Found google tcmalloc: inc=${TCMALLOC_INCLUDE_DIR}, lib=${TCMALLOC_LIBRARY}")
ELSE ()
    SET(TCMALLOC_FOUND FALSE)
    MESSAGE(STATUS "WARNING: Google tcmalloc not found. Using plain malloc instead (which is slower on multi-threads).")
    MESSAGE(STATUS "Try: 'sudo yum install gperftools-devel' (or apt-get)")
ENDIF ()
