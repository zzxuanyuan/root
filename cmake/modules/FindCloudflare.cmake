# Find the CLOUDFLARE includes and library.
#
# This module defines
# CLOUDFLARE_INCLUDE_DIR, where to locate CLOUDFLARE header files
# CLOUDFLARE_LIBRARIES, the libraries to link against to use cloudflare
# CLOUDFLARE_FOUND.  If false, you cannot build anything that requires cloudflare.

if(CLOUDFLARE_CONFIG_EXECUTABLE)
  set(CLOUDFLARE_FIND_QUIETLY 1)
endif()
set(CLOUDFLARE_FOUND 0)

find_path(CLOUDFLARE_INCLUDE_DIR zlib.h
  $ENV{CLOUDFLARE_DIR}/include
  /usr/local/include
  /usr/include/cloudflare
  /usr/local/include/cloudflare
  /opt/cloudflare/include
  DOC "Specify the directory containing zlib.h"
)

find_library(CLOUDFLARE_LIBRARY NAMES cloudflare PATHS
  $ENV{CLOUDFLARE_DIR}/lib
  /usr/local/cloudflare/lib
  /usr/local/lib
  /usr/lib/cloudflare
  /usr/local/lib/cloudflare
  /usr/cloudflare/lib /usr/lib
  /usr/cloudflare /usr/local/cloudflare
  /opt/cloudflare /opt/cloudflare/lib
  DOC "Specify the cloudflare library here."
)

if(CLOUDFLARE_INCLUDE_DIR AND CLOUDFLARE_LIBRARY)
  set(CLOUDFLARE_FOUND 1 )
  if(NOT CLOUDFLARE_FIND_QUIETLY)
     message(STATUS "Found CLOUDFLARE includes at ${CLOUDFLARE_INCLUDE_DIR}")
     message(STATUS "Found CLOUDFLARE library at ${CLOUDFLARE_LIBRARY}")
  endif()
endif()

set(CLOUDFLARE_LIBRARIES ${CLOUDFLARE_LIBRARY})
mark_as_advanced(CLOUDFLARE_FOUND CLOUDFLARE_LIBRARY CLOUDFLARE_INCLUDE_DIR)
