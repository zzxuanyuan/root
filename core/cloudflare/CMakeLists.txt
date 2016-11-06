############################################################################
# CMakeLists.txt file for building ROOT core/cloudflare package
############################################################################


#---The builtin Cloudflare library is built using the CMake ExternalProject standard module
#   in cmake/modules/SearchInstalledSoftare.cmake

#---Declare ZipCloudflare sources as part of libCore-------------------------------
set(headers ${CMAKE_CURRENT_SOURCE_DIR}/inc/ZipCloudflare.h)
set(sources ${CMAKE_CURRENT_SOURCE_DIR}/src/ZipCloudflare.c)


include_directories(${CLOUDFLARE_INCLUDE_DIR})
ROOT_OBJECT_LIBRARY(Cloudflare ${sources})

if(builtin_cloudflare)
  add_dependencies(Cloudflare CLOUDFLARE)
endif()

ROOT_INSTALL_HEADERS()
install(FILES ${CLOUDFLARE_headers} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
