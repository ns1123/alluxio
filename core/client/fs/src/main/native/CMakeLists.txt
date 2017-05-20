#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

cmake_minimum_required(VERSION 2.6)

project(liballuxio)

set(LIB_ALLUXIO_NAME alluxio)
set(LIB_ALLUXIO_VERSION_MAJOR 0)
set(LIB_ALLUXIO_VERSION_MINOR 1)
set(LIB_ALLUXIO_VERSION_PATCH 0)

add_library(${LIB_ALLUXIO_NAME} SHARED src/alluxio/client/security/OpenSSLCipher.c)
set_target_properties(${LIB_ALLUXIO_NAME} PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  VERSION ${LIB_ALLUXIO_VERSION_MAJOR}.${LIB_ALLUXIO_VERSION_MINOR}.${LIB_ALLUXIO_VERSION_PATCH}
  SOVERSION ${LIB_ALLUXIO_VERSION_MAJOR}
  LIBRARY_OUTPUT_DIRECTORY ../../../../../../../lib/native
)
find_package(JNI REQUIRED)
find_package(OpenSSL REQUIRED)
include_directories(
  ${JNI_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIR}
)
target_link_libraries(${LIB_ALLUXIO_NAME}
  ${OPENSSL_CRYPTO_LIBRARY}
)
