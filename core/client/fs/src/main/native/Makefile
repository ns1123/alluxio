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

OS = $(shell echo $(shell uname) | tr A-Z a-z)
ARCH = $(shell uname -m)

LIB_ALLUXIO_OBJ = liballuxio.o
ALLUXIO_NATIVE_LIB_DIR = $(abspath $(shell pwd)/../../../../../..)/lib/native
OPENSSL_CIPHER_C = src/alluxio/client/security/OpenSSLCipher.c

ifeq ($(OS), linux)
  JAVA_HOME = $(shell dirname $(shell dirname $(shell readlink -f $(shell which javac))))
  LIB_ALLUXIO = liballuxio.so
  OPENSSL_INCLUDE = /usr/include
  ifeq ($(ARCH), x86_64)
    OPENSSL_LIB = /usr/lib64
  else
    OPENSSL_LIB = /usr/lib
  endif
endif

ifeq ($(OS), darwin)
  JAVA_HOME = $(shell /usr/libexec/java_home)
  LIB_ALLUXIO = liballuxio.dylib
  OPENSSL_INCLUDE = /usr/local/opt/openssl/include
  OPENSSL_LIB = /usr/local/opt/openssl/lib
endif

all: liballuxio

liballuxio:
	gcc -fPIC \
		-I $(JAVA_HOME)/include \
    -I $(JAVA_HOME)/include/${OS} \
    -I $(OPENSSL_INCLUDE) \
    -c $(OPENSSL_CIPHER_C) \
    -o $(LIB_ALLUXIO_OBJ)
	mkdir -p $(ALLUXIO_NATIVE_LIB_DIR)
	gcc -fPIC -shared \
		-L $(OPENSSL_LIB) \
		-lcrypto \
		$(LIB_ALLUXIO_OBJ) \
		-o $(ALLUXIO_NATIVE_LIB_DIR)/$(LIB_ALLUXIO)
	rm -f $(LIB_ALLUXIO_OBJ)

clean:
	rm -f $(ALLUXIO_NATIVE_LIB_DIR)/$(LIB_ALLUXIO)
