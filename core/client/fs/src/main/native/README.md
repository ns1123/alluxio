# Alluxio native library

This repo contains C code for methods exported through JNI. It can be built into a platform
dependent shared library, which is liballuxio.so on Linux and liballuxio.dylib on macOS.

## Dependency

- [OpenSSL](https://www.openssl.org/) libcrypto
- JDK with JNI support
- [CMake](https://cmake.org)
- make

## Code Structure

- src/alluxio/client/security/OpenSSLCipher.c:
  JNI interface for Java class alluxio.client.security.OpenSSLCipher
  
## Build

`./build` will build the shared library and put it under ${alluxio.home}/lib/native, the generated
cmake directory contains files generated from CMakeLists.txt by `cmake`.
