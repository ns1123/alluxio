# TODO(cc): support both linux and mac

all: liballuxio

JAVA_HOME = `/usr/libexec/java_home`
OPENSSL_HOME = /usr/local/opt/openssl
OPENSSL_CIPHER_C = src/alluxio/client/security/OpenSSLCipher.c
LIB_ALLUXIO = liballuxio

liballuxio:
	gcc -I ${JAVA_HOME}/include \
    -I ${JAVA_HOME}/include/darwin \
    -I ${OPENSSL_HOME}/include \
    -c ${OPENSSL_CIPHER_C} \
    -o ${LIB_ALLUXIO}.o
	gcc -shared \
    -L ${OPENSSL_HOME}/lib \
    -lcrypto \
    ${LIB_ALLUXIO}.o \
		-o ${LIB_ALLUXIO}.dylib
	rm -f ${LIB_ALLUXIO}.o

clean:
	rm -f ${LIB_ALLUXIO}.dylib
