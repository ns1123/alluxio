/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

#include <jni.h>
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <string.h>

#define AES_GCM_TAG_LEN 16

#define THROW(env, exceptionName, message) \
{ \
  jclass ecls = (*env)->FindClass(env, exceptionName); \
  if (ecls) { \
    (*env)->ThrowNew(env, ecls, message); \
    (*env)->DeleteLocalRef(env, ecls); \
  } \
}

#define ERROR(env, message) \
  THROW(env, "java/security/GeneralSecurityException", message)

#define BYTE_ARRAY_LENGTH(env, array) \
  (*env)->GetArrayLength(env, array)

#define GET_CHAR_ARRAY(env, array) \
  (unsigned char*)(*env)->GetByteArrayElements(env, array, NULL)

#define RELEASE_CHAR_ARRAY(env, jArray, cArray, mode) \
  (*env)->ReleaseByteArrayElements(env, jArray, (jbyte*)cArray, mode)

/**
 * Returns the appropriate type of AES/GCM cipher according to the key length.
 */
const EVP_CIPHER* cipher(JNIEnv *env, jbyteArray key) {
  switch (BYTE_ARRAY_LENGTH(env, key)) {
  case 16:
    return EVP_aes_128_gcm();
  case 24:
    return EVP_aes_192_gcm();
  case 32:
    return EVP_aes_256_gcm();
  default:
    ERROR(env, "AES GCM only supports key with 128, 192, and 256 bits");
  }
  return NULL;
}

/**
 * The exported native method for encryption.
 */
JNIEXPORT jint JNICALL Java_alluxio_client_security_OpenSSLCipher_encrypt(JNIEnv *env, jobject obj,
    jbyteArray plaintext, jint plaintextOffset, jint plaintextLen, jbyteArray key, jbyteArray iv,
    jbyteArray ciphertext, jint ciphertextOffset)
{
  EVP_CIPHER_CTX *ctx;
  int len;
  int ciphertextLen;

  unsigned char* keyBuf = GET_CHAR_ARRAY(env, key);
  unsigned char* ivBuf = GET_CHAR_ARRAY(env, iv);
  unsigned char* ciphertextBuf = malloc((plaintextLen + AES_GCM_TAG_LEN) * sizeof(unsigned char));
  unsigned char* plaintextBuf = malloc(plaintextLen * sizeof(unsigned char));
  (*env)->GetByteArrayRegion(env, plaintext, plaintextOffset, plaintextLen,
      (jbyte*)plaintextBuf);

  // Create a new cipher context.
  if(!(ctx = EVP_CIPHER_CTX_new()))
      ERROR(env, "Failed to create a new cipher context");

  // Set the cipher type.
  if(1 != EVP_EncryptInit_ex(ctx, cipher(env, key), NULL, NULL, NULL))
      ERROR(env, "Failed to set the cipher type");
  // Set the IV length.
  if (1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, BYTE_ARRAY_LENGTH(env, iv), NULL))
      ERROR(env, "Failed to set the IV length");
  // Set the key and IV.
  if(1 != EVP_EncryptInit_ex(ctx, NULL, NULL, keyBuf, ivBuf))
      ERROR(env, "Failed to set the key and IV");
  // Disable padding.
  if (1 != EVP_CIPHER_CTX_set_padding(ctx, 0))
      ERROR(env, "Failed to disable padding");

  // Encrypt plaintext.
  if(1 != EVP_EncryptUpdate(ctx, ciphertextBuf, &len, plaintextBuf, plaintextLen))
      ERROR(env, "Failed to encrypt the plaintext");
  ciphertextLen = len;

  // Finalize the encryption.
  if(1 != EVP_EncryptFinal_ex(ctx, ciphertextBuf + ciphertextLen, &len))
      ERROR(env, "Failed to finalize the encryption");
  ciphertextLen += len;

  // Append the autentication tag.
  if (1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, AES_GCM_TAG_LEN,
      ciphertextBuf + ciphertextLen))
      ERROR(env, "Failed to add the authentication tag");
  ciphertextLen += AES_GCM_TAG_LEN;

  // Copy back the ciphertextBuf into the java byte array ciphertext.
  (*env)->SetByteArrayRegion(env, ciphertext, ciphertextOffset, ciphertextLen,
      (const jbyte*)ciphertextBuf);

  // Clean up.
  RELEASE_CHAR_ARRAY(env, key, keyBuf, JNI_ABORT);
  RELEASE_CHAR_ARRAY(env, iv, ivBuf, JNI_ABORT);
  free(ciphertextBuf);
  free(plaintextBuf);
  EVP_CIPHER_CTX_free(ctx);

  return ciphertextLen;
}

/**
 * The exported native method for decryption.
 */
JNIEXPORT jint JNICALL Java_alluxio_client_security_OpenSSLCipher_decrypt(JNIEnv *env, jobject obj,
    jbyteArray ciphertext, jint ciphertextOffset, jint ciphertextLen, jbyteArray key,
    jbyteArray iv, jbyteArray plaintext, jint plaintextOffset)
{
  EVP_CIPHER_CTX *ctx;
  int len;
  int plaintextLen;

  unsigned char* keyBuf = GET_CHAR_ARRAY(env, key);
  unsigned char* ivBuf = GET_CHAR_ARRAY(env, iv);
  unsigned char* plaintextBuf = malloc((ciphertextLen - AES_GCM_TAG_LEN) * sizeof(unsigned char));
  unsigned char* ciphertextBuf = malloc(ciphertextLen * sizeof(unsigned char));
  (*env)->GetByteArrayRegion(env, ciphertext, ciphertextOffset, ciphertextLen,
      (jbyte*)ciphertextBuf);
  ciphertextLen -= AES_GCM_TAG_LEN;

  // Create a new cipher context.
  if(!(ctx = EVP_CIPHER_CTX_new()))
      ERROR(env, "Failed to create a new cipher context");

  // Set the cipher type.
  if(1 != EVP_DecryptInit_ex(ctx, cipher(env, key), NULL, NULL, NULL))
      ERROR(env, "Failed to set the cipher type");
  // Set the IV length.
  if (1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, BYTE_ARRAY_LENGTH(env, iv), NULL))
      ERROR(env, "Failed to set the IV length");
  // Set the key and IV.
  if(1 != EVP_DecryptInit_ex(ctx, NULL, NULL, keyBuf, ivBuf))
      ERROR(env, "Failed to set the key and IV");
  // Disable padding.
  if (1 != EVP_CIPHER_CTX_set_padding(ctx, 0))
      ERROR(env, "Failed to disable padding");

  // Set the expected authentication tag.
  if(1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, AES_GCM_TAG_LEN,
      ciphertextBuf + ciphertextLen))
      ERROR(env, "Failed to set the expected authentication tag");

  // Decrypt ciphertext.
  if(1 != EVP_DecryptUpdate(ctx, plaintextBuf, &len, ciphertextBuf, ciphertextLen))
      ERROR(env, "Failed to decrypt the ciphertext");
  plaintextLen = len;

  // Finalize the decryption, verify the authentication tag.
  if (1 != EVP_DecryptFinal_ex(ctx, plaintextBuf + len, &len))
      ERROR(env, "Failed to match the authentication tag");
  plaintextLen += len;

  // Copy back the plaintextBuf into the java byte array plaintext.
  (*env)->SetByteArrayRegion(env, plaintext, plaintextOffset, plaintextLen,
      (const jbyte*)plaintextBuf);

  // Clean up.
  RELEASE_CHAR_ARRAY(env, key, keyBuf, JNI_ABORT);
  RELEASE_CHAR_ARRAY(env, iv, ivBuf, JNI_ABORT);
  free(plaintextBuf);
  free(ciphertextBuf);
  EVP_CIPHER_CTX_free(ctx);

  return plaintextLen;
}
