#include <jni.h>
#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <string.h>

#define GCM_TAG_LEN 16

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

#define GET_CHAR_ARRAY(env, array) \
  (unsigned char*)(*env)->GetByteArrayElements(env, array, NULL)

#define RELEASE_CHAR_ARRAY(env, jArray, cArray) \
  (*env)->ReleaseByteArrayElements(env, jArray, (jbyte*)cArray, 0);

JNIEXPORT jint JNICALL Java_alluxio_client_security_OpenSSLCipher_encrypt(JNIEnv *env, jobject obj,
    jbyteArray plaintext, jint plaintextOffset, jint plaintextLen, jbyteArray key, jbyteArray iv,
    jbyteArray ciphertext, jint ciphertextOffset)
{
  EVP_CIPHER_CTX *ctx;
  int len;
  int ciphertextLen;

  unsigned char* keyBuf = GET_CHAR_ARRAY(env, key);
  unsigned char* ivBuf = GET_CHAR_ARRAY(env, iv);
  unsigned char* plaintextBuf = GET_CHAR_ARRAY(env, plaintext);
  unsigned char* plaintextBufStart = plaintextBuf + plaintextOffset;
  unsigned char* ciphertextBuf = GET_CHAR_ARRAY(env, ciphertext);
  unsigned char* ciphertextBufStart = ciphertextBuf + ciphertextOffset;

  // Create a new cipher context.
  if(!(ctx = EVP_CIPHER_CTX_new()))
      ERROR(env, "Failed to create a new cipher context");

  // Set the cipher type.
  if(1 != EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, NULL, NULL))
      ERROR(env, "Failed to set the cipher type");
  // Set the IV length.
  if (1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, sizeof(ivBuf), NULL))
      ERROR(env, "Failed to set the IV length");
  // Set the key and IV.
  if(1 != EVP_EncryptInit_ex(ctx, NULL, NULL, keyBuf, ivBuf))
      ERROR(env, "Failed to set the key and IV");
  // Disable padding.
  if (1 != EVP_CIPHER_CTX_set_padding(ctx, 0))
      ERROR(env, "Failed to disable padding");

  // Encrypt plaintext.
  if(1 != EVP_EncryptUpdate(ctx, ciphertextBufStart, &len, plaintextBufStart, plaintextLen))
      ERROR(env, "Failed to encrypt the plaintext");
  ciphertextLen = len;

  // Finalize the encryption.
  if(1 != EVP_EncryptFinal_ex(ctx, ciphertextBufStart + ciphertextLen, &len)) 
      ERROR(env, "Failed to finalize the encryption");
  ciphertextLen += len;

  // Append the autentication tag.
  if (1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, GCM_TAG_LEN,
      ciphertextBufStart + ciphertextLen))
      ERROR(env, "Failed to add the authentication tag");
  ciphertextLen += GCM_TAG_LEN;

  // Free the context.
  EVP_CIPHER_CTX_free(ctx);

  // Copy back the content and release the native array.
  RELEASE_CHAR_ARRAY(env, key, keyBuf);
  RELEASE_CHAR_ARRAY(env, iv, ivBuf);
  RELEASE_CHAR_ARRAY(env, plaintext, plaintextBuf);
  RELEASE_CHAR_ARRAY(env, ciphertext, ciphertextBuf);

  return ciphertextLen;
}

JNIEXPORT jint JNICALL Java_alluxio_client_security_OpenSSLCipher_decrypt(JNIEnv *env, jobject obj,
    jbyteArray ciphertext, jint ciphertextOffset, jint ciphertextLen, jbyteArray key,
    jbyteArray iv, jbyteArray plaintext, jint plaintextOffset)
{
  EVP_CIPHER_CTX *ctx;
  int len;
  int plaintextLen;

  unsigned char* keyBuf = GET_CHAR_ARRAY(env, key);
  unsigned char* ivBuf = GET_CHAR_ARRAY(env, iv);
  unsigned char* plaintextBuf = GET_CHAR_ARRAY(env, plaintext);
  unsigned char* plaintextBufStart = plaintextBuf + plaintextOffset;
  unsigned char* ciphertextBuf = GET_CHAR_ARRAY(env, ciphertext);
  unsigned char* ciphertextBufStart = ciphertextBuf + ciphertextOffset;
  ciphertextLen -= GCM_TAG_LEN;

  // Create a new cipher context.
  if(!(ctx = EVP_CIPHER_CTX_new()))
      ERROR(env, "Failed to create a new cipher context");

  // Set the cipher type.
  if(1 != EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, NULL, NULL))
      ERROR(env, "Failed to set the cipher type");
  // Set the IV length.
  if (1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, sizeof(ivBuf), NULL))
      ERROR(env, "Failed to set the IV length");
  // Set the key and IV.
  if(1 != EVP_DecryptInit_ex(ctx, NULL, NULL, keyBuf, ivBuf))
      ERROR(env, "Failed to set the key and IV");
  // Disable padding.
  if (1 != EVP_CIPHER_CTX_set_padding(ctx, 0))
      ERROR(env, "Failed to disable padding");

  // Set the expected authentication tag.
  if(1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, GCM_TAG_LEN,
      ciphertextBufStart + ciphertextLen))
      ERROR(env, "Failed to set the expected authentication tag");

  // Decrypt ciphertext.
  if(1 != EVP_DecryptUpdate(ctx, plaintextBufStart, &len, ciphertextBufStart, ciphertextLen))
      ERROR(env, "Failed to decrypt the ciphertext");
  plaintextLen = len;

  // Finalize the decryption, verify the authentication tag.
  if (1 != EVP_DecryptFinal_ex(ctx, plaintextBufStart + len, &len))
      ERROR(env, "Failed to match the authentication tag");
  plaintextLen += len;

  // Free the context.
  EVP_CIPHER_CTX_free(ctx);

  // Copy back the content and release the native array.
  RELEASE_CHAR_ARRAY(env, key, keyBuf);
  RELEASE_CHAR_ARRAY(env, iv, ivBuf);
  RELEASE_CHAR_ARRAY(env, plaintext, plaintextBuf);
  RELEASE_CHAR_ARRAY(env, ciphertext, ciphertextBuf);

  return plaintextLen;
}
