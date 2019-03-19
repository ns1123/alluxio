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

package alluxio.security.kms.hadoop;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.proto.security.EncryptionProto;
import alluxio.security.authentication.AuthType;
import alluxio.security.kms.KmsClient;
import alluxio.util.proto.ProtoUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/**
 * Hadoop KMS client.
 *
 * If the Hadoop KMS uses kerberos authentication, {@link HadoopKmsClient} uses the principal and
 * keytab file specified in
 * {@link PropertyKey#SECURITY_KERBEROS_CLIENT_PRINCIPAL} and
 * {@link PropertyKey#SECURITY_KERBEROS_CLIENT_KEYTAB_FILE} for authentication.
 *
 * If the Hadoop KMS uses TLS/SSL, make sure the KMS certificate is available in
 * ${JAVA_HOME}/jre/lib/security/cacerts.
 *
 * The user authenticated to the KMS should have the permission to get and create keys.
 *
 * @see <a href="http://hadoop.apache.org/docs/r2.7.3/hadoop-kms/index.html">hadoop-kms
 * documentation</a> for more details on Hadoop KMS.
 */
public class HadoopKmsClient implements KmsClient {
  // Add prefix to the Alluxio crypto keys to distinguish from Hadoop crypto keys.
  private static final String ALLUXIO_KEY_NAME_PREFIX = "alluxio:";
  private static final String SHA1PRNG = "SHA1PRNG";
  // Initialization vector is stored as an attribute in the Hadoop KMS keys.
  private static final String ATTRIBUTE_IV = "IV";
  private static final int KEY_BIT_LENGTH = 256;
  private static final EncryptionProto.CryptoKey.Builder CRYPTO_KEY_PARTIAL_BUILDER =
      EncryptionProto.CryptoKey.newBuilder().setGenerationId("").setNeedsAuthTag(1);

  /**
   * Creates a new {@link HadoopKmsClient}.
   */
  public HadoopKmsClient() {}

  /**
   * Gets a {@link EncryptionProto.CryptoKey}.
   *
   * Names of Alluxio crypto keys in Hadoop KMS are in the format alluxio:{inputKey}.
   *
   * If a key with name alluxio:{inputKey} already exists in Hadoop KMS, the key is retrieved and
   * transformed to {@link EncryptionProto.CryptoKey}.
   *
   * Otherwise, if encrypt is true, a key and an initialization vector are generated by
   * {@link HadoopKmsClient}, a key creation request is sent to Hadoop KMS to store them, and an
   * {@link EncryptionProto.CryptoKey} will be created based on the generated key and IV;
   *
   * If encrypt is false, an {@link IOException} will be thrown indicating that no key with name
   * alluxio:{inputKey} exists.
   *
   * Note:
   * 1. Hadoop KMS does not provide generation ID, so the generation ID in the returned
   * {@link EncryptionProto.CryptoKey} is empty.
   * 2. NeedsAuthTag is always set to 1.
   *
   * @param kms the KMS endpoint in Hadoop KMS URI format like kms://{PROTO}@{HOST}:{PORT}/{PATH}
   * @param encrypt whether the key is for encryption or decryption
   * @param inputKey name of the key to be retrieved
   * @param alluxioConf Alluxio configuration
   * @return the crypto key
   * @throws IOException when the crypto key fails to be created
   */
  @Override
  public EncryptionProto.CryptoKey getCryptoKey(String kms, boolean encrypt, String inputKey,
      AlluxioConfiguration alluxioConf) throws IOException {
    URI kmsEndpoint;
    try {
      kmsEndpoint = new URI(kms);
    } catch (URISyntaxException e) {
      throw new IOException("Invalid hadoop KMS endpoint format", e);
    }

    Configuration hadoopConf = new Configuration();
    if (alluxioConf.getBoolean(PropertyKey.SECURITY_KMS_KERBEROS_ENABLED)) {
      String principal = alluxioConf.get(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
      String keytabFile = alluxioConf.get(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
      if (!principal.isEmpty() && !keytabFile.isEmpty()) {
        // Login Hadoop with Alluxio client kerberos principal and keytab.
        hadoopConf.set("hadoop.security.authentication", AuthType.KERBEROS.getAuthName());
        UserGroupInformation.setConfiguration(hadoopConf);
        UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
      }
    }

    KeyProvider keyProvider = KMSClientProvider.Factory.get(kmsEndpoint, hadoopConf);
    String keyName = ALLUXIO_KEY_NAME_PREFIX + inputKey;
    KeyProvider.KeyVersion keyVersion = keyProvider.getCurrentKey(keyName);
    if (keyVersion == null) {
      // The key does not exist.
      if (encrypt) {
        try {
          return createKey(keyProvider, keyName);
        } catch (IOException e) {
          // The key may have been created between the call to getCurrentKey and the call to
          // createKey, which causes the IOException, if this is the situation, ignore this
          // exception.
          keyVersion = keyProvider.getCurrentKey(keyName);
          if (keyVersion == null) {
            throw e;
          }
          // Continue with the newly retrieved key.
        }
      } else {
        // For decryption, the key should pre-exist.
        throw new IOException("No key named " + keyName + " exists");
      }
    }
    // The key exists.
    byte[] key = keyVersion.getMaterial();
    byte[] iv = new byte[0];
    KeyProvider.Metadata keyMetadata = keyProvider.getMetadata(keyName);
    Map<String, String> attr = keyMetadata.getAttributes();
    if (attr.containsKey(ATTRIBUTE_IV)) {
      // Some cipher type may not have IV.
      iv = attr.get(ATTRIBUTE_IV).getBytes();
    }
    String cipher = keyMetadata.getCipher();
    return ProtoUtils.setKey(
        ProtoUtils.setIv(CRYPTO_KEY_PARTIAL_BUILDER.setCipher(cipher), iv), key).build();
  }

  /**
   * Creates a random initialization vector.
   *
   * @param iv the initialization vector to be filled in
   */
  private void createInitializationVector(byte[] iv) {
    try {
      SecureRandom rand = SecureRandom.getInstance(SHA1PRNG);
      rand.nextBytes(iv);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Unknown random number generator algorithm: " + SHA1PRNG, e);
    }
  }

  /**
   * Creates and stores a new key with name in KMS, the key and initialization vector are all
   * randomly generated.
   *
   * @param provider the provider for generating the key
   * @param name the name of the key
   * @return the newly generated key
   * @throws IOException if the key fails to be created
   */
  private EncryptionProto.CryptoKey createKey(KeyProvider provider, String name)
      throws IOException {
    // TODO(cc): Avoid hard coding the cipher type and block size.
    try {
      KeyProvider.Options options = new KeyProvider.Options(new Configuration());
      options.setCipher(Constants.AES_GCM_NOPADDING);
      options.setBitLength(KEY_BIT_LENGTH);
      Map<String, String> attributes = new HashMap<>();
      byte[] iv = new byte[Constants.AES_GCM_NOPADDING_BLOCK_SIZE];
      createInitializationVector(iv);
      attributes.put(ATTRIBUTE_IV, new String(iv));
      options.setAttributes(attributes);
      byte[] key = provider.createKey(name, options).getMaterial();
      return ProtoUtils.setKey(
        ProtoUtils.setIv(
            CRYPTO_KEY_PARTIAL_BUILDER
                .setCipher(Constants.AES_GCM_NOPADDING),
            iv),
        key).build();
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }
}
