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

import alluxio.PropertyKey;
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

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;

/**
 * Hadoop KMS client.
 *
 * @see <a href="http://hadoop.apache.org/docs/r2.7.3/hadoop-kms/index.html">hadoop-kms
 * documentation</a> for more details on Hadoop KMS.
 *
 * TODO(cc): Only simple authentication is supported, Kerberos and SSL are not supported yet.
 * If this client tries to connect to a Hadoop KMS with Kerberos or SSL, connection will fail.
 */
public class HadoopKmsClient implements KmsClient {
  private static final String SHA1PRNG = "SHA1PRNG";

  /**
   * Creates a new {@link HadoopKmsClient}.
   */
  public HadoopKmsClient() {}

  /**
   * Gets cipher type and key from Hadoop KMS with inputKey as key name, then creates a
   * {@link EncryptionProto.CryptoKey}, note that some fields may not be set, details are below.
   *
   * Hadoop KMS does not store IV, the IV is randomly generated if encrypt is true, otherwise,
   * IV is empty in the returned {@link EncryptionProto.CryptoKey} and should be retrieved from
   * the {@link EncryptionProto.Meta} encoded in Alluxio encrypted files.
   *
   * Hadoop KMS does not provide generation ID, so the generation ID in the returned
   * {@link EncryptionProto.CryptoKey} is empty.
   *
   * NeedsAuthTag is always set to 1.
   *
   * @param kms the KMS endpoint in Hadoop KMS URI format like kms://{PROTO}@{HOST}:{PORT}/{PATH}
   * @param encrypt whether to encrypt or decrypt
   * @param inputKey name of the key to be retrieved
   * @return the available crypto key information
   * @throws IOException when the crypto key fails to be created
   */
  @Override
  public EncryptionProto.CryptoKey getCryptoKey(String kms, boolean encrypt, String inputKey)
      throws IOException {
    URI kmsEndpoint;
    try {
      kmsEndpoint = new URI(kms);
    } catch (URISyntaxException e) {
      throw new IOException("Invalid hadoop KMS endpoint format", e);
    }

    Configuration conf = new Configuration();
    String principal = alluxio.Configuration.get(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
    String keytabFile = alluxio.Configuration.get(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
    if (!principal.isEmpty() && !keytabFile.isEmpty()) {
      // Login Hadoop with Alluxio client kerberos principal and keytab.
      conf.set("hadoop.security.authentication", AuthType.KERBEROS.getAuthName());
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
    }

    KeyProvider keyProvider = KMSClientProvider.Factory.get(kmsEndpoint, conf);
    byte[] key = keyProvider.getCurrentKey(inputKey).getMaterial();
    String cipher = keyProvider.getMetadata(inputKey).getCipher();
    byte[] iv = new byte[0];
    if (encrypt) {
      // TODO(cc): in order to encode iv into file footer, iv needs to be added into
      // FileFooter.FileMetadata protobuf.
      // Generate a random IV for encryption. For decryption, the IV is retrieved from file
      // metadata.
      try {
        int blockSize = Cipher.getInstance(cipher).getBlockSize();
        if (blockSize > 0) {
          // Only block cipher has IV which has the same size as a block.
          iv = new byte[blockSize];
          createInitializationVector(iv);
        }
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        throw new IOException("Failed to get block size for cipher type " + cipher, e);
      }
    }
    // TODO(cc): for decryption, IV needs to be parsed from FileFooter.
    return ProtoUtils.setKey(
        ProtoUtils.setIv(
            EncryptionProto.CryptoKey.newBuilder()
                .setCipher(cipher)
                .setGenerationId("")
                .setNeedsAuthTag(1),
            iv),
        key).build();
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

}
