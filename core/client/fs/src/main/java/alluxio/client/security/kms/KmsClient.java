/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.client.security.kms;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.proto.security.EncryptionProto;

import java.io.IOException;

/**
 * Interface for different Key Management Service (KMS) clients.
 */
public interface KmsClient {
  /**
   * Gets a {@link EncryptionProto.CryptoKey} from the specified KMS endpoint and input key.
   *
   * @param kms the KMS endpoint
   * @param encrypt whether to encrypt or decrypt
   * @param inputKey the input key of the KMS calls
   * @return the fetched crypto key for encryption or decryption
   * @throws IOException when failed to get the key
   */
  EncryptionProto.CryptoKey getCryptoKey(String kms, boolean encrypt, String inputKey)
      throws IOException;

  /**
   * Factory for creating the appropriate instance of {@link KmsClient} based on
   * {@link alluxio.PropertyKey#SECURITY_KMS_PROVIDER}.
   */
  final class Factory {
    /**
     * Creates an instance of {@link KmsClient} implementation based on
     * {@link alluxio.PropertyKey#SECURITY_KMS_PROVIDER}.
     *
     * @return the {@link KmsClient} for the specified provider
     * @throws IOException when the provider is not supported
     */
    public static KmsClient create() throws IOException {
      String kmsProvider = Configuration.get(PropertyKey.SECURITY_KMS_PROVIDER).toLowerCase();
      switch (kmsProvider) {
        case Constants.KMS_HADOOP_PROVIDER_NAME:
          return new HadoopKmsClient();
        case Constants.KMS_TS_PROVIDER_NAME:
          return new TwoSigmaKmsClient();
        default:
          throw new IOException("Unsupported KMS provider " + kmsProvider);
      }
    }

    private Factory() {} // prevent instantiation
  }
}
