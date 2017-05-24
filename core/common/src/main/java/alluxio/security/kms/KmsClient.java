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

package alluxio.security.kms;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.proto.security.EncryptionProto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

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

  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Factory.class);
    private static final ServiceLoader<KmsClientFactory> SERVICE_LOADER =
        ServiceLoader.load(KmsClientFactory.class);

    private Factory() {} // prevent initialization

    /**
     * Finds a {@link KmsClientFactory} that supports the {@link PropertyKey#SECURITY_KMS_PROVIDER}
     * through {@link ServiceLoader} and creates a {@link KmsClient} by the factory.
     *
     * @return the {@link KmsClient} for the specified KMS provider
     * @throws IOException if there is no {@link KmsClientFactory} for the
     *    {@link PropertyKey#SECURITY_KMS_PROVIDER} or if no {@link KmsClient} could successfully be
     *    created
     */
    public static KmsClient create() throws IOException {
      String provider = Configuration.get(PropertyKey.SECURITY_KMS_PROVIDER);
      List<Throwable> errors = new ArrayList<>();
      for (KmsClientFactory factory : SERVICE_LOADER) {
        if (factory.supportsProvider(provider)) {
          try {
            return factory.create();
          } catch (IOException e) {
            errors.add(e);
            LOG.warn("Failed to create KmsClient", e);
          }
        }
      }
      StringBuilder errorStr = new StringBuilder();
      errorStr.append("All eligible KmsClientFactory service providers were unable to create an "
          + "instance for provider ").append(provider).append('\n');
      for (Throwable e : errors) {
        errorStr.append(e).append('\n');
      }
      throw new IOException(errorStr.toString());
    }
  }
}
