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

import java.io.IOException;

/**
 * Interface for {@link KmsClient} factories.
 */
public interface KmsClientFactory {
  /**
   * Creates an instance of {@link KmsClient} implementation.
   *
   * @return the {@link KmsClient} for the specified provider
   * @throws IOException when the provider is not supported
   */
  KmsClient create() throws IOException;

  /**
   * @param provider the KMS provider
   * @return whether this factory supports the KMS provider
   */
  boolean supportsProvider(String provider);
}
