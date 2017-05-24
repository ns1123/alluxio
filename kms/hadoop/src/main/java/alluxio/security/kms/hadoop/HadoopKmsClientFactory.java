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

package alluxio.security.kms.hadoop;

import alluxio.Constants;
import alluxio.security.kms.KmsClient;
import alluxio.security.kms.KmsClientFactory;

import java.io.IOException;

/**
 * Factory for creating {@link HadoopKmsClient}.
 */
public final class HadoopKmsClientFactory implements KmsClientFactory {
  /**
   * Constructs a {@link HadoopKmsClientFactory}.
   */
  public HadoopKmsClientFactory() {}

  @Override
  public KmsClient create() throws IOException {
    return new HadoopKmsClient();
  }

  @Override
  public boolean supportsProvider(String provider) {
    return provider.equalsIgnoreCase(Constants.KMS_HADOOP_PROVIDER_NAME);
  }
}
