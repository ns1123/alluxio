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

package alluxio.security.kms.dummy;

import alluxio.Constants;
import alluxio.proto.security.EncryptionProto;
import alluxio.security.kms.KmsClient;
import alluxio.util.proto.ProtoUtils;

import java.io.IOException;

/**
 * This class returns the hard coded {@link EncryptionProto.CryptoKey} for testing.
 */
public class DummyKmsClient implements KmsClient {
  private static final String CIPHER = Constants.AES_GCM_NOPADDING;

  /**
   * Creates a new {@link DummyKmsClient}.
   */
  public DummyKmsClient() {}

  @Override
  public EncryptionProto.CryptoKey getCryptoKey(String kms, boolean encrypt, String inputKey)
      throws IOException {
    return ProtoUtils.setIv(
        ProtoUtils.setKey(
            EncryptionProto.CryptoKey.newBuilder()
                .setCipher(CIPHER)
                .setNeedsAuthTag(1)
                .setGenerationId("generationId"),
            Constants.ENCRYPTION_KEY_FOR_TESTING.getBytes()),
        Constants.ENCRYPTION_IV_FOR_TESTING.getBytes()).build();
  }
}
