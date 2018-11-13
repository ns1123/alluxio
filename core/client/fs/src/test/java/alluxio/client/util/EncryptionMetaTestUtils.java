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

package alluxio.client.util;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.EncryptionMetaFactory;
import alluxio.client.LayoutUtils;
import alluxio.proto.security.EncryptionProto;
import alluxio.util.proto.ProtoUtils;

import java.io.IOException;

/**
 * Testing utils for {@link EncryptionProto.Meta}.
 */
public class EncryptionMetaTestUtils {
  private static final EncryptionProto.Meta PARTIAL_META =
      initializePartialMeta();

  private static final EncryptionProto.CryptoKey TESTING_CRYPTO_KEY =
      ProtoUtils.setIv(
          ProtoUtils.setKey(
              EncryptionProto.CryptoKey.newBuilder()
                  .setCipher(Constants.AES_GCM_NOPADDING)
                  .setNeedsAuthTag(1)
                  .setGenerationId("generationId"),
              Constants.ENCRYPTION_KEY_FOR_TESTING.getBytes()),
          Constants.ENCRYPTION_IV_FOR_TESTING.getBytes()).build();

  /**
   * Creates a new {@link EncryptionProto.Meta} from the configuration.
   *
   * @return the encryption meta
   */
  public static EncryptionProto.Meta create() throws IOException {
    // Use a dummy testing crypto key for invalid encryption id.
    return EncryptionMetaFactory.create(
        Constants.INVALID_ENCRYPTION_ID, Constants.INVALID_ENCRYPTION_ID,
        LayoutUtils.toPhysicalBlockLength(
            PARTIAL_META, Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT)),
        TESTING_CRYPTO_KEY);
  }

  private static EncryptionProto.Meta initializePartialMeta() {
    long blockHeaderSize = Configuration.getBytes(PropertyKey.USER_BLOCK_HEADER_SIZE_BYTES);
    long blockFooterSize = Configuration.getBytes(PropertyKey.USER_BLOCK_FOOTER_SIZE_BYTES);
    long chunkSize = Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
    long chunkHeaderSize =
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES);
    long chunkFooterSize =
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);

    return EncryptionProto.Meta.newBuilder()
        .setBlockHeaderSize(blockHeaderSize)
        .setBlockFooterSize(blockFooterSize)
        .setChunkHeaderSize(chunkHeaderSize)
        .setChunkSize(chunkSize)
        .setChunkFooterSize(chunkFooterSize)
        .buildPartial();
  }
}