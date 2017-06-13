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

package alluxio.client;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.security.CryptoUtils;
import alluxio.proto.layout.FileFooter;
import alluxio.proto.security.EncryptionProto;

import java.io.IOException;

/**
 * Factory to create {@link EncryptionProto.Meta}.
 */
public final class EncryptionMetaFactory {
  private static final FileFooter.FileMetadata PARTIAL_FILE_METADATA =
      initializePartialFileMetadata();
  private static final EncryptionProto.Meta PARTIAL_META =
      initializePartialMeta();

  /**
   * Creates a new {@link EncryptionProto.Meta} from the configuration.
   *
   * @return the encryption meta
   */
  public static EncryptionProto.Meta create() throws IOException {
    // Use an empty dummy crypto key for invalid encryption id because the key is never used.
    return create(Constants.INVALID_ENCRYPTION_ID, Constants.INVALID_ENCRYPTION_ID,
        LayoutUtils.toPhysicalBlockLength(
          PARTIAL_META, Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT)),
        EncryptionProto.CryptoKey.newBuilder().build());
  }

  /**
   * Creates a new {@link EncryptionProto.Meta} from the configuration and the specified file id.
   *
   * @param fileId the file id
   * @param encryptionId the encryption id
   * @param physicalBlockSize the physical block size
   * @return the encryption meta
   */
  public static EncryptionProto.Meta create(long fileId, long encryptionId, long physicalBlockSize)
      throws IOException {
    EncryptionProto.CryptoKey cryptoKey = CryptoUtils.getCryptoKey(
        Configuration.get(PropertyKey.SECURITY_KMS_ENDPOINT), true, String.valueOf(encryptionId));
    return create(fileId, encryptionId, physicalBlockSize, cryptoKey);
  }

  /**
   * Creates a new {@link EncryptionProto.Meta} from the specified file id and crypto key.
   *
   * @param fileId the file id
   * @param encryptionId the encryption id
   * @param physicalBlockSize the physical block size
   * @param cryptoKey the crypto key
   * @return the encryption meta
   */
  public static EncryptionProto.Meta create(
      long fileId, long encryptionId, long physicalBlockSize, EncryptionProto.CryptoKey cryptoKey)
      throws IOException {
    long logicalBlockSize = LayoutUtils.toLogicalBlockLength(PARTIAL_META, physicalBlockSize);
    return PARTIAL_META.toBuilder()
        .setEncryptionId(encryptionId)
        .setFileId(fileId)
        .setLogicalBlockSize(logicalBlockSize)
        .setPhysicalBlockSize(physicalBlockSize)
        .setEncodedMetaSize(
            PARTIAL_FILE_METADATA.toBuilder().setEncryptionId(encryptionId)
                .setPhysicalBlockSize(physicalBlockSize).build().getSerializedSize())
        .setCryptoKey(cryptoKey)
        .build();
  }

  private static FileFooter.FileMetadata initializePartialFileMetadata() {
    long blockHeaderSize = Configuration.getBytes(PropertyKey.USER_BLOCK_HEADER_SIZE_BYTES);
    long blockFooterSize = Configuration.getBytes(PropertyKey.USER_BLOCK_FOOTER_SIZE_BYTES);
    long chunkSize = Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
    long chunkHeaderSize =
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES);
    long chunkFooterSize =
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);

    return FileFooter.FileMetadata.newBuilder()
        .setBlockHeaderSize(blockHeaderSize)
        .setBlockFooterSize(blockFooterSize)
        .setChunkHeaderSize(chunkHeaderSize)
        .setChunkSize(chunkSize)
        .setChunkFooterSize(chunkFooterSize)
        .buildPartial();
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

  private EncryptionMetaFactory() {}  // prevent instantiation
}
