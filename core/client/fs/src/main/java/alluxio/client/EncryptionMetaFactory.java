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
import alluxio.proto.layout.FileFooter;
import alluxio.proto.security.EncryptionProto;

import com.google.common.base.Preconditions;

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
  public static EncryptionProto.Meta create() {
    return create(Constants.INVALID_ENCRYPTION_ID);
  }

  /**
   * Creates a new {@link EncryptionProto.Meta} with the specified file id.
   *
   * @param fileId the file id
   * @return the encryption meta
   */
  // TODO(chaomin): add logicalBlockSize as a param
  public static EncryptionProto.Meta create(long fileId) {
    return PARTIAL_META.toBuilder()
        .setEncryptionId(fileId)
        .setFileId(fileId)
        .setEncodedMetaSize(
            PARTIAL_FILE_METADATA.toBuilder().setEncryptionId(fileId).build().getSerializedSize())
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
    long defaultBlockSize = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    long logicalBlockSize = 0;
    long physicalBlockSize = 0;
    if (defaultBlockSize >= chunkSize) {
      Preconditions.checkState(defaultBlockSize % chunkSize == 0);
      logicalBlockSize = defaultBlockSize;
      physicalBlockSize = blockHeaderSize + blockFooterSize
          + logicalBlockSize / chunkSize * (chunkHeaderSize + chunkSize + chunkFooterSize);
    }

    return FileFooter.FileMetadata.newBuilder()
        .setBlockHeaderSize(blockHeaderSize)
        .setBlockFooterSize(blockFooterSize)
        .setChunkHeaderSize(chunkHeaderSize)
        .setChunkSize(chunkSize)
        .setChunkFooterSize(chunkFooterSize)
        .setPhysicalBlockSize(physicalBlockSize)
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
    long defaultBlockSize = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    long logicalBlockSize = 0;
    long physicalBlockSize = 0;
    if (defaultBlockSize >= chunkSize) {
      Preconditions.checkState(defaultBlockSize % chunkSize == 0);
      logicalBlockSize = defaultBlockSize;
      physicalBlockSize = blockHeaderSize + blockFooterSize
          + logicalBlockSize / chunkSize * (chunkHeaderSize + chunkSize + chunkFooterSize);
    }

    return EncryptionProto.Meta.newBuilder()
        .setBlockHeaderSize(blockHeaderSize)
        .setBlockFooterSize(blockFooterSize)
        .setChunkHeaderSize(chunkHeaderSize)
        .setChunkSize(chunkSize)
        .setChunkFooterSize(chunkFooterSize)
        .setLogicalBlockSize(logicalBlockSize)
        .setPhysicalBlockSize(physicalBlockSize)
        .buildPartial();
  }

  private EncryptionMetaFactory() {}  // prevent instantiation
}
