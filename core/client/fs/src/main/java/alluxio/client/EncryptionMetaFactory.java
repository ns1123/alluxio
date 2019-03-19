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

import alluxio.client.security.CryptoUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.proto.layout.FileFooter;
import alluxio.proto.security.EncryptionProto;

import java.io.IOException;

/**
 * Factory to create {@link EncryptionProto.Meta}.
 */
public final class EncryptionMetaFactory {

  /**
   * Creates a new {@link EncryptionProto.Meta} from the configuration, only for layout purpose.
   * Encryption id and crypto key are not set.
   *
   * @param conf Alluxio configuration
   * @return the encryption meta
   */
  public static EncryptionProto.Meta createLayout(AlluxioConfiguration conf) throws IOException {
    return initializePartialMeta(conf);
  }

  /**
   * Creates a new {@link EncryptionProto.Meta} from the configuration and the specified file id.
   *
   * @param fileId the file id
   * @param encryptionId the encryption id
   * @param physicalBlockSize the physical block size
   * @param conf Alluxio configuration
   * @return the encryption meta
   */
  public static EncryptionProto.Meta create(long fileId, long encryptionId,
      long physicalBlockSize, AlluxioConfiguration conf)
      throws IOException {
    EncryptionProto.CryptoKey cryptoKey = CryptoUtils.getCryptoKey(
        conf.get(PropertyKey.SECURITY_KMS_PROVIDER), conf.get(PropertyKey.SECURITY_KMS_ENDPOINT),
        true, String.valueOf(encryptionId), conf);
    return create(fileId, encryptionId, physicalBlockSize, cryptoKey, conf);
  }

  /**
   * Creates a new {@link EncryptionProto.Meta} from the specified file id and crypto key.
   *
   * @param fileId the file id
   * @param encryptionId the encryption id
   * @param physicalBlockSize the physical block size
   * @param cryptoKey the crypto key
   * @param conf Alluxio configuration
   * @return the encryption meta
   */
  public static EncryptionProto.Meta create(
      long fileId, long encryptionId, long physicalBlockSize, EncryptionProto.CryptoKey cryptoKey,
      AlluxioConfiguration conf) throws IOException {
    EncryptionProto.Meta partialMeta = initializePartialMeta(conf);
    long logicalBlockSize = LayoutUtils.toLogicalBlockLength(partialMeta,
        physicalBlockSize);
    return partialMeta.toBuilder()
        .setEncryptionId(encryptionId)
        .setFileId(fileId)
        .setLogicalBlockSize(logicalBlockSize)
        .setPhysicalBlockSize(physicalBlockSize)
        .setEncodedMetaSize(
            initializePartialFileMetadata(conf).toBuilder().setEncryptionId(encryptionId)
                .setPhysicalBlockSize(physicalBlockSize).build().getSerializedSize())
        .setCryptoKey(cryptoKey)
        .build();
  }

  private static FileFooter.FileMetadata initializePartialFileMetadata(AlluxioConfiguration alluxioConf) {
    long blockHeaderSize = alluxioConf.getBytes(PropertyKey.USER_BLOCK_HEADER_SIZE_BYTES);
    long blockFooterSize = alluxioConf.getBytes(PropertyKey.USER_BLOCK_FOOTER_SIZE_BYTES);
    long chunkSize = alluxioConf.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
    long chunkHeaderSize =
        alluxioConf.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES);
    long chunkFooterSize =
        alluxioConf.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);

    return FileFooter.FileMetadata.newBuilder()
        .setBlockHeaderSize(blockHeaderSize)
        .setBlockFooterSize(blockFooterSize)
        .setChunkHeaderSize(chunkHeaderSize)
        .setChunkSize(chunkSize)
        .setChunkFooterSize(chunkFooterSize)
        .buildPartial();
  }

  private static EncryptionProto.Meta initializePartialMeta(AlluxioConfiguration alluxioConf) {
    long blockHeaderSize = alluxioConf.getBytes(PropertyKey.USER_BLOCK_HEADER_SIZE_BYTES);
    long blockFooterSize = alluxioConf.getBytes(PropertyKey.USER_BLOCK_FOOTER_SIZE_BYTES);
    long chunkSize = alluxioConf.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
    long chunkHeaderSize =
        alluxioConf.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES);
    long chunkFooterSize =
        alluxioConf.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);

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
