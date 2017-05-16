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

package alluxio.client.security;

import alluxio.proto.journal.FileFooter;
import alluxio.proto.security.EncryptionProto;

import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class implements a threadsafe encryption metadata cache per Alluxio client JVM.
 * Only files under encrypted mount point have encryption metadata. Non-encrypted files are not
 * cached.
 *
 * The cache entry is added when a new encrypted file is being created or read.
 *
 * The cache entry is invalidated in the following scenarios:
 * 1. expiration time passed after last access
 * 2. max size is reached
 */
@ThreadSafe
// TODO(chaomin): maybe limit the max size to 1000 entries.
public final class EncryptionCache {
  private ConcurrentHashMapV8<Long, EncryptionProto.Meta> mCache =
      new ConcurrentHashMapV8<>();

  /**
   * Default constructor.
   */
  public EncryptionCache() {}

  /**
   * Clears the cache.
   */
  public void clear() {
    mCache.clear();
  }

  /**
   * Gets the encryption metadata for a file id.
   *
   * @param fileId the file id to query for
   * @return the encryption metadata
   */
  public EncryptionProto.Meta getMeta(Long fileId) {
    return mCache.get(fileId);
  }

  /**
   * Puts the encryption metadata for a file id into the cache.
   *
   * @param fileId the file id
   * @param meta the encryption metadata
   */
  public void putMeta(Long fileId, EncryptionProto.Meta meta) {
    mCache.put(fileId, meta);
  }

  /**
   * Puts the file metadata for a file id into the cache.
   *
   * @param fileId the file id
   * @param fileMetadata the file metadata
   */
  public void putWithFooter(Long fileId, FileFooter.FileMetadata fileMetadata) {
    mCache.put(fileId, fromFooterMetadata(fileId, fileMetadata));
  }

  private EncryptionProto.Meta fromFooterMetadata(
      Long fileId, FileFooter.FileMetadata fileMetadata) {
    long physicalChunkSize = fileMetadata.getChunkHeaderSize() + fileMetadata.getChunkSize()
        + fileMetadata.getChunkFooterSize();
    long logicalBlockSize = (fileMetadata.getPhysicalBlockSize() - fileMetadata.getBlockHeaderSize()
        - fileMetadata.getBlockFooterSize()) / physicalChunkSize * fileMetadata.getChunkSize();
    return EncryptionProto.Meta.newBuilder()
        .setBlockHeaderSize(fileMetadata.getBlockHeaderSize())
        .setBlockFooterSize(fileMetadata.getBlockFooterSize())
        .setChunkHeaderSize(fileMetadata.getChunkHeaderSize())
        .setChunkSize(fileMetadata.getChunkSize())
        .setChunkFooterSize(fileMetadata.getChunkFooterSize())
        .setPhysicalBlockSize(fileMetadata.getPhysicalBlockSize())
        .setLogicalBlockSize(logicalBlockSize)
        .setFileId(fileId)
        .setEncryptionId(fileMetadata.getEncryptionId())
        .build();
  }
}
