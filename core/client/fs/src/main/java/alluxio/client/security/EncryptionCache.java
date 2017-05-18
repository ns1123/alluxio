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

import alluxio.proto.security.EncryptionProto;

import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class implements a threadsafe encryption metadata cache.
 * Only files under encrypted mount point have encryption metadata. Non-encrypted files are not
 * cached.
 *
 * The cache entry is added when a new encrypted file is being created or accessed.
 */
@ThreadSafe
// TODO(chaomin): maybe limit the max size or store the encoded protobuf if this introduces much
// memory overhead in client JVM.
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
  public EncryptionProto.Meta get(Long fileId) {
    return mCache.get(fileId);
  }

  /**
   * Puts the encryption metadata for a file id into the cache.
   *
   * @param fileId the file id
   * @param meta the encryption metadata
   */
  public void put(Long fileId, EncryptionProto.Meta meta) {
    mCache.put(fileId, meta);
  }
}
