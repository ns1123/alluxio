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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class implements a threadsafe encryption metadata cache. Only files under encrypted mount
 * point have encryption metadata. Non-encrypted files are not cached.
 *
 * The cache entry is added when a new encrypted file is being created or accessed.
 */
@ThreadSafe
// TODO(chaomin): store the serialized protobuf if this introduces much memory overhead.
public final class EncryptionCache {
  Cache<Long, EncryptionProto.Meta> mCache;

  /**
   * Default constructor.
   */
  public EncryptionCache() {
    this(1000 /* max size */, 30 /* expiration min after access */);
  }

  /**
   * Creates an encryption cache.
   *
   * @param maxSize the max size
   * @param expirationMinAfterAccess the expiration time after access in minutes
   */
  public EncryptionCache(int maxSize, int expirationMinAfterAccess) {
    mCache = CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterAccess(expirationMinAfterAccess, TimeUnit.MINUTES)
        .build();
  }

  /**
   * Clears the cache.
   */
  public void clear() {
    mCache.cleanUp();
  }

  /**
   * Gets the encryption metadata for a file id.
   *
   * @param fileId the file id to query for
   * @return the encryption metadata
   */
  public EncryptionProto.Meta get(Long fileId) {
    return mCache.getIfPresent(fileId);
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
