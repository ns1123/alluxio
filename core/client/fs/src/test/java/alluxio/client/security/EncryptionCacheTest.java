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

import alluxio.client.EncryptionMetaFactory;
import alluxio.proto.security.EncryptionProto;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link EncryptionCache}.
 */
public final class EncryptionCacheTest {
  private static final long BLOCK_HEADER_SIZE = 1L;
  private static final long BLOCK_FOOTER_SIZE = 2L;
  private static final long CHUNK_HEADER_SIZE = 3L;
  private static final long CHUNK_SIZE = 4L;
  private static final long CHUNK_FOOTER_SIZE = 5L;
  private static final long LOGICAL_BLOCK_SIZE = CHUNK_SIZE;
  private static final long PHYSICAL_BLOCK_SIZE = BLOCK_HEADER_SIZE + BLOCK_FOOTER_SIZE
      + LOGICAL_BLOCK_SIZE / CHUNK_SIZE * (CHUNK_HEADER_SIZE + CHUNK_SIZE + CHUNK_FOOTER_SIZE);
  private static final long ENCRYPTION_ID = 111111L;

  @Test
  public void basic() throws Exception {
    EncryptionCache cache = new EncryptionCache();

    long fileId = 5L;
    EncryptionProto.Meta expected = EncryptionMetaFactory.create();
    cache.put(fileId, expected);

    EncryptionProto.Meta actual = cache.get(fileId);
    Assert.assertEquals(expected, actual);
    cache.clear();
  }

  @Test
  public void putWithExisting() throws Exception {
    EncryptionCache cache = new EncryptionCache();

    long fileId = 5L;
    EncryptionProto.Meta expected = EncryptionMetaFactory.create();
    cache.put(fileId, expected);

    EncryptionProto.Meta actual = cache.get(fileId);
    Assert.assertEquals(expected, actual);

    EncryptionProto.Meta newMeta = EncryptionProto.Meta.newBuilder()
        .setBlockHeaderSize(BLOCK_HEADER_SIZE)
        .setBlockFooterSize(BLOCK_FOOTER_SIZE)
        .setChunkHeaderSize(CHUNK_HEADER_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setChunkFooterSize(CHUNK_FOOTER_SIZE)
        .setPhysicalBlockSize(PHYSICAL_BLOCK_SIZE)
        .setLogicalBlockSize(LOGICAL_BLOCK_SIZE)
        .setEncryptionId(ENCRYPTION_ID)
        .setFileId(fileId)
        .build();

    cache.put(fileId, newMeta);
    actual = cache.get(fileId);
    Assert.assertEquals(newMeta, actual);

    cache.clear();
  }

  @Test
  public void putNull() throws Exception {
    EncryptionCache cache = new EncryptionCache();
    try {
      cache.put(1L, null);
      Assert.fail();
    } catch (NullPointerException e) {
      // expected
    }
    cache.clear();
  }
}
