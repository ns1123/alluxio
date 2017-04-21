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

import alluxio.Constants;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link LayoutSpec}.
 */
public final class LayoutSpecTest {
  private static final int BLOCK_HEADER_SIZE = 128;
  private static final int BLOCK_FOOTER_SIZE = 64;
  private static final int LOGICAL_BLOCK_SIZE = Constants.MB;
  private static final int CHUNK_SIZE = Constants.DEFAULT_CHUNK_SIZE;
  private static final int CHUNK_HEADER_SIZE = 32;
  private static final int CHUNK_FOOTER_SIZE = Constants.DEFAULT_CHUNK_FOOTER_SIZE;

  @Test
  public void basic() throws Exception {
    LayoutSpec spec = new LayoutSpec(BLOCK_HEADER_SIZE, BLOCK_FOOTER_SIZE,
        LOGICAL_BLOCK_SIZE, CHUNK_HEADER_SIZE, CHUNK_SIZE, CHUNK_FOOTER_SIZE);
    Assert.assertEquals(BLOCK_HEADER_SIZE, spec.getBlockHeaderSize());
    Assert.assertEquals(BLOCK_FOOTER_SIZE, spec.getBlockFooterSize());
    Assert.assertEquals(LOGICAL_BLOCK_SIZE, spec.getLogicalBlockSize());
    Assert.assertEquals(CHUNK_HEADER_SIZE, spec.getChunkHeaderSize());
    Assert.assertEquals(CHUNK_SIZE, spec.getChunkSize());
    Assert.assertEquals(CHUNK_FOOTER_SIZE, spec.getChunkFooterSize());
  }
}
