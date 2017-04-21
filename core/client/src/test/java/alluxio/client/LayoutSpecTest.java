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
  @Test
  public void basic() throws Exception {
    final int blockHeaderSize = 128;
    final int blockFooterSize = 64;
    final int logicalBlockSize = Constants.MB;
    final int chunkSize = Constants.DEFAULT_CHUNK_SIZE;
    final int chunkHeaderSize = 32;
    final int chunkFooterSize = Constants.DEFAULT_CHUNK_FOOTER_SIZE;
    LayoutSpec spec = new LayoutSpec(blockHeaderSize, blockFooterSize,
        logicalBlockSize, chunkHeaderSize, chunkSize, chunkFooterSize);

    Assert.assertEquals(blockHeaderSize, spec.getBlockHeaderSize());
    Assert.assertEquals(blockFooterSize, spec.getBlockFooterSize());
    Assert.assertEquals(logicalBlockSize, spec.getLogicalBlockSize());
    Assert.assertEquals(chunkHeaderSize, spec.getChunkHeaderSize());
    Assert.assertEquals(chunkSize, spec.getChunkSize());
    Assert.assertEquals(chunkFooterSize, spec.getChunkFooterSize());
  }
}
