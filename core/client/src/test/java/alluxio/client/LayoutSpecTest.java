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
    final long blockHeaderSize = 128L;
    final long blockFooterSize = 64L;
    final long logicalBlockSize = Constants.MB;
    final long chunkSize = Constants.DEFAULT_CHUNK_SIZE;
    final long chunkHeaderSize = 32L;
    final long chunkFooterSize = Constants.DEFAULT_CHUNK_FOOTER_SIZE;
    LayoutSpec spec = new LayoutSpec(blockHeaderSize, blockFooterSize,
        logicalBlockSize, chunkHeaderSize, chunkSize, chunkFooterSize);

    Assert.assertEquals(blockHeaderSize, spec.getBlockHeaderSize());
    Assert.assertEquals(blockFooterSize, spec.getBlockFooterSize());
    Assert.assertEquals(logicalBlockSize, spec.getLogicalBlockSize());
    Assert.assertEquals(chunkHeaderSize, spec.getChunkHeaderSize());
    Assert.assertEquals(chunkSize, spec.getChunkSize());
    Assert.assertEquals(chunkFooterSize, spec.getChunkFooterSize());
  }

  @Test
  public void createFromConfiguration() {
    LayoutSpec spec1 = LayoutSpec.Factory.createFromConfiguration();
    LayoutSpec spec2 = LayoutSpec.Factory.createFromConfiguration();
    Assert.assertEquals(spec1, spec2);
  }
}
