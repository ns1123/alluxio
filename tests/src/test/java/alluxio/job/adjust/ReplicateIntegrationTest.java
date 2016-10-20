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

package alluxio.job.adjust;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.job.JobIntegrationTest;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for {@link ReplicateDefinition}.
 */
public final class ReplicateIntegrationTest extends JobIntegrationTest {
  private static final String TEST_URI = "/test";
  private static final int TEST_BLOCK_SIZE = 100;
  private BlockStoreContext mBlockStoreContext;
  private long mBlockId1;
  private long mBlockId2;

  @Before
  public void before() throws Exception {
    super.before();
    mBlockStoreContext = BlockStoreContext.get();

    // write a file outside of Alluxio
    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.THROUGH)
            .setBlockSizeBytes(TEST_BLOCK_SIZE));
    os.write(BufferUtils.getIncreasingByteArray(TEST_BLOCK_SIZE + 1));
    os.close();

    URIStatus status = mFileSystem.getStatus(filePath);
    mBlockId1 = status.getBlockIds().get(0);
    mBlockId2 = status.getBlockIds().get(1);
  }

  @Test
  public void replicateFullBlockFromUFS() throws Exception {
    // run the replicate job for mBlockId1
    waitForJobToFinish(mJobMaster.runJob(new ReplicateConfig(mBlockId1, 1)));

    BlockInfo blockInfo1 = AdjustJobTestUtils.getBlock(mBlockId1, mBlockStoreContext);
    BlockInfo blockInfo2 = AdjustJobTestUtils.getBlock(mBlockId2, mBlockStoreContext);
    Assert.assertEquals(1, blockInfo1.getLocations().size());
    Assert.assertEquals(0, blockInfo2.getLocations().size());
    Assert.assertEquals(TEST_BLOCK_SIZE, blockInfo1.getLength());
    Assert.assertEquals(1, blockInfo2.getLength());
  }

  @Test
  public void replicateLastBlockFromUFS() throws Exception {
    // run the replicate job for mBlockId2
    waitForJobToFinish(mJobMaster.runJob(new ReplicateConfig(mBlockId2, 1)));

    BlockInfo blockInfo1 = AdjustJobTestUtils.getBlock(mBlockId1, mBlockStoreContext);
    BlockInfo blockInfo2 = AdjustJobTestUtils.getBlock(mBlockId2, mBlockStoreContext);
    Assert.assertEquals(0, blockInfo1.getLocations().size());
    Assert.assertEquals(1, blockInfo2.getLocations().size());
    Assert.assertEquals(TEST_BLOCK_SIZE, blockInfo1.getLength());
    Assert.assertEquals(1, blockInfo2.getLength());
  }
}
