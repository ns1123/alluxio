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

package alluxio.job.replicate;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.job.JobIntegrationTest;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for {@link EvictDefinition}.
 */
public final class EvictIntegrationTest extends JobIntegrationTest {
  private static final String TEST_URI = "/test";
  private static final int TEST_BLOCK_SIZE = 100;
  private long mBlockId1;
  private long mBlockId2;
  private WorkerNetAddress mWorker1;
  private WorkerNetAddress mWorker2;

  @Before
  public void before() throws Exception {
    super.before();

    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
            .setBlockSizeBytes(TEST_BLOCK_SIZE));
    os.write(BufferUtils.getIncreasingByteArray(TEST_BLOCK_SIZE + 1));
    os.close();

    URIStatus status = mFileSystem.getStatus(filePath);
    mBlockId1 = status.getBlockIds().get(0);
    mBlockId2 = status.getBlockIds().get(1);

    BlockInfo blockInfo1 = AdjustJobTestUtils.getBlock(mBlockId1, FileSystemContext.INSTANCE);
    BlockInfo blockInfo2 = AdjustJobTestUtils.getBlock(mBlockId2, FileSystemContext.INSTANCE);
    mWorker1 = blockInfo1.getLocations().get(0).getWorkerAddress();
    mWorker2 = blockInfo2.getLocations().get(0).getWorkerAddress();
  }

  @Test
  public void evictFullBlock() throws Exception {
    // run the evict job for full block mBlockId1
    waitForJobToFinish(mJobMaster.run(new EvictConfig(mBlockId1, 1)));
    Assert
        .assertFalse(AdjustJobTestUtils.hasBlock(mBlockId1, mWorker1, FileSystemContext.INSTANCE));
    Assert.assertTrue(AdjustJobTestUtils.hasBlock(mBlockId2, mWorker2, FileSystemContext.INSTANCE));
  }

  @Test
  public void evictLastBlock() throws Exception {
    // run the evict job for the last block mBlockId2
    waitForJobToFinish(mJobMaster.run(new EvictConfig(mBlockId2, 1)));
    Assert.assertTrue(AdjustJobTestUtils.hasBlock(mBlockId1, mWorker1, FileSystemContext.INSTANCE));
    Assert
        .assertFalse(AdjustJobTestUtils.hasBlock(mBlockId2, mWorker2, FileSystemContext.INSTANCE));
  }
}
