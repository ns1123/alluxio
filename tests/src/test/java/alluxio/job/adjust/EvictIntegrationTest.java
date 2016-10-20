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
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for {@link EvictDefinition}.
 */
public final class EvictIntegrationTest extends JobIntegrationTest {
  private static final String TEST_URI = "/test";
  private static int TEST_BLOCK_SIZE = 100;
  private BlockStoreContext mBlockStoreContext;
  private long mBlockId1;
  private long mBlockId2;
  private WorkerNetAddress mWorker1;
  private WorkerNetAddress mWorker2;

  @Before
  public void before() throws Exception {
    super.before();
    mBlockStoreContext = BlockStoreContext.get();

    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
            .setBlockSizeBytes(TEST_BLOCK_SIZE));
    os.write(BufferUtils.getIncreasingByteArray(TEST_BLOCK_SIZE + 1));
    os.close();

    URIStatus status = mFileSystem.getStatus(filePath);
    mBlockId1 = status.getBlockIds().get(0);
    mBlockId2 = status.getBlockIds().get(1);

    BlockInfo blockInfo1 = AdjustJobTestUtils.getBlockInfoFromMaster(mBlockId1, mBlockStoreContext);
    BlockInfo blockInfo2 = AdjustJobTestUtils.getBlockInfoFromMaster(mBlockId2, mBlockStoreContext);
    mWorker1 = blockInfo1.getLocations().get(0).getWorkerAddress();
    mWorker2 = blockInfo2.getLocations().get(0).getWorkerAddress();
  }

  @Test
  public void evictFullBlock() throws Exception {
    // run the evict job for full block mBlockId1
    waitForJobToFinish(mJobMaster.runJob(new EvictConfig(mBlockId1, 1)));
    Assert.assertFalse(
        AdjustJobTestUtils.checkBlockOnWorker(mBlockId1, mWorker1, mBlockStoreContext));
    Assert
        .assertTrue(AdjustJobTestUtils.checkBlockOnWorker(mBlockId2, mWorker2, mBlockStoreContext));
  }

  @Test
  public void evictLastBlock() throws Exception {
    // run the evict job for the last block mBlockId2
    waitForJobToFinish(mJobMaster.runJob(new EvictConfig(mBlockId2, 1)));
    Assert
        .assertTrue(AdjustJobTestUtils.checkBlockOnWorker(mBlockId1, mWorker1, mBlockStoreContext));
    Assert.assertFalse(
        AdjustJobTestUtils.checkBlockOnWorker(mBlockId2, mWorker2, mBlockStoreContext));
  }
}
