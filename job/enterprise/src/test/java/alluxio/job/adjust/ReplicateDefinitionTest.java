/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.adjust;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.BufferedBlockInStream;
import alluxio.client.block.BufferedBlockOutStream;
import alluxio.client.block.TestBufferedBlockInStream;
import alluxio.client.block.TestBufferedBlockOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.NoWorkerException;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.SerializableVoid;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link ReplicateConfig}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, BlockStoreContext.class, FileSystemContext.class,
    JobMasterContext.class})
public final class ReplicateDefinitionTest {
  private static final long TEST_BLOCK_ID = 1L;
  private static final long TEST_BLOCK_SIZE = 512L;
  private static final WorkerNetAddress ADDRESS_1 =
      new WorkerNetAddress().setHost("host1").setDataPort(10);
  private static final WorkerNetAddress ADDRESS_2 =
      new WorkerNetAddress().setHost("host2").setDataPort(10);
  private static final WorkerNetAddress ADDRESS_3 =
      new WorkerNetAddress().setHost("host3").setDataPort(10);
  private static final WorkerNetAddress LOCAL_ADDRESS =
      new WorkerNetAddress().setHost(NetworkAddressUtils.getLocalHostName()).setDataPort(10);
  private static final WorkerInfo WORKER_INFO_1 = new WorkerInfo().setAddress(ADDRESS_1);
  private static final WorkerInfo WORKER_INFO_2 = new WorkerInfo().setAddress(ADDRESS_2);
  private static final WorkerInfo WORKER_INFO_3 = new WorkerInfo().setAddress(ADDRESS_3);

  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private BlockStoreContext mMockBlockStoreContext;
  private JobMasterContext mMockJobMasterContext;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() {
    mMockJobMasterContext = Mockito.mock(JobMasterContext.class);
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockBlockStoreContext = PowerMockito.mock(BlockStoreContext.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
  }

  /**
   * Helper function to select executors.
   *
   * @param blockLocations where the block is store currently
   * @param numReplicas how many replicas to adjust or evict
   * @param workerInfoList a list of current available job workers
   * @return the selection result
   */
  private Map<WorkerInfo, SerializableVoid> selectExecutorsTestHelper(
      List<BlockLocation> blockLocations, int numReplicas, List<WorkerInfo> workerInfoList)
      throws Exception {
    BlockInfo blockInfo = new BlockInfo().setBlockId(TEST_BLOCK_ID);
    blockInfo.setLocations(blockLocations);
    Mockito.when(mMockBlockStore.getInfo(TEST_BLOCK_ID)).thenReturn(blockInfo);
    Mockito.when(mMockFileSystemContext.getAlluxioBlockStore()).thenReturn(mMockBlockStore);

    ReplicateConfig config = new ReplicateConfig(TEST_BLOCK_ID, numReplicas);
    ReplicateDefinition definition = new ReplicateDefinition(mMockFileSystemContext);
    return definition.selectExecutors(config, workerInfoList, mMockJobMasterContext);
  }

  /**
   * Helper function to run a replicate task.
   *
   * @param blockWorkers available block workers
   * @param mockInStream mock blockInStream returned by the Block Store
   * @param mockOutStream mock blockOutStream returned by the Block Store
   */
  private void runTaskReplicateTestHelper(List<BlockWorkerInfo> blockWorkers,
      BufferedBlockInStream mockInStream, BufferedBlockOutStream mockOutStream) throws Exception {
    Mockito.when(mMockBlockStore.getWorkerInfoList()).thenReturn(blockWorkers);
    Mockito.when(mMockBlockStore.getInStream(TEST_BLOCK_ID)).thenReturn(mockInStream);
    Mockito.when(mMockBlockStore.getOutStream(TEST_BLOCK_ID, -1, LOCAL_ADDRESS))
        .thenReturn(mockOutStream);
    Mockito.when(mMockBlockStore.getInfo(TEST_BLOCK_ID)).thenReturn(
        new BlockInfo().setBlockId(TEST_BLOCK_ID)
            .setLocations(Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1))));
    Mockito.when(mMockFileSystemContext.getAlluxioBlockStore()).thenReturn(mMockBlockStore);

    ReplicateConfig config = new ReplicateConfig(TEST_BLOCK_ID, 1 /* value not used */);
    ReplicateDefinition definition = new ReplicateDefinition(mMockFileSystemContext);
    definition.runTask(config, null, new JobWorkerContext(1, 1));
  }

  @Test
  public void selectExecutorsOnlyOneWorkerAvailable() throws Exception {
    Map<WorkerInfo, SerializableVoid> result =
        selectExecutorsTestHelper(Lists.<BlockLocation>newArrayList(), 1,
            Lists.newArrayList(WORKER_INFO_1));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_1, null);
    // select the only worker
    Assert.assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsOnlyOneWorkerValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_2, null);
    // select one worker left
    Assert.assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsTwoWorkersValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 2,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_2, null);
    expected.put(WORKER_INFO_3, null);
    // select both workers left
    Assert.assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsOneOutOFTwoWorkersValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    // select one worker out of two
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(null, result.values().iterator().next());
  }

  @Test
  public void selectExecutorsNoWorkerValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_1));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    // select none as no choice left
    Assert.assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsInsufficientWorkerValid() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 2,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_2, null);
    // select the only worker left though more copies are requested
    Assert.assertEquals(expected, result);
  }

  @Test
  public void runTaskNoBlockWorker() throws Exception {
    byte[] input = BufferUtils.getIncreasingByteArray(0, (int) TEST_BLOCK_SIZE);

    TestBufferedBlockInStream mockInStream = new TestBufferedBlockInStream(TEST_BLOCK_ID, input);
    TestBufferedBlockOutStream mockOutStream =
        new TestBufferedBlockOutStream(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mMockBlockStoreContext);
    mThrown.expect(NoWorkerException.class);
    mThrown.expectMessage(ExceptionMessage.NO_LOCAL_BLOCK_WORKER_REPLICATE_TASK
        .getMessage(TEST_BLOCK_ID));
    runTaskReplicateTestHelper(Lists.<BlockWorkerInfo>newArrayList(), mockInStream, mockOutStream);
  }

  @Test
  public void runTaskLocalBlockWorker() throws Exception {
    byte[] input = BufferUtils.getIncreasingByteArray(0, (int) TEST_BLOCK_SIZE);

    TestBufferedBlockInStream mockInStream = new TestBufferedBlockInStream(TEST_BLOCK_ID, input);
    TestBufferedBlockOutStream mockOutStream =
        new TestBufferedBlockOutStream(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mMockBlockStoreContext);
    BlockWorkerInfo localBlockWorker = new BlockWorkerInfo(LOCAL_ADDRESS, TEST_BLOCK_SIZE, 0);
    runTaskReplicateTestHelper(Lists.newArrayList(localBlockWorker), mockInStream, mockOutStream);
    Assert.assertTrue(Arrays.equals(input, mockOutStream.getWrittenData()));
  }
}
