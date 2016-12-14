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
import alluxio.client.file.FileSystemContext;
import alluxio.job.JobMasterContext;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;

/**
 * Tests {@link EvictDefinition}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, BlockStoreContext.class, FileSystemContext.class,
    JobMasterContext.class})
public final class EvictDefinitionTest {
  private static final long TEST_BLOCK_ID = 1L;
  private static final WorkerNetAddress ADDRESS_1 =
      new WorkerNetAddress().setHost("host1").setDataPort(10);
  private static final WorkerNetAddress ADDRESS_2 =
      new WorkerNetAddress().setHost("host2").setDataPort(10);
  private static final WorkerNetAddress ADDRESS_3 =
      new WorkerNetAddress().setHost("host3").setDataPort(10);
  private static final WorkerInfo WORKER_INFO_1 = new WorkerInfo().setAddress(ADDRESS_1);
  private static final WorkerInfo WORKER_INFO_2 = new WorkerInfo().setAddress(ADDRESS_2);
  private static final WorkerInfo WORKER_INFO_3 = new WorkerInfo().setAddress(ADDRESS_3);
  private static final Map<WorkerInfo, SerializableVoid> EMPTY = Maps.newHashMap();

  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private BlockStoreContext mMockBlockStoreContext;
  private JobMasterContext mMockJobMasterContext;

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
   * @param blockLocations where the block is stored currently
   * @param replicas how many replicas to evict
   * @param workerInfoList a list of currently available job workers
   * @return the selection result
   */
  private Map<WorkerInfo, SerializableVoid> selectExecutorsTestHelper(
      List<BlockLocation> blockLocations, int replicas, List<WorkerInfo> workerInfoList)
      throws Exception {
    BlockInfo blockInfo = new BlockInfo().setBlockId(TEST_BLOCK_ID);
    blockInfo.setLocations(blockLocations);
    Mockito.when(mMockBlockStore.getInfo(TEST_BLOCK_ID)).thenReturn(blockInfo);
    Mockito.when(mMockFileSystemContext.getAlluxioBlockStore()).thenReturn(mMockBlockStore);
    Mockito.when(mMockFileSystemContext.getBlockStoreContext()).thenReturn(mMockBlockStoreContext);

    EvictConfig config = new EvictConfig(TEST_BLOCK_ID, replicas);
    EvictDefinition definition = new EvictDefinition(mMockFileSystemContext);
    return definition.selectExecutors(config, workerInfoList, mMockJobMasterContext);
  }

  @Test
  public void selectExecutorsNoBlockWorkerHasBlock() throws Exception {
    Map<WorkerInfo, SerializableVoid> result =
        selectExecutorsTestHelper(Lists.<BlockLocation>newArrayList(), 1,
            Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    // Expect none as no worker has this copy
    Assert.assertEquals(EMPTY, result);
  }

  @Test
  public void selectExecutorsNoJobWorkerHasBlock() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_2, WORKER_INFO_3));
    // Expect none as no worker that is available has this copy
    Assert.assertEquals(EMPTY, result);
  }

  @Test
  public void selectExecutorsOnlyOneBlockWorkerHasBlock() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(
        Lists.newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1)), 1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_1, null);
    // Expect the only worker 1 having this block
    Assert.assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsAnyOneWorkers() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(Lists
            .newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1),
                new BlockLocation().setWorkerAddress(ADDRESS_2),
                new BlockLocation().setWorkerAddress(ADDRESS_3)), 1,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    // Expect one worker from all workers having this block
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(null, result.values().iterator().next());
  }

  @Test
  public void selectExecutorsAllWorkers() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(Lists
            .newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1),
                new BlockLocation().setWorkerAddress(ADDRESS_2),
                new BlockLocation().setWorkerAddress(ADDRESS_3)), 3,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_1, null);
    expected.put(WORKER_INFO_2, null);
    expected.put(WORKER_INFO_3, null);
    // Expect all workers are selected as they all have this block
    Assert.assertEquals(expected, result);
  }

  @Test
  public void selectExecutorsBothWorkers() throws Exception {
    Map<WorkerInfo, SerializableVoid> result = selectExecutorsTestHelper(Lists
            .newArrayList(new BlockLocation().setWorkerAddress(ADDRESS_1),
                new BlockLocation().setWorkerAddress(ADDRESS_2)), 3,
        Lists.newArrayList(WORKER_INFO_1, WORKER_INFO_2, WORKER_INFO_3));
    Map<WorkerInfo, SerializableVoid> expected = Maps.newHashMap();
    expected.put(WORKER_INFO_1, null);
    expected.put(WORKER_INFO_2, null);
    // Expect both workers having this block should be selected
    Assert.assertEquals(expected, result);
  }
}
