/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.load;

import alluxio.AlluxioURI;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.job.JobMasterContext;
import alluxio.job.load.LoadDefinition.LoadTask;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Tests {@link LoadDefinition}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, JobMasterContext.class, FileSystemContext.class,
    AlluxioBlockStore.class})
public class LoadDefinitionTest {
  private static final String TEST_URI = "/test";

  private static final List<WorkerInfo> JOB_WORKERS = new ImmutableList.Builder<WorkerInfo>()
      .add(new WorkerInfo().setId(0).setAddress(new WorkerNetAddress().setHost("host0")))
      .add(new WorkerInfo().setId(1).setAddress(new WorkerNetAddress().setHost("host1")))
      .add(new WorkerInfo().setId(2).setAddress(new WorkerNetAddress().setHost("host2")))
      .add(new WorkerInfo().setId(3).setAddress(new WorkerNetAddress().setHost("host3"))).build();

  private static final List<BlockWorkerInfo> BLOCK_WORKERS =
      new ImmutableList.Builder<BlockWorkerInfo>()
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host0"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host1"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host2"), 0, 0))
          .add(new BlockWorkerInfo(new WorkerNetAddress().setHost("host3"), 0, 0)).build();

  private FileSystem mMockFileSystem;
  private AlluxioBlockStore mMockBlockStore;
  private JobMasterContext mMockJobMasterContext;

  @Before
  public void before() throws Exception {
    mMockJobMasterContext = Mockito.mock(JobMasterContext.class);
    mMockFileSystem = PowerMockito.mock(FileSystem.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create()).thenReturn(mMockBlockStore);
    Mockito.when(mMockBlockStore.getWorkerInfoList()).thenReturn(BLOCK_WORKERS);
  }

  @Test
  public void replicationSatisfied() throws Exception {
    int numBlocks = 7;
    int replication = 3;
    createFileWithNoLocations(TEST_URI, numBlocks);
    LoadConfig config = new LoadConfig(TEST_URI, replication);
    Map<WorkerInfo, ArrayList<LoadTask>> assignments =
        new LoadDefinition(mMockFileSystem).selectExecutors(config,
            JOB_WORKERS, mMockJobMasterContext);
    // Check that we are loading the right number of blocks.
    int totalBlockLoads = 0;
    for (List<LoadTask> blocks : assignments.values()) {
      totalBlockLoads += blocks.size();
    }
    Assert.assertEquals(numBlocks * replication, totalBlockLoads);
    checkLocalAssignments(assignments);
  }

  @Test
  public void multipleAlluxioWorkersOneJobWorkerSameHostReplication() throws Exception {
    // Two block workers on the same host with different rpc ports.
    Mockito.when(mMockBlockStore.getWorkerInfoList())
        .thenReturn(Arrays.asList(
            new BlockWorkerInfo(new WorkerNetAddress().setHost("host0").setRpcPort(0), 0, 0),
            new BlockWorkerInfo(new WorkerNetAddress().setHost("host0").setRpcPort(1), 0, 0)));
    WorkerInfo jobWorker = new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host0"));
    List<WorkerInfo> singleJobWorker = Arrays.asList(jobWorker);

    int numBlocks = 3;
    int replication = 2;
    createFileWithNoLocations(TEST_URI, numBlocks);
    LoadConfig config = new LoadConfig(TEST_URI, replication);
    Map<WorkerInfo, ArrayList<LoadTask>> assignments =
        new LoadDefinition(mMockFileSystem).selectExecutors(config,
            singleJobWorker, mMockJobMasterContext);
    Assert.assertEquals(1, assignments.size());
    // Load 3 blocks to each of the two block workers.
    Assert.assertEquals(6, assignments.get(jobWorker).size());
  }

  @Test
  public void multipleJobWorkersOneBlockWorkerSameHostReplication() throws Exception {
    Mockito.when(mMockBlockStore.getWorkerInfoList()).thenReturn(
        Arrays.asList(new BlockWorkerInfo(new WorkerNetAddress().setHost("host0"), 0, 0)));
    WorkerInfo jobWorker1 =
        new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host0").setRpcPort(0));
    WorkerInfo jobWorker2 =
        new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host0").setRpcPort(1));
    List<WorkerInfo> jobWorkers = Arrays.asList(jobWorker1, jobWorker2);

    int numBlocks = 4;
    int replication = 1;
    createFileWithNoLocations(TEST_URI, numBlocks);
    LoadConfig config = new LoadConfig(TEST_URI, replication);
    Map<WorkerInfo, ArrayList<LoadTask>> assignments =
        new LoadDefinition(mMockFileSystem).selectExecutors(config,
            jobWorkers, mMockJobMasterContext);
    Assert.assertEquals(2, assignments.size());
    // Each worker gets half the blocks.
    Assert.assertEquals(2, assignments.get(jobWorker1).size());
    Assert.assertEquals(2, assignments.get(jobWorker2).size());
  }

  @Test
  public void multipleJobWorkersMultipleBlockWorkerSameHostReplication() throws Exception {
    List<BlockWorkerInfo> blockWorkers = Arrays.asList(
        new BlockWorkerInfo(new WorkerNetAddress().setHost("host0").setRpcPort(0), 0, 0),
        new BlockWorkerInfo(new WorkerNetAddress().setHost("host0").setRpcPort(1), 0, 0),
        new BlockWorkerInfo(new WorkerNetAddress().setHost("host0").setRpcPort(2), 0, 0));
    Mockito.when(mMockBlockStore.getWorkerInfoList()).thenReturn(blockWorkers);
    WorkerInfo jobWorker1 =
        new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host0").setRpcPort(0));
    WorkerInfo jobWorker2 =
        new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host0").setRpcPort(1));
    List<WorkerInfo> jobWorkers = Arrays.asList(jobWorker1, jobWorker2);

    int numBlocks = 12;
    int replication = 3;
    createFileWithNoLocations(TEST_URI, numBlocks);
    LoadConfig config = new LoadConfig(TEST_URI, replication);
    Map<WorkerInfo, ArrayList<LoadTask>> assignments =
        new LoadDefinition(mMockFileSystem).selectExecutors(config,
            jobWorkers, mMockJobMasterContext);

    Assert.assertEquals(2, assignments.size());
    // 36 total block writes should be divided evenly between the job workers.
    Assert.assertEquals(18, assignments.get(jobWorker1).size());
    Assert.assertEquals(18, assignments.get(jobWorker2).size());
    // 1/3 of the block writes should go to each block worker
    int workerCount1 = 0;
    int workerCount2 = 0;
    int workerCount3 = 0;
    for (Collection<LoadTask> tasks : assignments.values()) {
      for (LoadTask task : tasks) {
        if (task.getWorkerNetAddress().equals(blockWorkers.get(0).getNetAddress())) {
          workerCount1++;
        } else if (task.getWorkerNetAddress().equals(blockWorkers.get(1).getNetAddress())) {
          workerCount2++;
        } else if (task.getWorkerNetAddress().equals(blockWorkers.get(2).getNetAddress())) {
          workerCount3++;
        } else {
          throw new RuntimeException("All load tasks must be assigned to one of the block workers");
        }
      }
    }
    Assert.assertEquals(12, workerCount1);
    Assert.assertEquals(12, workerCount2);
    Assert.assertEquals(12, workerCount3);
  }

  @Test
  public void skipJobWorkersWithoutLocalBlockWorkers() throws Exception {
    List<BlockWorkerInfo> blockWorkers =
        Arrays.asList(new BlockWorkerInfo(new WorkerNetAddress().setHost("host0"), 0, 0));
    Mockito.when(mMockBlockStore.getWorkerInfoList()).thenReturn(blockWorkers);
    createFileWithNoLocations(TEST_URI, 10);
    LoadConfig config = new LoadConfig(TEST_URI, 1);
    Map<WorkerInfo, ArrayList<LoadTask>> assignments =
        new LoadDefinition(mMockFileSystem).selectExecutors(config,
            JOB_WORKERS, mMockJobMasterContext);
    Assert.assertEquals(1, assignments.size());
    Assert.assertEquals(10, assignments.values().iterator().next().size());
  }

  @Test
  public void notEnoughWorkersForReplication() throws Exception {
    createFileWithNoLocations(TEST_URI, 1);
    LoadConfig config = new LoadConfig(TEST_URI, 5); // set replication to 5
    try {
      new LoadDefinition(mMockFileSystem).selectExecutors(config,
          JOB_WORKERS, mMockJobMasterContext);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals("Failed to find enough block workers to replicate to", e.getMessage());
    }
  }

  @Test
  public void notEnoughJobWorkersWithLocalBlockWorkers() throws Exception {
    List<BlockWorkerInfo> blockWorkers =
        Arrays.asList(new BlockWorkerInfo(new WorkerNetAddress().setHost("host0"), 0, 0),
            new BlockWorkerInfo(new WorkerNetAddress().setHost("otherhost"), 0, 0));
    Mockito.when(mMockBlockStore.getWorkerInfoList()).thenReturn(blockWorkers);
    createFileWithNoLocations(TEST_URI, 1);
    LoadConfig config = new LoadConfig(TEST_URI, 2); // set replication to 2
    try {
      new LoadDefinition(mMockFileSystem).selectExecutors(config,
          JOB_WORKERS, mMockJobMasterContext);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

  /**
   * Checks that job workers are only assigned to load blocks on local block workers.
   *
   * @param assignments the assignments map to check
   */
  private void checkLocalAssignments(Map<WorkerInfo, ArrayList<LoadTask>> assignments) {
    for (Entry<WorkerInfo, ArrayList<LoadTask>> assignment : assignments.entrySet()) {
      String host = assignment.getKey().getAddress().getHost();
      for (LoadTask task : assignment.getValue()) {
        Assert.assertEquals(host, task.getWorkerNetAddress().getHost());
      }
    }
  }

  private FileInfo createFileWithNoLocations(String testFile, int numOfBlocks) throws Exception {
    FileInfo testFileInfo = new FileInfo();
    AlluxioURI uri = new AlluxioURI(testFile);
    List<FileBlockInfo> blockInfos = Lists.newArrayList();
    for (int i = 0; i < numOfBlocks; i++) {
      blockInfos.add(new FileBlockInfo()
          .setBlockInfo(new BlockInfo().setLocations(Lists.<BlockLocation>newArrayList())));
    }
    testFileInfo.setFolder(false).setPath(testFile).setFileBlockInfos(blockInfos);
    Mockito.when(mMockFileSystem.listStatus(uri))
        .thenReturn(Lists.newArrayList(new URIStatus(testFileInfo)));
    Mockito.when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));
    return testFileInfo;
  }
}
