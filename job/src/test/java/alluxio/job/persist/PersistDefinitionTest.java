/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.persist;

import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.job.JobMasterContext;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

/**
 * Tests {@link PersistDefinition}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(
    {AlluxioBlockStore.class, FileSystem.class, FileSystemContext.class, JobMasterContext.class})
public final class PersistDefinitionTest {
  private FileSystem mMockFileSystem;
  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private JobMasterContext mMockJobMasterContext;

  @Before
  public void before() {
    mMockJobMasterContext = Mockito.mock(JobMasterContext.class);
    mMockFileSystem = PowerMockito.mock(FileSystem.class);
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    when(mMockJobMasterContext.getFileSystem()).thenReturn(mMockFileSystem);
    when(mMockJobMasterContext.getFileSystemContext()).thenReturn(mMockFileSystemContext);
    when(mMockFileSystemContext.getAluxioBlockStore()).thenReturn(mMockBlockStore);
  }

  @Test
  public void selectExecutorsTest() throws Exception {
    PersistConfig config = new PersistConfig("/test", true);

    WorkerNetAddress workerNetAddress = new WorkerNetAddress().setDataPort(10);
    WorkerInfo workerInfo = new WorkerInfo().setAddress(workerNetAddress);

    long blockId = 1;
    BlockInfo blockInfo = new BlockInfo().setBlockId(blockId);
    FileBlockInfo fileBlockInfo = new FileBlockInfo().setBlockInfo(blockInfo);
    BlockLocation location = new BlockLocation();
    location.setWorkerAddress(workerNetAddress);
    blockInfo.setLocations(Lists.newArrayList(location));
    URIStatus status = Mockito.mock(URIStatus.class);
    Mockito.when(mMockFileSystem.getStatus(Mockito.eq(new AlluxioURI("/test")))).thenReturn(status);
    Mockito.when(status.getFileBlockInfos()).thenReturn(Lists.newArrayList(fileBlockInfo));

    Map<WorkerInfo, Void> result = (new PersistDefinition())
        .selectExecutors(config, Lists.newArrayList(workerInfo), mMockJobMasterContext);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(workerInfo, result.keySet().iterator().next());
  }

  @Test
  public void selectExecutorsMissingLocationTest() throws Exception {
    PersistConfig config = new PersistConfig("/test", true);
    JobMasterContext context = new JobMasterContext(1);

    long blockId = 1;
    BlockInfo blockInfo = new BlockInfo().setBlockId(blockId);
    FileBlockInfo fileBlockInfo = new FileBlockInfo().setBlockInfo(blockInfo);
    URIStatus status = Mockito.mock(URIStatus.class);
    Mockito.when(mMockFileSystem.getStatus(Mockito.eq(new AlluxioURI("/test")))).thenReturn(status);
    Mockito.when(status.getFileBlockInfos()).thenReturn(Lists.newArrayList(fileBlockInfo));

    try {
      (new PersistDefinition()).selectExecutors(config, Lists.newArrayList(new WorkerInfo()),
          context);
    } catch (Exception e) {
      Assert.assertEquals("Block " + blockId + " does not exist", e.getMessage());
    }
  }
}
