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
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.job.JobMasterContext;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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
@Ignore
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioBlockStore.class, BaseFileSystem.class, FileSystemContext.class,
    JobMasterContext.class})
public final class PersistDefinitionTest {
  private FileSystem mMockFileSystem;
  private FileSystemContext mMockFileSystemContext;
  private AlluxioBlockStore mMockBlockStore;
  private JobMasterContext mMockJobMasterContext;

  @Before
  public void before() {
    mMockJobMasterContext = Mockito.mock(JobMasterContext.class);
    mMockFileSystem = PowerMockito.mock(BaseFileSystem.class);
    mMockFileSystemContext = PowerMockito.mock(FileSystemContext.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    when(mMockJobMasterContext.getFileSystem()).thenReturn(mMockFileSystem);
    when(mMockJobMasterContext.getFileSystemContext()).thenReturn(mMockFileSystemContext);
    // when(mMockFileSystemContext.getAlluxioBlockStore()).thenReturn(mMockBlockStore);
  }

  @Test
  public void selectExecutorsTest() throws Exception {
    AlluxioURI uri = new AlluxioURI("/test");
    PersistConfig config = new PersistConfig(uri.getPath(), true);

    WorkerNetAddress workerNetAddress = new WorkerNetAddress().setDataPort(10);
    WorkerInfo workerInfo = new WorkerInfo().setAddress(workerNetAddress);

    long blockId = 1;
    BlockInfo blockInfo = new BlockInfo().setBlockId(blockId);
    FileBlockInfo fileBlockInfo = new FileBlockInfo().setBlockInfo(blockInfo);
    BlockLocation location = new BlockLocation();
    location.setWorkerAddress(workerNetAddress);
    blockInfo.setLocations(Lists.newArrayList(location));
    FileInfo testFileInfo = new FileInfo();
    testFileInfo.setFileBlockInfos(Lists.newArrayList(fileBlockInfo));
    Mockito.when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));

    Map<WorkerInfo, Void> result = (new PersistDefinition())
        .selectExecutors(config, Lists.newArrayList(workerInfo), mMockJobMasterContext);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(workerInfo, result.keySet().iterator().next());
  }

  @Test
  public void selectExecutorsMissingLocationTest() throws Exception {
    AlluxioURI uri = new AlluxioURI("/test");
    PersistConfig config = new PersistConfig(uri.getPath(), true);

    long blockId = 1;
    BlockInfo blockInfo = new BlockInfo().setBlockId(blockId);
    FileBlockInfo fileBlockInfo = new FileBlockInfo().setBlockInfo(blockInfo);
    FileInfo testFileInfo = new FileInfo();
    testFileInfo.setFileBlockInfos(Lists.newArrayList(fileBlockInfo));
    Mockito.when(mMockFileSystem.getStatus(uri)).thenReturn(new URIStatus(testFileInfo));

    try {
      (new PersistDefinition())
          .selectExecutors(config, Lists.newArrayList(new WorkerInfo()), mMockJobMasterContext);
    } catch (Exception e) {
      Assert.assertEquals("Block " + blockId + " does not exist", e.getMessage());
    }
  }
}
