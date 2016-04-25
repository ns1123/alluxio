/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.persist;

import alluxio.AlluxioURI;
import alluxio.job.JobMasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
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
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

/**
 * Tests {@link PersistDefinition}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class, BlockMaster.class})
public final class PersistDefinitionTest {
  private FileSystemMaster mFileSystemMaster;
  private BlockMaster mBlockMaster;

  @Before
  public void before() {
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mBlockMaster = Mockito.mock(BlockMaster.class);
  }

  @Test
  public void selectExecutorsTest() throws Exception {
    PersistConfig config = new PersistConfig("/test", true);
    JobMasterContext context = new JobMasterContext(mFileSystemMaster, mBlockMaster, 1);

    WorkerNetAddress workerNetAddress = new WorkerNetAddress().setDataPort(10);
    WorkerInfo workerInfo = new WorkerInfo().setAddress(workerNetAddress);

    long blockId = 1;
    BlockInfo blockInfo = new BlockInfo().setBlockId(blockId);
    FileBlockInfo fileBlockInfo = new FileBlockInfo().setBlockInfo(blockInfo);
    BlockLocation location = new BlockLocation();
    location.setWorkerAddress(workerNetAddress);
    blockInfo.setLocations(Lists.newArrayList(location));
    Mockito.when(mFileSystemMaster.getFileBlockInfoList(Mockito.eq(new AlluxioURI("/test"))))
        .thenReturn(Lists.newArrayList(fileBlockInfo));

    Map<WorkerInfo, Void> result =
        (new PersistDefinition()).selectExecutors(config, Lists.newArrayList(workerInfo), context);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(workerInfo, result.keySet().iterator().next());
  }

  @Test
  public void selectExecutorsMissingLocationTest() throws Exception {
    PersistConfig config = new PersistConfig("/test", true);
    JobMasterContext context = new JobMasterContext(mFileSystemMaster, mBlockMaster, 1);

    long blockId = 1;
    BlockInfo blockInfo = new BlockInfo().setBlockId(blockId);
    FileBlockInfo fileBlockInfo = new FileBlockInfo().setBlockInfo(blockInfo);
    Mockito.when(mFileSystemMaster.getFileBlockInfoList(Mockito.eq(new AlluxioURI("/test"))))
        .thenReturn(Lists.newArrayList(fileBlockInfo));

    try {
      (new PersistDefinition()).selectExecutors(config, Lists.newArrayList(new WorkerInfo()),
          context);
    } catch (Exception e) {
      Assert.assertEquals("Block " + blockId + " does not exist", e.getMessage());
    }
  }
}
