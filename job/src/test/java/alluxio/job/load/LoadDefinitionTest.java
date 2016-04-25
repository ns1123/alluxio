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
import alluxio.job.JobMasterContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Sets;
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
import java.util.Random;

/**
 * Tests {@link LoadDefinition}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class, JobMasterContext.class})
public class LoadDefinitionTest {
  private static final String TEST_URI = "/test";

  private static final List<WorkerInfo> WORKERS = new ImmutableList.Builder<WorkerInfo>()
      .add(new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host0")))
      .add(new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host1")))
      .add(new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host2")))
      .add(new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host3"))).build();

  private JobMasterContext mMockJobMasterContext;
  private FileSystemMaster mMockFileSystemMaster;

  @Before
  public void before() throws Exception {
    mMockJobMasterContext = PowerMockito.mock(JobMasterContext.class);
    mMockFileSystemMaster = PowerMockito.mock(FileSystemMaster.class);
    Mockito.when(mMockJobMasterContext.getFileSystemMaster()).thenReturn(mMockFileSystemMaster);
  }

  @Test
  public void assignRandomWorkersTest() throws Exception {
    Random random = new Random();
    int size = random.nextInt(WORKERS.size());
    createFileWithNoLocations(TEST_URI, size);
    LoadConfig config = new LoadConfig(TEST_URI);
    Map<WorkerInfo, List<Long>> actual =
        new LoadDefinition().selectExecutors(config, WORKERS, mMockJobMasterContext);
    Assert.assertEquals(Sets.newHashSet(WORKERS.subList(0, size)), actual.keySet());
  }

  private FileInfo createFileWithNoLocations(String testFile, int numOfBlocks) throws Exception {
    FileInfo testFileInfo = new FileInfo();
    AlluxioURI uri = new AlluxioURI(testFile);
    List<FileBlockInfo> blockInfos = Lists.newArrayList();
    for (int i = 0; i < numOfBlocks; i++) {
      blockInfos.add(new FileBlockInfo()
          .setBlockInfo(new BlockInfo().setLocations(Lists.<BlockLocation>newArrayList())));
    }
    testFileInfo.setFolder(false).setPath(testFile);
    Mockito.when(mMockFileSystemMaster.getFileInfoList(uri))
        .thenReturn(Lists.newArrayList(testFileInfo));
    Mockito.when(mMockFileSystemMaster.getFileBlockInfoList(uri)).thenReturn(blockInfos);
    Mockito.when(mMockFileSystemMaster.getFileInfo(uri)).thenReturn(testFileInfo);
    return testFileInfo;
  }
}
