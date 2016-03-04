/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.job.JobConfig;
import alluxio.job.JobDefinition;
import alluxio.job.JobDefinitionRegistry;
import alluxio.job.JobMasterContext;
import alluxio.job.util.JobManagerTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.meta.JobInfo;
import alluxio.thrift.JobManangerCommand;
import alluxio.wire.WorkerInfo;

import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;

/**
 * Tests {@link JobCoordinator}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class, BlockMaster.class, JobDefinitionRegistry.class})
public final class JobCoordinatorTest {
  private FileSystemMaster mFileSystemMaster;
  private BlockMaster mBlockMaster;

  @Before
  public void before() {
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mBlockMaster = Mockito.mock(BlockMaster.class);
  }

  @After
  public void after() {
    JobManagerTestUtils.cleanUpCommandManager();
  }

  @Test
  public void createJobCoordinatorTest() throws Exception {
    WorkerInfo workerInfo = new WorkerInfo();
    long workerId = 0;
    workerInfo.setId(workerId);
    List<WorkerInfo> workerInfoList = Lists.newArrayList(workerInfo);
    Mockito.when(mBlockMaster.getWorkerInfoList()).thenReturn(workerInfoList);

    long jobId = 1;
    JobConfig jobConfig = Mockito.mock(JobConfig.class, Mockito.withSettings().serializable());
    Mockito.when(jobConfig.getName()).thenReturn("mock");
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Object> jobDefinition = Mockito.mock(JobDefinition.class);
    JobDefinitionRegistry singleton = PowerMockito.mock(JobDefinitionRegistry.class);
    Whitebox.setInternalState(JobDefinitionRegistry.class, "INSTANCE", singleton);
    Mockito.when(singleton.getJobDefinition(jobConfig)).thenReturn(jobDefinition);
    Map<WorkerInfo, Object> taskAddressToArgs = Maps.newHashMap();
    taskAddressToArgs.put(workerInfo, Lists.newArrayList(1));
    Mockito.when(jobDefinition.selectExecutors(Mockito.eq(jobConfig), Mockito.eq(workerInfoList),
        Mockito.any(JobMasterContext.class))).thenReturn(taskAddressToArgs);

    JobInfo jobInfo = new JobInfo(jobId, jobConfig.getName(), jobConfig);
    JobCoordinator.create(jobInfo, mFileSystemMaster, mBlockMaster);

    List<JobManangerCommand> commands = CommandManager.INSTANCE.pollAllPendingCommands(workerId);
    Assert.assertEquals(1, commands.size());
    Assert.assertEquals(jobId, commands.get(0).getRunTaskCommand().getJobId());
    Assert.assertEquals(0, commands.get(0).getRunTaskCommand().getTaskId());
  }
}
