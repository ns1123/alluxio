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
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.meta.JobInfo;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.Status;
import alluxio.thrift.TaskInfo;
import alluxio.wire.WorkerInfo;

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
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link JobCoordinator}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class, BlockMaster.class, JobDefinitionRegistry.class})
public final class JobCoordinatorTest {
  private long mWorkerId;
  private long mJobId;
  private FileSystemMaster mFileSystemMaster;
  private BlockMaster mBlockMaster;
  private JobInfo mJobInfo;
  private JobCoordinator mJobCoordinator;
  private CommandManager mCommandManager;
  private JobDefinition<JobConfig, Object, Object> mJobDefinition;

  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mBlockMaster = Mockito.mock(BlockMaster.class);
    WorkerInfo workerInfo = new WorkerInfo();
    mWorkerId = 0;
    workerInfo.setId(mWorkerId);
    List<WorkerInfo> workerInfoList = Lists.newArrayList(workerInfo);
    Mockito.when(mBlockMaster.getWorkerInfoList()).thenReturn(workerInfoList);

    mJobId = 1;
    JobConfig jobConfig = Mockito.mock(JobConfig.class, Mockito.withSettings().serializable());
    Mockito.when(jobConfig.getName()).thenReturn("mock");
    mJobDefinition = Mockito.mock(JobDefinition.class);
    JobDefinitionRegistry singleton = PowerMockito.mock(JobDefinitionRegistry.class);
    Whitebox.setInternalState(JobDefinitionRegistry.class, "INSTANCE", singleton);
    Mockito.when(singleton.getJobDefinition(jobConfig)).thenReturn(mJobDefinition);
    Map<WorkerInfo, Object> taskAddressToArgs = Maps.newHashMap();
    taskAddressToArgs.put(workerInfo, Lists.newArrayList(1));
    Mockito.when(mJobDefinition.selectExecutors(Mockito.eq(jobConfig), Mockito.eq(workerInfoList),
        Mockito.any(JobMasterContext.class))).thenReturn(taskAddressToArgs);

    mJobInfo = new JobInfo(mJobId, jobConfig.getName(), jobConfig);
    mCommandManager = new CommandManager();
    mJobCoordinator =
        JobCoordinator.create(mCommandManager, new ArrayList<WorkerInfo>(), mJobInfo);
  }

  @Test
  public void createJobCoordinatorTest() throws Exception {
    List<JobManangerCommand> commands = mCommandManager.pollAllPendingCommands(mWorkerId);
    Assert.assertEquals(1, commands.size());
    Assert.assertEquals(mJobId, commands.get(0).getRunTaskCommand().getJobId());
    Assert.assertEquals(0, commands.get(0).getRunTaskCommand().getTaskId());
  }

  @Test
  public void updateStatusFailureTest() {
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.RUNNING, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.FAILED, "failed", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobCoordinator.updateStatus();

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
    Assert.assertEquals("The task execution failed", mJobInfo.getErrorMessage());
  }

  @Test
  public void updateStatusFailureOverCancelTest() {
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.CANCELED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.FAILED, "failed", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobCoordinator.updateStatus();

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
  }

  @Test
  public void updateStatusCancelTest() {
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.CANCELED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.RUNNING, "", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobCoordinator.updateStatus();

    Assert.assertEquals(Status.CANCELED, mJobInfo.getStatus());
  }

  @Test
  public void updateStatusRunningTest() {
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.RUNNING, "", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobCoordinator.updateStatus();

    Assert.assertEquals(Status.RUNNING, mJobInfo.getStatus());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void updateStatusCompletedTest() throws Exception {
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobCoordinator.updateStatus();

    Assert.assertEquals(Status.COMPLETED, mJobInfo.getStatus());
    Mockito.verify(mJobDefinition).join(Mockito.eq(mJobInfo.getJobConfig()), Mockito.anyMap());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void updateStatusJoinFailureTest() throws Exception {
    Mockito.when(mJobDefinition.join(Mockito.eq(mJobInfo.getJobConfig()), Mockito.anyMap()))
        .thenThrow(new UnsupportedOperationException("test exception"));
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobCoordinator.updateStatus();

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
    Assert.assertEquals("test exception", mJobInfo.getErrorMessage());
  }
}
