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
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.meta.JobInfo;
<<<<<<< HEAD:jobmanager/src/test/java/alluxio/master/job/JobCoordinatorTest.java
import alluxio.thrift.JobManangerCommand;
||||||| merged common ancestors
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.Status;
import alluxio.thrift.TaskInfo;
=======
import alluxio.thrift.JobCommand;
import alluxio.thrift.Status;
import alluxio.thrift.TaskInfo;
>>>>>>> origin/master:job/src/test/java/alluxio/master/job/JobCoordinatorTest.java
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

import java.util.List;
import java.util.Map;

/**
 * Tests {@link JobCoordinator}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class, BlockMaster.class, JobDefinitionRegistry.class})
public final class JobCoordinatorTest {
  private WorkerInfo mWorkerInfo;
  private long mJobId;
  private FileSystemMaster mFileSystemMaster;
  private BlockMaster mBlockMaster;
  private JobInfo mJobInfo;
  private CommandManager mCommandManager;
  private JobDefinition<JobConfig, Object, Object> mJobDefinition;

  @Before
  public void before() throws Exception {
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mBlockMaster = Mockito.mock(BlockMaster.class);
    mCommandManager = new CommandManager();

    // Create mock job info.
    JobConfig jobConfig = Mockito.mock(JobConfig.class, Mockito.withSettings().serializable());
    Mockito.when(jobConfig.getName()).thenReturn("mock");
    mJobId = 1;
    mJobInfo = new JobInfo(mJobId, jobConfig.getName(), jobConfig);

    // Create mock job definition.
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Object, Object> mockJobDefinition = Mockito.mock(JobDefinition.class);
    JobDefinitionRegistry singleton = PowerMockito.mock(JobDefinitionRegistry.class);
    Whitebox.setInternalState(JobDefinitionRegistry.class, "INSTANCE", singleton);
<<<<<<< HEAD:jobmanager/src/test/java/alluxio/master/job/JobCoordinatorTest.java
    Mockito.when(singleton.getJobDefinition(jobConfig)).thenReturn(mockJobDefinition);
    mJobDefinition = mockJobDefinition;

    // Create test worker.
    mWorkerInfo = new WorkerInfo();
    mWorkerInfo.setId(0);
    List<WorkerInfo> workerInfoList = Lists.newArrayList(mWorkerInfo);
    Mockito.when(mBlockMaster.getWorkerInfoList()).thenReturn(workerInfoList);
||||||| merged common ancestors
    Mockito.when(singleton.getJobDefinition(jobConfig)).thenReturn(mJobDefinition);
    Map<WorkerInfo, Object> taskAddressToArgs = Maps.newHashMap();
    taskAddressToArgs.put(workerInfo, Lists.newArrayList(1));
    Mockito.when(mJobDefinition.selectExecutors(Mockito.eq(jobConfig), Mockito.eq(workerInfoList),
        Mockito.any(JobMasterContext.class))).thenReturn(taskAddressToArgs);

    mJobInfo = new JobInfo(mJobId, jobConfig.getName(), jobConfig);
    mCommandManager = new CommandManager();
    mJobCoordinator =
        JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);

=======
    Mockito.when(singleton.getJobDefinition(jobConfig)).thenReturn(mJobDefinition);
    Map<WorkerInfo, Object> taskAddressToArgs = Maps.newHashMap();
    taskAddressToArgs.put(workerInfo, Lists.newArrayList(1));
    Mockito.when(mJobDefinition.selectExecutors(Mockito.eq(jobConfig), Mockito.eq(workerInfoList),
        Mockito.any(JobMasterContext.class))).thenReturn(taskAddressToArgs);

    mJobInfo = new JobInfo(mJobId, jobConfig.getName(), jobConfig);
    mCommandManager = new CommandManager();
    mJobCoordinator =
        JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);
>>>>>>> origin/master:job/src/test/java/alluxio/master/job/JobCoordinatorTest.java
  }

  @Test
  public void createJobCoordinatorTest() throws Exception {
<<<<<<< HEAD:jobmanager/src/test/java/alluxio/master/job/JobCoordinatorTest.java
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);

    List<JobManangerCommand> commands = mCommandManager.pollAllPendingCommands(mWorkerInfo.getId());
||||||| merged common ancestors
    List<JobManangerCommand> commands = mCommandManager.pollAllPendingCommands(mWorkerId);
=======
    List<JobCommand> commands = mCommandManager.pollAllPendingCommands(mWorkerId);
>>>>>>> origin/master:job/src/test/java/alluxio/master/job/JobCoordinatorTest.java
    Assert.assertEquals(1, commands.size());
    Assert.assertEquals(mJobId, commands.get(0).getRunTaskCommand().getJobId());
    Assert.assertEquals(0, commands.get(0).getRunTaskCommand().getTaskId());
  }

  @Test
  public void updateStatusFailureTest() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);
    setTasksWithStatuses(Status.RUNNING, Status.FAILED, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
    Assert.assertEquals("The task execution failed", mJobInfo.getErrorMessage());
  }

  @Test
  public void updateStatusFailureOverCancelTest() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);
    setTasksWithStatuses(Status.RUNNING, Status.FAILED, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
  }

  @Test
  public void updateStatusCancelTest() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);
    setTasksWithStatuses(Status.CANCELED, Status.RUNNING, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.CANCELED, mJobInfo.getStatus());
  }

  @Test
  public void updateStatusRunningTest() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);
    setTasksWithStatuses(Status.COMPLETED, Status.RUNNING, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.RUNNING, mJobInfo.getStatus());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void updateStatusCompletedTest() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);
    setTasksWithStatuses(Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.COMPLETED, mJobInfo.getStatus());
    Mockito.verify(mJobDefinition).join(Mockito.eq(mJobInfo.getJobConfig()), Mockito.anyMap());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void updateStatusJoinFailureTest() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    Mockito.when(mJobDefinition.join(Mockito.eq(mJobInfo.getJobConfig()), Mockito.anyMap()))
        .thenThrow(new UnsupportedOperationException("test exception"));
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);
    setTasksWithStatuses(Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
    Assert.assertEquals("test exception", mJobInfo.getErrorMessage());
  }

  @Test
  public void noTasksTest() throws Exception {
    mockSelectExecutors();
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mJobInfo, mFileSystemMaster, mBlockMaster);
    Assert.assertEquals(Status.COMPLETED, mJobInfo.getStatus());
  }

  /**
   * @param workerInfos the worker infos to return from the mocked selectExecutors method
   */
  private void mockSelectExecutors(WorkerInfo... workerInfos) throws Exception {
    Map<WorkerInfo, Object> taskAddressToArgs = Maps.newHashMap();
    for (WorkerInfo workerInfo : workerInfos) {
      taskAddressToArgs.put(workerInfo, null);
    }
    Mockito
        .when(mJobDefinition.selectExecutors(Mockito.eq(mJobInfo.getJobConfig()),
            Mockito.eq(Lists.newArrayList(mWorkerInfo)), Mockito.any(JobMasterContext.class)))
        .thenReturn(taskAddressToArgs);
  }

  private void setTasksWithStatuses(Status... statuses) throws Exception {
    int taskId = 0;
    for (Status status : statuses) {
      mJobInfo.setTaskInfo(taskId, new TaskInfo(mJobId, 0, status, "", null));
      taskId++;
    }
  }
}
