/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.command;

import alluxio.job.JobConfig;
import alluxio.job.JobWorkerContext;
import alluxio.job.TestJobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.thrift.JobCommand;
import alluxio.thrift.RunTaskCommand;
import alluxio.thrift.TaskInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.job.JobMasterClient;
import alluxio.worker.job.command.CommandHandlingExecutor;
import alluxio.worker.job.task.TaskExecutorManager;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link CommandHandlingExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TaskExecutorManager.class, WorkerNetAddress.class})
public final class CommandHandlingExecutorTest {
  private CommandHandlingExecutor mCommandHandlingExecutor;
  private JobMasterClient mJobMasterClient;
  private long mWorkerId;
  private TaskExecutorManager mTaskExecutorManager;

  @Before
  public void before() {
    mWorkerId = 0;
    mJobMasterClient = Mockito.mock(JobMasterClient.class);
    mTaskExecutorManager = PowerMockito.mock(TaskExecutorManager.class);
    WorkerNetAddress workerNetAddress = PowerMockito.mock(WorkerNetAddress.class);
    mCommandHandlingExecutor =
        new CommandHandlingExecutor(mTaskExecutorManager, mJobMasterClient, workerNetAddress);
  }

  @Test
  public void heartbeat() throws Exception {
    JobCommand command = new JobCommand();
    RunTaskCommand runTaskCommand = new RunTaskCommand();
    long jobId = 1;
    runTaskCommand.setJobId(jobId);
    int taskId = 2;
    runTaskCommand.setTaskId(taskId);
    JobConfig jobConfig = new TestJobConfig("/test");
    runTaskCommand.setJobConfig(SerializationUtils.serialize(jobConfig));
    Serializable taskArgs = Lists.newArrayList(1);
    runTaskCommand.setTaskArgs(SerializationUtils.serialize(taskArgs));

    command.setRunTaskCommand(runTaskCommand);
    Mockito.when(mJobMasterClient.heartbeat(mWorkerId, Lists.<TaskInfo>newArrayList()))
        .thenReturn(Lists.newArrayList(command));

    mCommandHandlingExecutor.heartbeat();
    ExecutorService executorService =
        Whitebox.getInternalState(mCommandHandlingExecutor, "mCommandHandlingService");
    executorService.shutdown();
    Assert.assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));

    Mockito.verify(mTaskExecutorManager).getAndClearTaskUpdates();
    Mockito.verify(mTaskExecutorManager).executeTask(Mockito.eq(jobId), Mockito.eq(taskId),
        Mockito.eq(jobConfig), Mockito.eq(taskArgs), Mockito.any(JobWorkerContext.class));
  }

}
