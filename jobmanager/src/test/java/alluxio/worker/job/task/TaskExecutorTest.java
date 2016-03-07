/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker.job.task;

import alluxio.job.JobConfig;
import alluxio.job.JobDefinition;
import alluxio.job.JobDefinitionRegistry;
import alluxio.job.JobWorkerContext;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Tests {@link TaskExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TaskExecutorManager.class, JobDefinitionRegistry.class, JobWorkerContext.class})
public final class TaskExecutorTest {
  private TaskExecutorManager mTaskExecutorManager;
  private JobDefinitionRegistry mRegistry;

  @Before
  public void before() {
    mTaskExecutorManager = PowerMockito.mock(TaskExecutorManager.class);
    Whitebox.setInternalState(TaskExecutorManager.class, "INSTANCE", mTaskExecutorManager);

    mRegistry = PowerMockito.mock(JobDefinitionRegistry.class);
    Whitebox.setInternalState(JobDefinitionRegistry.class, "INSTANCE", mRegistry);
  }

  @Test
  public void runCompletionTest() throws Exception {
    long jobId = 1;
    int taskId = 2;
    JobConfig jobConfig = Mockito.mock(JobConfig.class);
    Object taskArgs = Lists.newArrayList(1);
    JobWorkerContext context = Mockito.mock(JobWorkerContext.class);
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Object> jobDefinition = Mockito.mock(JobDefinition.class);
    Mockito.when(mRegistry.getJobDefinition(jobConfig)).thenReturn(jobDefinition);

    TaskExecutor executor = new TaskExecutor(jobId, taskId, jobConfig, taskArgs, context);
    executor.run();

    Mockito.verify(jobDefinition).runTask(jobConfig, taskArgs, context);
    Mockito.verify(mTaskExecutorManager).notifyTaskCompletion(jobId, taskId);
  }

  @Test
  public void runFailureTest() throws Exception {
    long jobId = 1;
    int taskId = 2;
    JobConfig jobConfig = Mockito.mock(JobConfig.class);
    Object taskArgs = Lists.newArrayList(1);
    JobWorkerContext context = Mockito.mock(JobWorkerContext.class);
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Object> jobDefinition = Mockito.mock(JobDefinition.class);
    Mockito.when(mRegistry.getJobDefinition(jobConfig)).thenReturn(jobDefinition);
    Mockito.doThrow(new UnsupportedOperationException("failure")).when(jobDefinition)
        .runTask(jobConfig, taskArgs, context);

    TaskExecutor executor = new TaskExecutor(jobId, taskId, jobConfig, taskArgs, context);
    executor.run();

    Mockito.verify(mTaskExecutorManager).notifyTaskFailure(jobId, taskId, "failure");
  }

  @Test
  public void runCancelationTest() throws Exception {
    long jobId = 1;
    int taskId = 2;
    JobConfig jobConfig = Mockito.mock(JobConfig.class);
    Object taskArgs = Lists.newArrayList(1);
    JobWorkerContext context = Mockito.mock(JobWorkerContext.class);
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Object> jobDefinition = Mockito.mock(JobDefinition.class);
    Mockito.when(mRegistry.getJobDefinition(jobConfig)).thenReturn(jobDefinition);
    Mockito.doThrow(new InterruptedException("interupt")).when(jobDefinition).runTask(jobConfig,
        taskArgs, context);

    TaskExecutor executor = new TaskExecutor(jobId, taskId, jobConfig, taskArgs, context);
    executor.run();

    Mockito.verify(mTaskExecutorManager).notifyTaskCancellation(jobId, taskId);
  }
}
