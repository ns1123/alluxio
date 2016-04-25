/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker.job.task;

import alluxio.Constants;
import alluxio.job.JobConfig;
import alluxio.job.JobDefinition;
import alluxio.job.JobDefinitionRegistry;
import alluxio.job.JobWorkerContext;
import alluxio.job.exception.JobDoesNotExistException;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A thread that runs the task.
 */
@NotThreadSafe
public final class TaskExecutor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final long mJobId;
  private final int mTaskId;
  private final JobConfig mJobConfig;
  private final Object mTaskArgs;
  private final JobWorkerContext mContext;
  private final TaskExecutorManager mTaskExecutorManager;

  /**
   * Creates a new instance of {@link TaskExecutor}.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param jobConfig the job configuration
   * @param taskArgs the arguments passed to the task
   * @param context the context on the worker
   * @param taskExecutorManager the task executor manager
   */
  public TaskExecutor(long jobId, int taskId, JobConfig jobConfig, Object taskArgs,
      JobWorkerContext context, TaskExecutorManager taskExecutorManager) {
    mJobId = jobId;
    mTaskId = taskId;
    mJobConfig = jobConfig;
    mTaskArgs = taskArgs;
    mContext = Preconditions.checkNotNull(context);
    mTaskExecutorManager = Preconditions.checkNotNull(taskExecutorManager);
  }

  @Override
  public void run() {
    // TODO(yupeng) set other logger
    JobDefinition<JobConfig, Object, Object> definition;
    try {
      definition = JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobConfig);
    } catch (JobDoesNotExistException e1) {
      LOG.error("The job definition doesn't exist for config " + mJobConfig.getName());
      return;
    }
    Object result = null;
    try {
      result = definition.runTask(mJobConfig, mTaskArgs, mContext);
    } catch (InterruptedException e) {
      mTaskExecutorManager.notifyTaskCancellation(mJobId, mTaskId);
      return;
    } catch (Exception e) {
      mTaskExecutorManager.notifyTaskFailure(mJobId, mTaskId, ExceptionUtils.getStackTrace(e));
      LOG.warn("Exception running task for job {}({})", mJobConfig.getName(), mTaskArgs.toString(),
          e);
      return;
    }
    mTaskExecutorManager.notifyTaskCompletion(mJobId, mTaskId, result);
  }
}
