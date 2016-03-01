/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.worker.jobmanager.task;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.jobmanager.exception.JobDoesNotExistException;
import alluxio.jobmanager.job.JobConfig;
import alluxio.jobmanager.job.JobDefinition;
import alluxio.jobmanager.job.JobDefinitionRegistry;
import alluxio.jobmanager.job.JobWorkerContext;

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

  /**
   * Creates a new instance of {@link TaskExecutor}.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param jobConfig the job configuration
   * @param taskArgs the arguments passed to the task
   * @param context the context on the worker
   */
  public TaskExecutor(long jobId, int taskId, JobConfig jobConfig, Object taskArgs,
      JobWorkerContext context) {
    mJobId = jobId;
    mTaskId = taskId;
    mJobConfig = jobConfig;
    mTaskArgs = taskArgs;
    mContext = Preconditions.checkNotNull(context);
  }

  @Override
  public void run() {
    // TODO(yupeng) set other logger
    JobDefinition<JobConfig, Object> definition;
    try {
      definition = JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobConfig);
    } catch (JobDoesNotExistException e1) {
      LOG.error("The job definition doesn't exist for config " + mJobConfig.getName());
      return;
    }
    try {
      definition.runTask(mJobConfig, mTaskArgs, mContext);
    } catch (InterruptedException e) {
      TaskExecutorManager.INSTANCE.notifyTaskCancellation(mJobId, mTaskId);
      return;
    } catch (Exception e) {
      TaskExecutorManager.INSTANCE.notifyTaskFailure(mJobId, mTaskId, e.getMessage());
      return;
    }
    TaskExecutorManager.INSTANCE.notifyTaskCompletion(mJobId, mTaskId);
  }
}
