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

package alluxio.jobmanager.worker.task;

import alluxio.jobmanager.job.JobConfig;
import alluxio.jobmanager.job.JobDefinition;
import alluxio.jobmanager.job.JobDefinitionRegistry;

/**
 * A thread that runs the task.
 */
public class TaskExecutor implements Runnable {
  private final long mJobId;
  private final int mTaskId;
  private final JobConfig mJobConfig;
  private final Object mTaskArgs;

  public TaskExecutor(long jobId, int taskId, JobConfig jobConfig, Object taskArgs) {
    mJobId = jobId;
    mTaskId = taskId;
    mJobConfig = jobConfig;
    mTaskArgs = taskArgs;
  }

  @Override
  public void run() {
    // TODO(yupeng) set other logger
    JobDefinition<JobConfig, Object> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobConfig);
    try {
      definition.runTask(mJobConfig, mTaskArgs);
    } catch (InterruptedException e) {
      TaskExecutorManager.INSTANCE.notifyTaskInterruption(mJobId, mTaskId);
      return;
    } catch (Exception e) {
      TaskExecutorManager.INSTANCE.notifyTaskFailure(mJobId, mTaskId, e.getMessage());
      return;
    }
    TaskExecutorManager.INSTANCE.notifyTaskSuccess(mJobId, mTaskId);
  }
}
