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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.jobmanager.job.JobConfig;
import alluxio.jobmanager.job.JobWorkerContext;
import alluxio.thrift.Status;
import alluxio.thrift.TaskInfo;
import alluxio.util.ThreadFactoryUtils;

/**
 * Manages the task executors.
 */
public enum TaskExecutorManager {
  INSTANCE;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int DEFAULT_TASK_EXECUTOR_POOL_SIZE = 10;
  private final ExecutorService mTaskExecutionService = Executors.newFixedThreadPool(
      DEFAULT_TASK_EXECUTOR_POOL_SIZE, ThreadFactoryUtils.build("task-execution-service-%d", true));
  /** Map of (JobId, TaskId) to task future */
  private Map<Pair<Long, Integer>, Future<?>> mIdToFuture;

  /** Map of (JobId, TaskId) to task status */
  private Map<Pair<Long, Integer>, TaskInfo> mIdToInfo;

  private TaskExecutorManager() {
    mIdToFuture = Maps.newHashMap();
    mIdToInfo = Maps.newHashMap();
  }

  public synchronized void notifyTaskSuccess(long jobId, int taskId) {
    LOG.info("notfied success");
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    TaskInfo taskInfo = mIdToInfo.get(id);
    taskInfo.setStatus(Status.SUCCESS);
    mIdToFuture.remove(id);
  }

  public synchronized void notifyTaskFailure(long jobId, int taskId, String errorMessage) {
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    TaskInfo taskInfo = mIdToInfo.get(id);
    taskInfo.setStatus(Status.ERROR);
    taskInfo.setErrorMessage(errorMessage);
    mIdToFuture.remove(id);
  }

  public synchronized void notifyTaskInterruption(long jobId, int taskId) {
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    TaskInfo taskInfo = mIdToInfo.get(id);
    taskInfo.setStatus(Status.CANCELED);
  }

  public synchronized void executeTask(long jobId, int taskId, JobConfig jobConfig,
      Object taskArgs, JobWorkerContext context) {
    Future<?> future =
        mTaskExecutionService.submit(new TaskExecutor(jobId, taskId, jobConfig, taskArgs, context));
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    mIdToFuture.put(id, future);
    TaskInfo taskInfo = new TaskInfo();
    taskInfo.setJobId(jobId);
    taskInfo.setTaskId(taskId);
    taskInfo.setStatus(Status.INPROGRESS);
    mIdToInfo.put(id, taskInfo);
  }

  public synchronized void cancelTask(long jobId, int taskId) {
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    TaskInfo taskInfo = mIdToInfo.get(id);
    if (!mIdToFuture.containsKey(id)|| taskInfo.getStatus().equals(Status.CANCELED)) {
      // job has finished, or failed, or canceled
      return;
    }

    Future<?> future = mIdToFuture.get(id);
    if (!future.cancel(true)) {
      taskInfo.setStatus(Status.ERROR);
      taskInfo.setErrorMessage("Failed to cancel the task");
    }
  }

  public synchronized List<TaskInfo> getTaskInfoList() {
    return Lists.newArrayList(mIdToInfo.values());
  }
}
