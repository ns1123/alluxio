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
import alluxio.collections.Pair;
import alluxio.job.JobConfig;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.SerializationUtils;
import alluxio.thrift.Status;
import alluxio.thrift.TaskInfo;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Manages the task executors.
 */
@ThreadSafe
public class TaskExecutorManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int DEFAULT_TASK_EXECUTOR_POOL_SIZE = 10;
  private final ExecutorService mTaskExecutionService = Executors.newFixedThreadPool(
      DEFAULT_TASK_EXECUTOR_POOL_SIZE, ThreadFactoryUtils.build("task-execution-service-%d", true));
  /** Map of (JobId, TaskId) to task future. */
  private Map<Pair<Long, Integer>, Future<?>> mIdToFuture;

  /** Map of (JobId, TaskId) to task status. */
  private Map<Pair<Long, Integer>, TaskInfo> mIdToInfo;

  /**
   *  Constructs a new instance of {@link TaskExecutorManager}.
   */
  public TaskExecutorManager() {
    mIdToFuture = Maps.newHashMap();
    mIdToInfo = Maps.newHashMap();
  }

  /**
   * Notifies the completion of the task.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param result the task execution result
   */
  public synchronized void notifyTaskCompletion(long jobId, int taskId, Object result) {
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    TaskInfo taskInfo = mIdToInfo.get(id);
    taskInfo.setStatus(Status.COMPLETED);
    try {
      taskInfo.setResult(SerializationUtils.serialize(result));
    } catch (IOException e) {
      // TODO(yupeng) better error handling
      LOG.error("Failed to serialize " + result, e);
      return;
    }
    mIdToFuture.remove(id);
  }

  /**
   * Notifies the failure of the task.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param errorMessage the error message
   */
  public synchronized void notifyTaskFailure(long jobId, int taskId, String errorMessage) {
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    TaskInfo taskInfo = mIdToInfo.get(id);
    taskInfo.setStatus(Status.FAILED);
    taskInfo.setErrorMessage(errorMessage);
    mIdToFuture.remove(id);
  }

  /**
   * Notifies the cancellation of the task.
   *
   * @param jobId the job id
   * @param taskId the task id
   */
  public synchronized void notifyTaskCancellation(long jobId, int taskId) {
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    TaskInfo taskInfo = mIdToInfo.get(id);
    taskInfo.setStatus(Status.CANCELED);
  }

  /**
   * Executes the given task.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param jobConfig the job configuration
   * @param taskArgs the arguments
   * @param context the context of the worker
   */
  public synchronized void executeTask(long jobId, int taskId, JobConfig jobConfig, Object taskArgs,
      JobWorkerContext context) {
    Future<?> future = mTaskExecutionService
        .submit(new TaskExecutor(jobId, taskId, jobConfig, taskArgs, context, this));
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    mIdToFuture.put(id, future);
    TaskInfo taskInfo = new TaskInfo();
    taskInfo.setJobId(jobId);
    taskInfo.setTaskId(taskId);
    taskInfo.setStatus(Status.RUNNING);
    mIdToInfo.put(id, taskInfo);
  }

  /**
   * Cancels the given task.
   *
   * @param jobId the job id
   * @param taskId the task id
   */
  public synchronized void cancelTask(long jobId, int taskId) {
    Pair<Long, Integer> id = new Pair<Long, Integer>(jobId, taskId);
    TaskInfo taskInfo = mIdToInfo.get(id);
    if (!mIdToFuture.containsKey(id) || taskInfo.getStatus().equals(Status.CANCELED)) {
      // job has finished, or failed, or canceled
      return;
    }

    Future<?> future = mIdToFuture.get(id);
    if (!future.cancel(true)) {
      taskInfo.setStatus(Status.FAILED);
      taskInfo.setErrorMessage("Failed to cancel the task");
    }
  }

  /**
   * @return the list of task information
   */
  public synchronized List<TaskInfo> getTaskInfoList() {
    return Lists.newArrayList(mIdToInfo.values());
  }
}
