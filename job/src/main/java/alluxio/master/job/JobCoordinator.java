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
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.meta.JobInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job coordinator that coordinates the distributed task execution on the worker nodes.
 */
@ThreadSafe
public final class JobCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
  private final JobInfo mJobInfo;
  private final CommandManager mCommandManager;
  private final List<WorkerInfo> mWorkersInfoList;
  private Map<Integer, WorkerInfo> mTaskIdToWorkerInfo;

  private JobCoordinator(CommandManager commandManager, List<WorkerInfo> workerInfoList,
      JobInfo jobInfo) {
    mJobInfo = Preconditions.checkNotNull(jobInfo);
    mCommandManager = Preconditions.checkNotNull(commandManager);
    mWorkersInfoList = workerInfoList;
    mTaskIdToWorkerInfo = Maps.newHashMap();
  }

  /**
   * Creates a new instance of the {@link JobCoordinator}.
   *
   * @param commandManager the command manager
   * @param workerInfoList the list of workers to use
   * @param jobInfo the job information
   * @return the created coordinator
   * @throws JobDoesNotExistException when the job definition doesn't exist
   */
  public static JobCoordinator create(CommandManager commandManager,
      List<WorkerInfo> workerInfoList, JobInfo jobInfo) throws JobDoesNotExistException {
    JobCoordinator jobCoordinator = new JobCoordinator(commandManager, workerInfoList, jobInfo);
    jobCoordinator.start();
    // start the coordinator, create the tasks
    return jobCoordinator;
  }

  private synchronized void start() throws JobDoesNotExistException {
    // get the job definition
    JobDefinition<JobConfig, ?, ?> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());

    JobMasterContext context = new JobMasterContext(mJobInfo.getId());
    Map<WorkerInfo, ?> taskAddressToArgs;
    try {
      taskAddressToArgs = definition
          .selectExecutors(mJobInfo.getJobConfig(), mWorkersInfoList, context);
    } catch (Exception e) {
      LOG.warn("select executor failed", e);
      mJobInfo.setErrorMessage(e.getMessage());
      return;
    }
    if (taskAddressToArgs.isEmpty()) {
      LOG.info("No executor is selected");
      updateStatus();
    }

    for (Entry<WorkerInfo, ?> entry : taskAddressToArgs.entrySet()) {
      LOG.info("selected executor " + entry.getKey() + " with parameters " + entry.getValue());
      int taskId = mTaskIdToWorkerInfo.size();
      // create task
      mJobInfo.addTask(taskId);
      // submit commands
      mCommandManager.submitRunTaskCommand(mJobInfo.getId(), taskId, mJobInfo.getJobConfig(),
          entry.getValue(), entry.getKey().getId());
      mTaskIdToWorkerInfo.put(taskId, entry.getKey());
    }
  }

  /**
   * Cancels the current job.
   */
  public void cancel() {
    for (int taskId : mJobInfo.getTaskIdList()) {
      mCommandManager.submitCancelTaskCommand(mJobInfo.getId(), taskId,
          mTaskIdToWorkerInfo.get(taskId).getId());
    }
  }

  /**
   * Updates the status of the job. When all the tasks are completed, run the join method in the
   * definition.
   */
  public void updateStatus() {
    int completed = 0;
    synchronized (mJobInfo) {
      List<TaskInfo> taskInfoList = mJobInfo.getTaskInfoList();
      for (TaskInfo info : taskInfoList) {
        switch (info.getStatus()) {
          case FAILED:
            mJobInfo.setStatus(Status.FAILED);
            if (mJobInfo.getErrorMessage().isEmpty()) {
              mJobInfo.setErrorMessage("The task execution failed");
            }
            return;
          case CANCELED:
            if (mJobInfo.getStatus() != Status.FAILED) {
              mJobInfo.setStatus(Status.CANCELED);
            }
            break;
          case RUNNING:
            if (mJobInfo.getStatus() != Status.FAILED && mJobInfo.getStatus() != Status.CANCELED) {
              mJobInfo.setStatus(Status.RUNNING);
            }
            break;
          case COMPLETED:
            completed++;
            break;
          case CREATED:
            // do nothing
            break;
          default:
            throw new IllegalArgumentException("Unsupported status " + info.getStatus());
        }
      }
      if (completed == taskInfoList.size()) {
        // all the tasks completed, run join
        try {
          mJobInfo.setResult(join(taskInfoList));
        } catch (Exception e) {
          mJobInfo.setStatus(Status.FAILED);
          mJobInfo.setErrorMessage(e.getMessage());
          return;
        }
        mJobInfo.setStatus(Status.COMPLETED);
      }
    }
  }

  /**
   * Joins the task results and produces a final result.
   *
   * @param taskInfoList the list of task information
   * @return the aggregated result as a String
   * @throws Exception if any error occurs
   */
  private String join(List<TaskInfo> taskInfoList) throws Exception {
    // get the job definition
    JobDefinition<JobConfig, ?, Object> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());
    Map<WorkerInfo, Object> taskResults = Maps.newHashMap();
    for (TaskInfo taskInfo : taskInfoList) {
      Object taskResult = SerializationUtils.deserialize(taskInfo.getResult());
      taskResults.put(mTaskIdToWorkerInfo.get(taskInfo.getTaskId()), taskResult);
    }
    return definition.join(mJobInfo.getJobConfig(), taskResults);
  }
}
