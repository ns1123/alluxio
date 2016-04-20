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
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.meta.JobInfo;
import alluxio.thrift.Status;
import alluxio.thrift.TaskInfo;
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
  private final BlockMaster mBlockMaster;
  private final FileSystemMaster mFileSystemMaster;
  private Map<Integer, WorkerInfo> mTaskIdToWorkerInfo;

  private JobCoordinator(CommandManager commandManager, JobInfo jobInfo,
      FileSystemMaster fileSystemMaster, BlockMaster blockMaster) {
    mJobInfo = Preconditions.checkNotNull(jobInfo);
    mCommandManager = Preconditions.checkNotNull(commandManager);
    mBlockMaster = Preconditions.checkNotNull(blockMaster);
    mTaskIdToWorkerInfo = Maps.newHashMap();
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
  }

  /**
   * Creates a new instance of the {@link JobCoordinator}.
   *
   * @param commandManager the command manager
   * @param jobInfo the job information
   * @param fileSystemMaster the {@link FileSystemMaster} in Alluxio
   * @param blockMaster the {@link BlockMaster} in Alluxio
   * @return the created coordinator
   * @throws JobDoesNotExistException when the job definition doesn't exist
   */
  public static JobCoordinator create(CommandManager commandManager, JobInfo jobInfo,
      FileSystemMaster fileSystemMaster, BlockMaster blockMaster) throws JobDoesNotExistException {
    JobCoordinator jobCoordinator =
        new JobCoordinator(commandManager, jobInfo, fileSystemMaster, blockMaster);
    jobCoordinator.start();
    // start the coordinator, create the tasks
    return jobCoordinator;
  }

  private synchronized void start() throws JobDoesNotExistException {
    // get the job definition
    JobDefinition<JobConfig, ?, ?> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());
    List<WorkerInfo> workerInfoList = mBlockMaster.getWorkerInfoList();

    JobMasterContext context =
        new JobMasterContext(mFileSystemMaster, mBlockMaster, mJobInfo.getId());
    Map<WorkerInfo, ?> taskAddressToArgs;
    try {
      taskAddressToArgs =
          definition.selectExecutors(mJobInfo.getJobConfig(), workerInfoList, context);
    } catch (Exception e) {
      LOG.warn("select executor failed", e);
      mJobInfo.setErrorMessage(e.getMessage());
      return;
    }
    if (taskAddressToArgs.isEmpty()) {
      LOG.warn("No executor is selected");
    }

    for (Entry<WorkerInfo, ?> entry : taskAddressToArgs.entrySet()) {
      LOG.info("selectd executor " + entry.getKey() + " with parameters " + entry.getValue());
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
  }

  /**
   * Joins the task results and produces a final result.
   *
   * @param taskInfoList the list of task information
   * @return the aggregated result in string
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
