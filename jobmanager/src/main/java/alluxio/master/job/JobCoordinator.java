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
  private Map<Integer, Long> mTaskIdToWorkerId;

  private JobCoordinator(CommandManager commandManager, List<WorkerInfo> workerInfoList,
      JobInfo jobInfo) {
    mJobInfo = Preconditions.checkNotNull(jobInfo);
    mCommandManager = Preconditions.checkNotNull(commandManager);
    mWorkersInfoList = workerInfoList;
    mTaskIdToWorkerId = Maps.newHashMap();
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
    JobDefinition<JobConfig, ?> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());

    JobMasterContext context = new JobMasterContext();
    Map<WorkerInfo, ?> taskAddressToArgs;
    try {
      taskAddressToArgs =
          definition.selectExecutors(mJobInfo.getJobConfig(), mWorkersInfoList, context);
    } catch (Exception e) {
      LOG.warn("select executor failed", e);
      mJobInfo.setErrorMessage(e.getMessage());
      return;
    }
    if (taskAddressToArgs.isEmpty()) {
      LOG.warn("No executor is selected");
    }

    for (Entry<WorkerInfo, ?> entry : taskAddressToArgs.entrySet()) {
      LOG.info("selected executor " + entry.getKey() + " with parameters " + entry.getValue());
      int taskId = mTaskIdToWorkerId.size();
      // create task
      mJobInfo.addTask(taskId);
      // submit commands
      mCommandManager.submitRunTaskCommand(mJobInfo.getId(), taskId, mJobInfo.getJobConfig(),
          entry.getValue(), entry.getKey().getId());
      mTaskIdToWorkerId.put(taskId, entry.getKey().getId());
    }
  }

  /**
   * Cancels the current job.
   */
  public void cancel() {
    for (int taskId : mJobInfo.getTaskIdList()) {
      mCommandManager.submitCancelTaskCommand(mJobInfo.getId(), taskId,
          mTaskIdToWorkerId.get(taskId));
    }
  }
}
