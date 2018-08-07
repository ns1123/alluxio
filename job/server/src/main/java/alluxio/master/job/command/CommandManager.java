/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job.command;

import alluxio.job.JobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.thrift.CancelTaskCommand;
import alluxio.thrift.JobCommand;
import alluxio.thrift.RunTaskCommand;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A command manager that manages the commands to issue to the workers.
 */
@ThreadSafe
public final class CommandManager {

  private static final Logger LOG = LoggerFactory.getLogger(CommandManager.class);

  // TODO(yupeng) add retry support
  private final Map<Long, List<JobCommand>> mWorkerIdToPendingCommands = Maps.newHashMap();

  /**
   * Constructs a new {@link CommandManager}.
   */
  public CommandManager() {}

  /**
   * Submits a run-task command to a specified worker.
   *
   * @param jobId the id of the job
   * @param taskId the id of the task
   * @param jobConfig the job configuration
   * @param taskArgs the arguments passed to the executor on the worker
   * @param workerId the id of the worker
   */
  public synchronized void submitRunTaskCommand(long jobId, int taskId, JobConfig jobConfig,
      Object taskArgs, long workerId) {
    RunTaskCommand runTaskCommand = new RunTaskCommand();
    runTaskCommand.setJobId(jobId);
    runTaskCommand.setTaskId(taskId);
    try {
      runTaskCommand.setJobConfig(SerializationUtils.serialize(jobConfig));
      runTaskCommand.setTaskArgs(SerializationUtils.serialize(taskArgs));
    } catch (IOException e) {
      // TODO(yupeng) better exception handling
      LOG.info("Failed to serialize the run task command:" + e);
      return;
    }
    JobCommand command = new JobCommand();
    command.setRunTaskCommand(runTaskCommand);
    if (!mWorkerIdToPendingCommands.containsKey(workerId)) {
      mWorkerIdToPendingCommands.put(workerId, Lists.<JobCommand>newArrayList());
    }
    mWorkerIdToPendingCommands.get(workerId).add(command);
  }

  /**
   * Submits a cancel-task command to a specified worker.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param workerId the worker id
   */
  public synchronized void submitCancelTaskCommand(long jobId, int taskId, long workerId) {
    CancelTaskCommand cancelTaskCommand = new CancelTaskCommand();
    cancelTaskCommand.setJobId(jobId);
    cancelTaskCommand.setTaskId(taskId);
    JobCommand command = new JobCommand();
    command.setCancelTaskCommand(cancelTaskCommand);
    if (!mWorkerIdToPendingCommands.containsKey(workerId)) {
      mWorkerIdToPendingCommands.put(workerId, Lists.<JobCommand>newArrayList());
    }
    mWorkerIdToPendingCommands.get(workerId).add(command);
  }

  /**
   * Polls all the pending commands to a worker and removes the commands from the queue.
   *
   * @param workerId id of the worker to send the commands to
   * @return the list of the commends polled
   */
  public synchronized List<JobCommand> pollAllPendingCommands(long workerId) {
    if (!mWorkerIdToPendingCommands.containsKey(workerId)) {
      return Lists.newArrayList();
    }
    List<JobCommand> commands =
        Lists.newArrayList(mWorkerIdToPendingCommands.get(workerId));
    mWorkerIdToPendingCommands.get(workerId).clear();
    return commands;
  }
}