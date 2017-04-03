/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker.job.command;

import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.job.JobConfig;
import alluxio.job.JobWorkerContext;
import alluxio.worker.job.RetryHandlingJobMasterClient;
import alluxio.job.util.SerializationUtils;
import alluxio.thrift.CancelTaskCommand;
import alluxio.thrift.JobCommand;
import alluxio.thrift.RunTaskCommand;
import alluxio.thrift.TaskInfo;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.JobWorkerIdRegistry;
import alluxio.worker.job.task.TaskExecutorManager;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Manages the communication with the master. Dispatches the received command from master to
 * handlers, and sends the status of all the tasks to master.
 */
@NotThreadSafe
public class CommandHandlingExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CommandHandlingExecutor.class);
  private static final int DEFAULT_COMMAND_HANDLING_POOL_SIZE = 4;

  private final RetryHandlingJobMasterClient mMasterClient;
  private final TaskExecutorManager mTaskExecutorManager;
  private final WorkerNetAddress mWorkerNetAddress;

  private final ExecutorService mCommandHandlingService =
      Executors.newFixedThreadPool(DEFAULT_COMMAND_HANDLING_POOL_SIZE,
          ThreadFactoryUtils.build("command-handling-service-%d", true));

  /**
   * Creates a new instance of {@link CommandHandlingExecutor}.
   *
   * @param taskExecutorManager the {@link TaskExecutorManager}
   * @param masterClient the {@link RetryHandlingJobMasterClient}
   * @param workerNetAddress the connection info for this worker
   */
  public CommandHandlingExecutor(TaskExecutorManager taskExecutorManager,
      RetryHandlingJobMasterClient masterClient, WorkerNetAddress workerNetAddress) {
    mTaskExecutorManager = Preconditions.checkNotNull(taskExecutorManager, "taskExecutorManager");
    mMasterClient = Preconditions.checkNotNull(masterClient, "masterClient");
    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress, "workerNetAddress");
  }

  @Override
  public void heartbeat() {
    List<TaskInfo> taskStatusList = mTaskExecutorManager.getAndClearTaskUpdates();

    List<JobCommand> commands;
    try {
      commands = mMasterClient.heartbeat(JobWorkerIdRegistry.getWorkerId(), taskStatusList);
    } catch (AlluxioException | IOException e) {
      // Restore the task updates so that they can be accessed in the next heartbeat.
      mTaskExecutorManager.restoreTaskUpdates(taskStatusList);
      // TODO(yupeng) better error handling
      LOG.error("Failed to heartbeat", e);
      return;
    }

    for (JobCommand command : commands) {
      mCommandHandlingService.execute(new CommandHandler(command));
    }
  }

  @Override
  public void close() {}

  /**
   * A handler that handles a command sent from the master.
   */
  class CommandHandler implements Runnable {
    private final JobCommand mCommand;

    CommandHandler(JobCommand command) {
      mCommand = command;
    }

    @Override
    public void run() {
      if (mCommand.isSetRunTaskCommand()) {
        RunTaskCommand command = mCommand.getRunTaskCommand();
        long jobId = command.getJobId();
        int taskId = command.getTaskId();
        JobConfig jobConfig;
        try {
          jobConfig = (JobConfig) SerializationUtils.deserialize(command.getJobConfig());
          Serializable taskArgs = SerializationUtils.deserialize(command.getTaskArgs());
          JobWorkerContext context = new JobWorkerContext(jobId, taskId);
          LOG.info("Received run task " + taskId + " for job " + jobId + " on worker "
              + JobWorkerIdRegistry.getWorkerId());
          mTaskExecutorManager.executeTask(jobId, taskId, jobConfig, taskArgs, context);
        } catch (ClassNotFoundException | IOException e) {
          // TODO(yupeng) better error handling
          LOG.error("Failed to deserialize ", e);
        }
      } else if (mCommand.isSetCancelTaskCommand()) {
        CancelTaskCommand command = mCommand.getCancelTaskCommand();
        long jobId = command.getJobId();
        int taskId = command.getTaskId();
        mTaskExecutorManager.cancelTask(jobId, taskId);
      } else if (mCommand.isSetRegisterCommand()) {
        try {
          JobWorkerIdRegistry.registerWorker(mMasterClient, mWorkerNetAddress);
        } catch (ConnectionFailedException | IOException e) {
          throw Throwables.propagate(e);
        }
      } else {
        throw new RuntimeException("unsupported command type:" + mCommand.toString());
      }
    }
  }
}
