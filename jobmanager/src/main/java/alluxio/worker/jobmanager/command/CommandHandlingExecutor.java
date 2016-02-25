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

package alluxio.worker.jobmanager.command;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.jobmanager.job.JobConfig;
import alluxio.jobmanager.job.JobWorkerContext;
import alluxio.jobmanager.util.SerializationUtils;
import alluxio.thrift.CancelTaskCommand;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.RunTaskCommand;
import alluxio.thrift.TaskInfo;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.WorkerIdRegistry;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.jobmanager.JobManagerMasterClient;
import alluxio.worker.jobmanager.task.TaskExecutorManager;

/**
 * Manages the communication with the master. Dispatches the recevied command from master to
 * handers, and send the status of all the tasks to master.
 */
@NotThreadSafe
public class CommandHandlingExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int DEFAULT_COMMAND_HANDLING_POOL_SIZE = 4;

  private final JobManagerMasterClient mMasterClient;
  private final BlockWorker mBlockWorker;

  private final ExecutorService mCommandHandlingService =
      Executors.newFixedThreadPool(DEFAULT_COMMAND_HANDLING_POOL_SIZE,
          ThreadFactoryUtils.build("command-handling-service-%d", true));

  /**
   * Creates a new instance of {@link CommandHandlingExecutor}.
   *
   * @param masterClient the {@link JobManagerMasterClient}
   * @param blockWorker the {@link BlockWorker} in Alluxio
   */
  public CommandHandlingExecutor(JobManagerMasterClient masterClient, BlockWorker blockWorker) {
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mMasterClient = Preconditions.checkNotNull(masterClient);
  }

  @Override
  public void heartbeat() {
    List<TaskInfo> taskStatusList = TaskExecutorManager.INSTANCE.getTaskInfoList();

    List<JobManangerCommand> commands = null;
    try {
      commands = mMasterClient.heartbeat(WorkerIdRegistry.getWorkerId(), taskStatusList);
    } catch (AlluxioException | IOException e) {
      // TODO(yupeng) better error handling
      LOG.error("Failed to heartbeat", e);
      return;
    }

    for (JobManangerCommand command : commands) {
      mCommandHandlingService.execute(new CommandHandler(command));
    }
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  /**
   * A handler that handles a command sent from the master.
   */
  class CommandHandler implements Runnable {
    private final TaskExecutorManager mTaskExecutorManager;
    private final JobManangerCommand mCommand;

    CommandHandler(JobManangerCommand command) {
      mTaskExecutorManager = TaskExecutorManager.INSTANCE;
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
          Object taskArgs = SerializationUtils.deserialize(command.getTaskArgs());
          JobWorkerContext context = new JobWorkerContext(mBlockWorker);
          LOG.info("Received run task command " + taskId + " for worker "
              + WorkerIdRegistry.getWorkerId());
          mTaskExecutorManager.executeTask(jobId, taskId, jobConfig, taskArgs, context);
        } catch (ClassNotFoundException | IOException e) {
          // TODO(yupeng) better error handling
          LOG.error("Failed to deserialize ", e);
          return;
        }
      } else if (mCommand.isSetCancelTaskCommand()) {
        CancelTaskCommand command = mCommand.getCancelTaskCommand();
        long jobId = command.getJobId();
        int taskId = command.getTaskId();
        mTaskExecutorManager.cancelTask(jobId, taskId);
      } else {
        throw new RuntimeException("unsupported command type:" + mCommand.toString());
      }
    }
  }
}
