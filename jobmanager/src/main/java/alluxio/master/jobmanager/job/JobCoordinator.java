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

package alluxio.master.jobmanager.job;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import alluxio.jobmanager.exception.JobDoesNotExistException;
import alluxio.jobmanager.job.JobConfig;
import alluxio.jobmanager.job.JobDefinition;
import alluxio.jobmanager.job.JobDefinitionRegistry;
import alluxio.jobmanager.job.JobMasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.jobmanager.command.CommandManager;
import alluxio.wire.WorkerInfo;

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
  private Map<Integer, Long> mTaskIdToWorkerId;

  private JobCoordinator(JobInfo jobInfo, FileSystemMaster fileSystemMaster,
      BlockMaster blockMaster) {
    mJobInfo = Preconditions.checkNotNull(jobInfo);
    mCommandManager = CommandManager.ISNTANCE;
    mBlockMaster = Preconditions.checkNotNull(blockMaster);
    mTaskIdToWorkerId = Maps.newHashMap();
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
  }

  /**
   * Creates a new instance of the {@link JobCoordinator}.
   *
   * @param jobInfo the job information
   * @param fileSystemMaster the {@link FileSystemMaster} in Alluxio
   * @param blockMaster the {@link BlockMaster} in Alluxio
   * @return the created coordinator
   * @throws JobDoesNotExistException when the job definition doesn't exist
   */
  public static JobCoordinator create(JobInfo jobInfo, FileSystemMaster fileSystemMaster,
      BlockMaster blockMaster) throws JobDoesNotExistException {
    JobCoordinator jobCoordinator = new JobCoordinator(jobInfo, fileSystemMaster, blockMaster);
    jobCoordinator.start();
    // start the coordinator, create the tasks
    return jobCoordinator;
  }

  private synchronized void start() throws JobDoesNotExistException {
    // get the job definition
    JobDefinition<JobConfig, ?> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());
    List<WorkerInfo> workerInfoList = mBlockMaster.getWorkerInfoList();

    JobMasterContext context = new JobMasterContext(mFileSystemMaster, mBlockMaster);
    Map<WorkerInfo, ?> taskAddressToArgs;
    try {
      taskAddressToArgs =
          definition.selectExecutors(mJobInfo.getJobConfig(), workerInfoList, context);
    } catch (Exception e) {
      LOG.warn("select executor failed with " + e);
      mJobInfo.setErrorMessage(e.getMessage());
      return;
    }
    if (taskAddressToArgs.isEmpty()) {
      LOG.warn("No executor is selected");
    }

    for (Entry<WorkerInfo, ?> entry : taskAddressToArgs.entrySet()) {
      LOG.info("selectd executor " + entry.getKey() + " with parameters " + entry.getValue());
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
