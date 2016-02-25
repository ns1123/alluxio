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

package alluxio.master.jobmanager;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import alluxio.EnterpriseConstants;
import alluxio.exception.EnterpriseExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.jobmanager.job.JobConfig;
import alluxio.master.AbstractMaster;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.jobmanager.command.CommandManager;
import alluxio.master.jobmanager.job.JobCoordinator;
import alluxio.master.jobmanager.job.JobInfo;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.thrift.JobManagerMasterWorkerService;
import alluxio.thrift.JobManangerCommand;
import alluxio.thrift.TaskInfo;
import alluxio.util.io.PathUtils;
import jersey.repackaged.com.google.common.collect.Lists;

/**
 * The master that handles all job managing operations.
 */
@ThreadSafe
public final class JobManagerMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final FileSystemMaster mFileSystemMaster;
  private final BlockMaster mBlockMaster;
  /** Manage all the jobs' status */
  private final JobIdGenerator mJobIdGenerator;
  private final CommandManager mCommandManager;
  private final Map<Long, JobCoordinator> mIdToJobCoordinator;
  private final Map<Long, JobInfo> mIdToJobInfo;

  /**
   * Constructs the master.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} in Alluxio
   * @param blockMaster the {@link BlockMaster} in Alluxio
   * @param journal the journal to use for tracking master operations
   */
  public JobManagerMaster(FileSystemMaster fileSystemMaster, BlockMaster blockMaster,
      Journal journal) {
    super(journal, 2);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mBlockMaster = Preconditions.checkNotNull(blockMaster);
    mJobIdGenerator = new JobIdGenerator();
    mIdToJobInfo = Maps.newHashMap();
    mIdToJobCoordinator = Maps.newHashMap();
    mCommandManager = CommandManager.ISNTANCE;
  }

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, EnterpriseConstants.JOB_MANAGER_MASTER_NAME);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = Maps.newHashMap();
    services.put(EnterpriseConstants.JOB_MANAGER_MASTER_WORKER_SERVICE_NAME,
        new JobManagerMasterWorkerService.Processor<>(
            new JobManagerMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return EnterpriseConstants.JOB_MANAGER_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // do nothing
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    // do nothing
  }

  /**
   * Runs a job with the given configuration.
   *
   * @param jobConfig the job configuration.
   * @return the job id tracking the progress
   * @throws JobDoesNotExistException when the job doesn't exist
   */
  public long runJob(JobConfig jobConfig) throws JobDoesNotExistException {
    long jobId = mJobIdGenerator.getNewJobId();
    JobInfo jobInfo = new JobInfo(jobId, jobConfig.getName(), jobConfig);
    synchronized (mIdToJobInfo) {
      mIdToJobInfo.put(jobId, jobInfo);
    }
    JobCoordinator jobCoordinator = JobCoordinator.create(jobInfo, mFileSystemMaster, mBlockMaster);
    synchronized (mIdToJobCoordinator) {
      mIdToJobCoordinator.put(jobId, jobCoordinator);
    }
    return jobId;
  }

  /**
   * Cancels a job.
   *
   * @param jobId the id of the job
   * @throws JobDoesNotExistException when the job does not exist
   */
  public void cancelJob(long jobId) throws JobDoesNotExistException {
    synchronized (mIdToJobCoordinator) {
      if (!mIdToJobCoordinator.containsKey(jobId)) {
        throw new JobDoesNotExistException(
            EnterpriseExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(jobId));
      }
      JobCoordinator jobCoordinator = mIdToJobCoordinator.get(jobId);
      jobCoordinator.cancel();
    }
  }

  /**
   * @return list all the job ids.
   */
  public List<Long> listJobs() {
    synchronized (mIdToJobInfo) {
      return Lists.newArrayList(mIdToJobInfo.keySet());
    }
  }

  /**
   * Gets information of the given job id.
   *
   * @param jobId the id of the job
   * @return the job information
   */
  public JobInfo getJobInfo(long jobId) {
    return mIdToJobInfo.get(jobId);
  }

  /**
   * Updates the tasks' status when a worker periodically heartbeats with the master, and sends the
   * commands for the worker to execute.
   *
   * @param workerId the worker id
   * @param taskInfoList the list of the task information
   * @return the list of {@link JobManangerCommand} to the worker
   */
  public synchronized List<JobManangerCommand> workerHeartbeat(long workerId,
      List<TaskInfo> taskInfoList) {
    // update the job info
    for (TaskInfo taskInfo : taskInfoList) {
      JobInfo jobInfo = mIdToJobInfo.get(taskInfo.getJobId());
      jobInfo.setTaskInfo(taskInfo.getTaskId(), taskInfo);
    }
    List<JobManangerCommand> comands = mCommandManager.pollAllPendingCommands(workerId);
    return comands;
  }
}
