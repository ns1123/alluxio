/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.job.JobConfig;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.wire.TaskInfo;
import alluxio.master.AbstractMaster;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.meta.JobIdGenerator;
import alluxio.master.job.meta.JobInfo;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.thrift.JobMasterWorkerService;
import alluxio.thrift.JobCommand;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import jersey.repackaged.com.google.common.collect.Lists;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The master that handles all job managing operations.
 */
@ThreadSafe
public final class JobMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  private final FileSystemMaster mFileSystemMaster;
  private final BlockMaster mBlockMaster;
  /** Manage all the jobs' status. */
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
  public JobMaster(FileSystemMaster fileSystemMaster, BlockMaster blockMaster,
      Journal journal) {
    super(journal, 2);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mBlockMaster = Preconditions.checkNotNull(blockMaster);
    mJobIdGenerator = new JobIdGenerator();
    mIdToJobInfo = Maps.newHashMap();
    mIdToJobCoordinator = Maps.newHashMap();
    mCommandManager = new CommandManager();
  }

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.JOB_MASTER_NAME);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = Maps.newHashMap();
    services.put(Constants.JOB_MASTER_WORKER_SERVICE_NAME,
        new JobMasterWorkerService.Processor<>(new JobMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.JOB_MASTER_NAME;
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
   * @param jobConfig the job configuration
   * @return the job id tracking the progress
   * @throws JobDoesNotExistException when the job doesn't exist
   */
  public long runJob(JobConfig jobConfig) throws JobDoesNotExistException {
    long jobId = mJobIdGenerator.getNewJobId();
    JobInfo jobInfo = new JobInfo(jobId, jobConfig.getName(), jobConfig);
    synchronized (mIdToJobInfo) {
      mIdToJobInfo.put(jobId, jobInfo);
    }
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, jobInfo, mFileSystemMaster, mBlockMaster);
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
        throw new JobDoesNotExistException(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(jobId));
      }
      JobCoordinator jobCoordinator = mIdToJobCoordinator.get(jobId);
      jobCoordinator.cancel();
    }
  }

  /**
   * @return list all the job ids
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
   * @throws JobDoesNotExistException if the job does not exist
   */
  public JobInfo getJobInfo(long jobId) throws JobDoesNotExistException {
    synchronized (mIdToJobInfo) {
      if (!mIdToJobInfo.containsKey(jobId)) {
        throw new JobDoesNotExistException(jobId);
      }
      return mIdToJobInfo.get(jobId);
    }
  }

  /**
   * Updates the tasks' status when a worker periodically heartbeats with the master, and sends the
   * commands for the worker to execute.
   *
   * @param workerId the worker id
   * @param taskInfoList the list of the task information
   * @return the list of {@link JobCommand} to the worker
   */
  public synchronized List<JobCommand> workerHeartbeat(long workerId,
      List<TaskInfo> taskInfoList) {
    // update the job info
    List<Long> updatedJobIds = new ArrayList<>();
    for (TaskInfo taskInfo : taskInfoList) {
      JobInfo jobInfo = mIdToJobInfo.get(taskInfo.getJobId());
      jobInfo.setTaskInfo(taskInfo.getTaskId(), taskInfo);
      updatedJobIds.add(taskInfo.getJobId());
    }
    for (long updatedJobId : updatedJobIds) {
      // update the job status
      JobCoordinator coordinator = mIdToJobCoordinator.get(updatedJobId);
      coordinator.updateStatus();
    }

    List<JobCommand> comands = mCommandManager.pollAllPendingCommands(workerId);
    return comands;
  }
}
