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
import alluxio.collections.IndexedSet;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.NoWorkerException;
import alluxio.job.JobConfig;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.wire.TaskInfo;
import alluxio.master.AbstractMaster;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.meta.JobIdGenerator;
import alluxio.master.job.meta.JobInfo;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.thrift.JobMasterWorkerService;
import alluxio.thrift.JobCommand;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The master that handles all job managing operations.
 */
@ThreadSafe
public final class JobMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  // Worker metadata management.
  private final IndexedSet.FieldIndex<MasterWorkerInfo> mIdIndex =
      new IndexedSet.FieldIndex<MasterWorkerInfo>() {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getId();
        }
      };

  private final IndexedSet.FieldIndex<MasterWorkerInfo> mAddressIndex =
      new IndexedSet.FieldIndex<MasterWorkerInfo>() {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getWorkerAddress();
        }
      };

  /**
   * All worker information. Access must be synchronized on mWorkers. If both block and worker
   * metadata must be locked, mBlocks must be locked first.
   */
  @GuardedBy("itself")
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<MasterWorkerInfo>(mIdIndex, mAddressIndex);

  /** The next worker id to use. This state must be journaled. */
  private final AtomicLong mNextWorkerId = new AtomicLong(1);

  /** Manage all the jobs' status. */
  private final JobIdGenerator mJobIdGenerator;
  private final CommandManager mCommandManager;
  private final Map<Long, JobCoordinator> mIdToJobCoordinator;
  private final Map<Long, JobInfo> mIdToJobInfo;

  /**
   * Creates a new instance of {@link JobMaster}.
   *
   * @param journal the journal to use for tracking master operations
   */
  public JobMaster(Journal journal) {
    super(journal, 2);
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
        JobCoordinator.create(mCommandManager, getWorkerInfoList(), jobInfo);
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
   * Returns a worker id for the given worker.
   *
   * @param workerNetAddress the worker {@link WorkerNetAddress}
   * @return the worker id for this worker
   */
  public long getWorkerId(WorkerNetAddress workerNetAddress) {
    // TODO(gene): This NetAddress cloned in case thrift re-uses the object. Does thrift re-use it?
    synchronized (mWorkers) {
      if (mWorkers.contains(mAddressIndex, workerNetAddress)) {
        // This worker address is already mapped to a worker id.
        long oldWorkerId = mWorkers.getFirstByField(mAddressIndex, workerNetAddress).getId();
        LOG.warn("The worker {} already exists as id {}.", workerNetAddress, oldWorkerId);
        return oldWorkerId;
      }

      // Generate a new worker id.
      long workerId = mNextWorkerId.getAndIncrement();
      mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress));

      LOG.info("getWorkerId(): WorkerNetAddress: {} id: {}", workerNetAddress, workerId);
      return workerId;
    }
  }

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  public List<WorkerInfo> getWorkerInfoList() {
    synchronized (mWorkers) {
      List<WorkerInfo> workerInfoList = new ArrayList<WorkerInfo>(mWorkers.size());
      for (MasterWorkerInfo masterWorkerInfo : mWorkers) {
        workerInfoList.add(masterWorkerInfo.generateClientWorkerInfo());
      }
      return workerInfoList;
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

  /**
   * Updates metadata when a worker registers with the master.
   *
   * @param workerId the worker id of the worker registering
   * @throws NoWorkerException if workerId cannot be found
   */
  public void workerRegister(long workerId) throws NoWorkerException {
    synchronized (mWorkers) {
      if (!mWorkers.contains(mIdIndex, workerId)) {
        throw new NoWorkerException("Could not find worker id: " + workerId + " to register.");
      }
      MasterWorkerInfo workerInfo = mWorkers.getFirstByField(mIdIndex, workerId);
      workerInfo.updateLastUpdatedTimeMs();

      LOG.info("registerWorker(): {}", workerInfo);
    }
  }
}
