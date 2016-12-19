/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.clock.SystemClock;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.ExceptionMessage;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.job.JobConfig;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.meta.JobIdGenerator;
import alluxio.job.meta.JobInfo;
import alluxio.job.meta.MasterWorkerInfo;
import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.AbstractMaster;
import alluxio.master.job.command.CommandManager;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Job;
import alluxio.proto.journal.Job.FinishJobEntry;
import alluxio.proto.journal.Job.StartJobEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.thrift.JobCommand;
import alluxio.thrift.JobMasterWorkerService;
import alluxio.thrift.RegisterCommand;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
  private final IndexDefinition<MasterWorkerInfo> mIdIndex =
      new IndexDefinition<MasterWorkerInfo>(true) {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getId();
        }
      };

  private final IndexDefinition<MasterWorkerInfo> mAddressIndex =
      new IndexDefinition<MasterWorkerInfo>(true) {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getWorkerAddress();
        }
      };

  private final JournalEntryWriter mJournalEntryWriter =
      new JournalEntryWriter() {
        @Override
        public void writeJournalEntry(JournalEntry entry) {
          JobMaster.this.writeJournalEntry(entry);
          JobMaster.this.flushJournal();
        }
  };

  /**
   * All worker information. Access must be synchronized on mWorkers. If both block and worker
   * metadata must be locked, mBlocks must be locked first.
   */
  // TODO(jiri): Replace MasterWorkerInfo with a simpler data structure.
  @GuardedBy("itself")
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<MasterWorkerInfo>(mIdIndex, mAddressIndex);

  /** The next worker id to use. This state must be journaled. */
  private final AtomicLong mNextWorkerId = new AtomicLong(1);

  /** Manage all the jobs' status. */
  private final JobIdGenerator mJobIdGenerator;
  private final CommandManager mCommandManager;
  private final Map<Long, JobCoordinator> mIdToJobCoordinator;

  /**
   * Creates a new instance of {@link JobMaster}.
   *
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  public JobMaster(JournalFactory journalFactory) {
    super(journalFactory.get(Constants.JOB_MASTER_NAME), new SystemClock(), ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.JOB_MASTER_NAME, 2));
    mJobIdGenerator = new JobIdGenerator();
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
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
    // Fail any jobs that were still running when the last job master stopped.
    for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
      JobInfo jobInfo = jobCoordinator.getJobInfo();
      if (!jobInfo.getStatus().isFinished()) {
        jobInfo.setStatus(Status.FAILED);
        jobInfo.setErrorMessage("Job failed: Job master shut down during execution");
        if (isLeader) {
          jobCoordinator.journalFinishedJob(mJournalEntryWriter);
        }
      }
    }
    if (isLeader) {
      getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.JOB_MASTER_LOST_WORKER_DETECTION,
              new LostWorkerDetectionHeartbeatExecutor(),
              Configuration.getInt(PropertyKey.JOB_MASTER_LOST_WORKER_INTERVAL_MS)));
    }
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
    JobInfo jobInfo;
    switch (entry.getEntryCase()) {
      case START_JOB:
        StartJobEntry startJob = entry.getStartJob();
        JobConfig jobConfig = (JobConfig) SerializationUtils.deserialize(
            startJob.getSerializedJobConfig().toByteArray(), "Failed to deserialize job config");
        jobInfo = new JobInfo(startJob.getJobId(), startJob.getName(), jobConfig);
        mIdToJobCoordinator.put(jobInfo.getId(),
            JobCoordinator.createForFinishedJob(jobInfo, mJournalEntryWriter));
        mJobIdGenerator.setNextJobId(Math.max(mJobIdGenerator.getNewJobId(), jobInfo.getId() + 1));
        break;
      case FINISH_JOB:
        FinishJobEntry finishJob = entry.getFinishJob();
        jobInfo = mIdToJobCoordinator.get(finishJob.getJobId()).getJobInfo();
        jobInfo.setStatus(ProtoUtils.fromProto(finishJob.getStatus()));
        jobInfo.setErrorMessage(finishJob.getErrorMessage());
        jobInfo.setResult(finishJob.getResult());
        for (Job.TaskInfo taskInfo : finishJob.getTaskInfoList()) {
          jobInfo.setTaskInfo(taskInfo.getTaskId(), ProtoUtils.fromProto(taskInfo));
        }
        break;
      default:
        break;
    }
  }

  @Override
  public void streamToJournalCheckpoint(final JournalOutputStream outputStream) {
    for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
      jobCoordinator.streamToJournalCheckpoint(new JournalEntryWriter() {
        @Override
        public void writeJournalEntry(JournalEntry entry) {
          try {
            outputStream.writeEntry(entry);
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
      });
    }
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
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, getWorkerInfoList(), jobInfo, mJournalEntryWriter);
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
    synchronized (mIdToJobCoordinator) {
      return Lists.newArrayList(mIdToJobCoordinator.keySet());
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
    synchronized (mIdToJobCoordinator) {
      if (!mIdToJobCoordinator.containsKey(jobId)) {
        throw new JobDoesNotExistException(jobId);
      }
      return mIdToJobCoordinator.get(jobId).getJobInfo();
    }
  }

  /**
   * Returns a worker id for the given worker.
   *
   * @param workerNetAddress the worker {@link WorkerNetAddress}
   * @return the worker id for this worker
   */
  public long registerWorker(WorkerNetAddress workerNetAddress) {
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

      LOG.info("registerWorker(): WorkerNetAddress: {} id: {}", workerNetAddress, workerId);
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
    MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
    if (worker == null) {
      return Arrays.asList(JobCommand.registerCommand(new RegisterCommand()));
    }
    worker.updateLastUpdatedTimeMs();
    // update the job info
    List<Long> updatedJobIds = new ArrayList<>();
    for (TaskInfo taskInfo : taskInfoList) {
      JobInfo jobInfo = mIdToJobCoordinator.get(taskInfo.getJobId()).getJobInfo();
      if (jobInfo == null) {
        // The master must have restarted and forgotten about the job.
        continue;
      }
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
   * Lost worker periodic check.
   */
  private final class LostWorkerDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostWorkerDetectionHeartbeatExecutor}.
     */
    public LostWorkerDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat() {
      int masterWorkerTimeoutMs = Configuration.getInt(PropertyKey.JOB_MASTER_WORKER_TIMEOUT_MS);
      for (MasterWorkerInfo worker : mWorkers) {
        synchronized (worker) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.error("The worker {} timed out after {}ms without a heartbeat!", worker,
                lastUpdate);
            for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
              jobCoordinator.failTasksForWorker(worker.getId());
            }
            mWorkers.remove(worker);
          }
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
