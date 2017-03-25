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
import alluxio.job.exception.ErrorConfig;
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
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The master that handles all job managing operations.
 */
@ThreadSafe
public final class JobMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(JobMaster.class);
  private static final long MAX_CAPACITY = 10;
  private static final long TIMEOUT_WINDOW = 10000;

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
  private final PriorityQueue<JobInfo> mJobCache;

  /**
   * Creates a new instance of {@link JobMaster}.
   *
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  public JobMaster(JournalFactory journalFactory) {
    super(journalFactory.create(Constants.JOB_MASTER_NAME), new SystemClock(),
        ExecutorServiceFactories
            .fixedThreadPoolExecutorServiceFactory(Constants.JOB_MASTER_NAME, 2));
    mJobIdGenerator = new JobIdGenerator();
    mCommandManager = new CommandManager();
    mIdToJobCoordinator = Maps.newHashMap();
    mJobCache = new PriorityQueue<>();
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
    if (entry.hasStartJob()) {
      StartJobEntry startJob = entry.getStartJob();
      JobConfig jobConfig;
      try {
        jobConfig = (JobConfig) SerializationUtils.deserialize(
            startJob.getSerializedJobConfig().toByteArray());
      } catch (ClassNotFoundException e) {
        LOG.warn("Failed to deserialize job configuration from journal: {}", e.getMessage());
        jobConfig = new ErrorConfig("Failed to deserialize job config: " + e.toString());
      }
      try {
        jobInfo = createJob(startJob.getJobId(), startJob.getName(), jobConfig, startJob.getLastModified());
      } catch (IllegalStateException e) {
        LOG.warn("Failed create a job: {}", e.getMessage());
        return;
      }
      mIdToJobCoordinator.put(jobInfo.getId(),
          JobCoordinator.createForFinishedJob(jobInfo, mJournalEntryWriter));
      mJobIdGenerator.setNextJobId(Math.max(mJobIdGenerator.getNewJobId(), jobInfo.getId() + 1));
    } else if (entry.hasFinishJob()) {
      FinishJobEntry finishJob = entry.getFinishJob();
      jobInfo = mIdToJobCoordinator.get(finishJob.getJobId()).getJobInfo();
      // The job might no longer exist if it got evicted.
      if (jobInfo != null) {
        jobInfo.setStatus(ProtoUtils.fromProto(finishJob.getStatus()));
        jobInfo.setErrorMessage(finishJob.getErrorMessage());
        jobInfo.setResult(finishJob.getResult());
        for (Job.TaskInfo taskInfo : finishJob.getTaskInfoList()) {
          jobInfo.setTaskInfo(taskInfo.getTaskId(), ProtoUtils.fromProto(taskInfo));
        }
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void streamToJournalCheckpoint(final JournalOutputStream outputStream) {
    for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
      jobCoordinator.streamToJournalCheckpoint(new JournalEntryWriter() {
        @Override
        public void writeJournalEntry(JournalEntry entry) {
          try {
            outputStream.write(entry);
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
  public synchronized long run(JobConfig jobConfig) throws JobDoesNotExistException {
    long jobId = mJobIdGenerator.getNewJobId();
    JobInfo jobInfo;
    try {
      jobInfo = createJob(jobId, jobConfig.getName(), jobConfig, CommonUtils.getCurrentMs());
    } catch (IllegalStateException e) {
      throw new JobDoesNotExistException(e.getMessage());
    }
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, getWorkerInfoList(), jobInfo, mJournalEntryWriter);
    mIdToJobCoordinator.put(jobId, jobCoordinator);
    return jobId;
  }

  /**
   * Cancels a job.
   *
   * @param jobId the id of the job
   * @throws JobDoesNotExistException when the job does not exist
   */
  public synchronized void cancel(long jobId) throws JobDoesNotExistException {
    if (!mIdToJobCoordinator.containsKey(jobId)) {
      throw new JobDoesNotExistException(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(jobId));
    }
    JobCoordinator jobCoordinator = mIdToJobCoordinator.get(jobId);
    jobCoordinator.cancel();
  }

  /**
   * @return list all the job ids
   */
  public synchronized List<Long> list() {
    return Lists.newArrayList(mIdToJobCoordinator.keySet());
  }

  /**
   * Gets information of the given job id.
   *
   * @param jobId the id of the job
   * @return the job information
   * @throws JobDoesNotExistException if the job does not exist
   */
  public synchronized alluxio.job.wire.JobInfo getStatus(long jobId) throws JobDoesNotExistException {
    if (!mIdToJobCoordinator.containsKey(jobId)) {
      throw new JobDoesNotExistException(jobId);
    }
    return new alluxio.job.wire.JobInfo(mIdToJobCoordinator.get(jobId).getJobInfo());
  }

  /**
   * Returns a worker id for the given worker.
   *
   * @param workerNetAddress the worker {@link WorkerNetAddress}
   * @return the worker id for this worker
   */
  public synchronized long registerWorker(WorkerNetAddress workerNetAddress) {
    if (mWorkers.contains(mAddressIndex, workerNetAddress)) {
      // If the worker is trying to reregister, it must have died and been restarted. We need to
      // clean up the dead worker.
      LOG.info(
          "Worker at address {} is reregistering. Failing tasks for previous worker at that address",
          workerNetAddress);
      MasterWorkerInfo deadWorker = mWorkers.getFirstByField(mAddressIndex, workerNetAddress);
      for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
        jobCoordinator.failTasksForWorker(deadWorker.getId());
      }
      mWorkers.remove(deadWorker);
    }

    // Generate a new worker id.
    long workerId = mNextWorkerId.getAndIncrement();
    mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress));

    LOG.info("registerWorker(): WorkerNetAddress: {} id: {}", workerNetAddress, workerId);
    return workerId;
  }

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  public synchronized List<WorkerInfo> getWorkerInfoList() {
    List<WorkerInfo> workerInfoList = new ArrayList<WorkerInfo>(mWorkers.size());
    for (MasterWorkerInfo masterWorkerInfo : mWorkers) {
      workerInfoList.add(masterWorkerInfo.generateClientWorkerInfo());
    }
    return workerInfoList;
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
      return Collections.singletonList(JobCommand.registerCommand(new RegisterCommand()));
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

    return mCommandManager.pollAllPendingCommands(workerId);
  }

  private synchronized JobInfo createJob(long jobId, String jobName, JobConfig jobConfig,
      long lastModifiedMs) throws IllegalStateException {
    JobInfo jobInfo = new JobInfo(jobId, jobName, jobConfig, lastModifiedMs);
    if (mJobCache.size() < MAX_CAPACITY) {
      mJobCache.add(jobInfo);
    } else {
      // Check if the top item can be evicted.
      JobInfo evictionCandidate = mJobCache.peek();
      Status status = evictionCandidate.getStatus();
      switch (status) {
        case CREATED:
        case RUNNING:
          // do not evict the candidate job if it is still running
          throw new IllegalStateException(ExceptionMessage.RESOURCE_UNAVAILABLE.getMessage());
        case CANCELED:
        case COMPLETED:
        case FAILED:
          if (CommonUtils.getCurrentMs() - evictionCandidate.getLastModifiedMs() < TIMEOUT_WINDOW) {
            // do not evict the candidate job if it has been updated recently
            throw new IllegalStateException(ExceptionMessage.RESOURCE_UNAVAILABLE.getMessage());
          }
          mJobCache.poll();
          mIdToJobCoordinator.remove(evictionCandidate.getId());
          mJobCache.add(jobInfo);
          break;
        default:
          throw new IllegalStateException("Unexpected job status: " + status);
      }
    }
    return jobInfo;
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
      synchronized (JobMaster.this) {
        for (MasterWorkerInfo worker : mWorkers) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.warn("The worker {} timed out after {}ms without a heartbeat!", worker, lastUpdate);
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
