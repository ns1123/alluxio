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
import alluxio.job.meta.JobInfo;
import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.job.command.CommandManager;
import alluxio.proto.journal.Job.FinishJobEntry;
import alluxio.proto.journal.Job.FinishJobEntry.Builder;
import alluxio.proto.journal.Job.StartJobEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
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
  /**
   * List of all job workers at the time when the job was started. If this coordinator was created
   * to represent an already-completed job, this list will be empty.
   */
  private final List<WorkerInfo> mWorkersInfoList;
  private JournalEntryWriter mJournalEntryWriter;
  /**
   * Map containing the worker info for every task associated with the coordinated job. If this
   * coordinator was created to represent an already-completed job, this map will be empty.
   */
  private final Map<Integer, WorkerInfo> mTaskIdToWorkerInfo = Maps.newHashMap();
  /**
   * Mapping from workers running tasks for this job to the ids of those tasks. If this
   * coordinator was created to represent an already-completed job, this map will be empty.
   */
  private final Map<Long, Integer> mWorkerIdToTaskId = Maps.newHashMap();

  private JobCoordinator(CommandManager commandManager, List<WorkerInfo> workerInfoList,
      JobInfo jobInfo, JournalEntryWriter journalEntryWriter) {
    mJobInfo = Preconditions.checkNotNull(jobInfo);
    mCommandManager = commandManager;
    mWorkersInfoList = workerInfoList;
    mJournalEntryWriter = journalEntryWriter;
  }

  /**
   * Creates a new instance of {@link JobCoordinator} for a completed job. This coordinator is only
   * used to track the conclusion of a finished job.
   *
   * @param jobInfo info for the completed job
   * @param writer an object to use for writing journal entries
   * @return the created coordinator
   */
  public static JobCoordinator createForFinishedJob(JobInfo jobInfo, JournalEntryWriter writer) {
    return new JobCoordinator(null, Lists.<WorkerInfo>newArrayList(), jobInfo, writer);
  }

  /**
   * Creates a new instance of the {@link JobCoordinator}.
   *
   * @param commandManager the command manager
   * @param workerInfoList the list of workers to use
   * @param jobInfo the job information
   * @param journalEntryWriter an object to use for writing journal entries
   * @return the created coordinator
   * @throws JobDoesNotExistException when the job definition doesn't exist
   */
  public static JobCoordinator create(CommandManager commandManager,
      List<WorkerInfo> workerInfoList, JobInfo jobInfo, JournalEntryWriter journalEntryWriter)
          throws JobDoesNotExistException {
    Preconditions.checkNotNull(commandManager);
    JobCoordinator jobCoordinator =
        new JobCoordinator(commandManager, workerInfoList, jobInfo, journalEntryWriter);
    jobCoordinator.start();
    // start the coordinator, create the tasks
    return jobCoordinator;
  }

  private synchronized void start() throws JobDoesNotExistException {
    // get the job definition
    JobDefinition<JobConfig, ?, ?> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());

    journalStartedJob(mJournalEntryWriter);

    JobMasterContext context = new JobMasterContext(mJobInfo.getId());
    Map<WorkerInfo, ?> taskAddressToArgs;
    try {
      taskAddressToArgs = definition
          .selectExecutors(mJobInfo.getJobConfig(), mWorkersInfoList, context);
    } catch (Exception e) {
      LOG.warn("select executor failed", e);
      mJobInfo.setStatus(Status.FAILED);
      mJobInfo.setErrorMessage(e.getMessage());
      journalFinishedJob(mJournalEntryWriter);
      return;
    }
    if (taskAddressToArgs.isEmpty()) {
      LOG.info("No executor is selected");
      updateStatus();
    }

    for (Entry<WorkerInfo, ?> entry : taskAddressToArgs.entrySet()) {
      LOG.info("selected executor " + entry.getKey() + " with parameters " + entry.getValue());
      int taskId = mTaskIdToWorkerInfo.size();
      // create task
      mJobInfo.addTask(taskId);
      // submit commands
      mCommandManager.submitRunTaskCommand(mJobInfo.getId(), taskId, mJobInfo.getJobConfig(),
          entry.getValue(), entry.getKey().getId());
      mTaskIdToWorkerInfo.put(taskId, entry.getKey());
      mWorkerIdToTaskId.put(entry.getKey().getId(), taskId);
    }
  }

  /**
   * Cancels the current job.
   */
  public void cancel() {
    for (int taskId : mJobInfo.getTaskIdList()) {
      mCommandManager.submitCancelTaskCommand(mJobInfo.getId(), taskId,
          mTaskIdToWorkerInfo.get(taskId).getId());
    }
  }

  /**
   * Updates the status of the job. When all the tasks are completed, run the join method in the
   * definition.
   */
  public void updateStatus() {
    int completed = 0;
    synchronized (mJobInfo) {
      List<TaskInfo> taskInfoList = mJobInfo.getTaskInfoList();
      for (TaskInfo info : taskInfoList) {
        switch (info.getStatus()) {
          case FAILED:
            mJobInfo.setStatus(Status.FAILED);
            if (mJobInfo.getErrorMessage().isEmpty()) {
              mJobInfo.setErrorMessage("Task execution failed: " + info.getErrorMessage());
            }
            journalFinishedJob(mJournalEntryWriter);
            return;
          case CANCELED:
            if (mJobInfo.getStatus() != Status.FAILED) {
              mJobInfo.setStatus(Status.CANCELED);
            }
            journalFinishedJob(mJournalEntryWriter);
            break;
          case RUNNING:
            if (mJobInfo.getStatus() != Status.FAILED && mJobInfo.getStatus() != Status.CANCELED) {
              mJobInfo.setStatus(Status.RUNNING);
            }
            break;
          case COMPLETED:
            completed++;
            break;
          case CREATED:
            // do nothing
            break;
          default:
            throw new IllegalArgumentException("Unsupported status " + info.getStatus());
        }
      }
      if (completed == taskInfoList.size()) {
        if (mJobInfo.getStatus() == Status.COMPLETED) {
          return;
        }

        // all the tasks completed, run join
        try {
          mJobInfo.setStatus(Status.COMPLETED);
          mJobInfo.setResult(join(taskInfoList));
        } catch (Exception e) {
          mJobInfo.setStatus(Status.FAILED);
          mJobInfo.setErrorMessage(e.getMessage());
          return;
        } finally {
          journalFinishedJob(mJournalEntryWriter);
        }
      }
    }
  }

  /**
   * Fails any incomplete tasks being run on the specified worker.
   *
   * @param workerId the id of the worker to fail tasks for
   */
  public void failTasksForWorker(Long workerId) {
    Integer taskId = mWorkerIdToTaskId.get(workerId);
    if (taskId == null) {
      return;
    }
    TaskInfo taskInfo = mJobInfo.getTaskInfo(taskId);
    if (taskInfo.getStatus() == Status.RUNNING || taskInfo.getStatus() == Status.CREATED) {
      taskInfo.setStatus(Status.FAILED);
      taskInfo.setErrorMessage("Job worker was lost before the task could complete");
      updateStatus();
      journalFinishedJob(mJournalEntryWriter);
    }
  }

  /**
   * @return the job info for the job being coordinated
   */
  public JobInfo getJobInfo() {
    return mJobInfo;
  }

  /**
   * Joins the task results and produces a final result.
   *
   * @param taskInfoList the list of task information
   * @return the aggregated result as a String
   * @throws Exception if any error occurs
   */
  private String join(List<TaskInfo> taskInfoList) throws Exception {
    // get the job definition
    JobDefinition<JobConfig, Serializable, Serializable> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());
    Map<WorkerInfo, Serializable> taskResults = Maps.newHashMap();
    for (TaskInfo taskInfo : taskInfoList) {
      taskResults.put(mTaskIdToWorkerInfo.get(taskInfo.getTaskId()), taskInfo.getResult());
    }
    return definition.join(mJobInfo.getJobConfig(), taskResults);
  }

  /**
   * Journals the starting of the job being coordinated.
   *
   * @param writer the journal entry writer to journal to
   */
  private void journalStartedJob(JournalEntryWriter writer) {
    writer.writeJournalEntry(JournalEntry.newBuilder()
        .setStartJob(StartJobEntry.newBuilder()
            .setJobId(mJobInfo.getId())
            .setName(mJobInfo.getName())
            .setSerializedJobConfig(ByteString.copyFrom(SerializationUtils.serialize(
                mJobInfo.getJobConfig(), "Failed to serialize job config"))))
        .build());
  }

  /**
   * Journals the finishing of the job being coordinated.
   *
   * @param writer the journal entry writer to journal to
   */
  public void journalFinishedJob(JournalEntryWriter writer) {
    List<alluxio.proto.journal.Job.TaskInfo> taskInfos = new ArrayList<>();
    for (TaskInfo taskInfo : mJobInfo.getTaskInfoList()) {
      taskInfos.add(ProtoUtils.toProto(taskInfo));
    }
    Builder builder = FinishJobEntry.newBuilder()
            .setJobId(mJobInfo.getId())
            .addAllTaskInfo(taskInfos)
            .setStatus(ProtoUtils.toProto(mJobInfo.getStatus()))
            .setErrorMessage(mJobInfo.getErrorMessage());
    if (mJobInfo.getResult() != null) {
      builder.setResult(mJobInfo.getResult());
    }
    writer.writeJournalEntry(JournalEntry.newBuilder()
        .setFinishJob(builder).build());
  }

  /**
   * Writes journal checkpoint information to the given output stream.
   *
   * @param writer the stream to write to
   */
  public synchronized void streamToJournalCheckpoint(JournalEntryWriter writer) {
    journalStartedJob(writer);
    if (mJobInfo.getStatus().isFinished()) {
      journalFinishedJob(writer);
    }
  }
}
