/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.meta;

import alluxio.job.JobConfig;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The job information used by the job master internally.
 */
@ThreadSafe
public final class JobInfo implements Comparable<JobInfo> {
  private final long mId;
  private final String mName;
  private Set<JobInfo> mFinishedJobs;
  private final JobConfig mJobConfig;
  private final Map<Integer, TaskInfo> mTaskIdToInfo;
  private long mLastModifiedTimeMs;
  private String mErrorMessage;
  private Status mStatus;
  private String mResult;

  /**
   * Creates a new instance of {@link JobInfo}.
   *
   * @param id the job id
   * @param name the job name
   * @param jobConfig the job configuration
   * @param finishedJobs the set of finished jobs
   */
  public JobInfo(long id, String name, JobConfig jobConfig, Set<JobInfo> finishedJobs) {
    mId = id;
    mName = Preconditions.checkNotNull(name);
    mFinishedJobs = finishedJobs;
    mJobConfig = Preconditions.checkNotNull(jobConfig);
    mTaskIdToInfo = Maps.newHashMap();
    mLastModifiedTimeMs = CommonUtils.getCurrentMs();
    mErrorMessage = "";
    mStatus = Status.CREATED;
  }

  /**
   * {@inheritDoc}
   *
   * This method orders jobs using the time their status was last modified.
   */
  @Override
  public synchronized int compareTo(JobInfo other) {
    return Long.compare(mLastModifiedTimeMs, other.getLastModifiedTimeMs());
  }

  /**
   * Registers a task.
   *
   * @param taskId the task id
   */
  public synchronized void addTask(int taskId) {
    Preconditions.checkArgument(!mTaskIdToInfo.containsKey(taskId), "");
    mTaskIdToInfo.put(taskId, new TaskInfo().setJobId(mId).setTaskId(taskId)
        .setStatus(Status.CREATED).setErrorMessage("").setResult(null));
  }

  /**
   * @return the job id
   */
  public synchronized long getId() {
    return mId;
  }

  /**
   * @return the job name
   */
  public synchronized String getName() {
    return mName;
  }

  /**
   * @return the job configuration
   */
  public synchronized JobConfig getJobConfig() {
    return mJobConfig;
  }

  /**
   * @return the time when the job status was last modified (in milliseconds)
   */
  public synchronized long getLastModifiedTimeMs() {
    return mLastModifiedTimeMs;
  }

  /**
   * @param errorMessage the error message
   */
  public synchronized void setErrorMessage(String errorMessage) {
    mErrorMessage = errorMessage == null ? "" : errorMessage;
  }

  /**
   * @return the error message
   */
  public synchronized String getErrorMessage() {
    return mErrorMessage;
  }

  /**
   * @param taskId the task ID to get the task info for
   * @return the task info, or null if the task ID doesn't exist
   */
  public synchronized TaskInfo getTaskInfo(int taskId) {
    return mTaskIdToInfo.get(taskId);
  }

  /**
   * Sets the information of a task.
   *
   * @param taskId the task id
   * @param taskInfo the task information
   */
  public synchronized void setTaskInfo(int taskId, TaskInfo taskInfo) {
    mTaskIdToInfo.put(taskId, taskInfo);
  }

  /**
   * @return the list of task ids
   */
  public synchronized List<Integer> getTaskIdList() {
    return Lists.newArrayList(mTaskIdToInfo.keySet());
  }

  /**
   * @param status the job status
   */
  public synchronized void setStatus(Status status) {
    mStatus = status;
    mLastModifiedTimeMs = CommonUtils.getCurrentMs();
    if (mStatus.isFinished()) {
      mFinishedJobs.remove(this);
      mFinishedJobs.add(this);
    }
  }

  /**
   * @return the status of the job
   */
  public synchronized Status getStatus() {
    return mStatus;
  }

  /**
   * @param result the joined job result
   */
  public synchronized void setResult(String result) {
    mResult = result;
  }

  /**
   * @return the result of the job
   */
  public synchronized String getResult() {
    return mResult;
  }

  /**
   * @return the list of task information
   */
  public synchronized List<TaskInfo> getTaskInfoList() {
    return Lists.newArrayList(mTaskIdToInfo.values());
  }
}
