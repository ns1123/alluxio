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

import javax.annotation.concurrent.ThreadSafe;

/**
 * The job information used by the job master internally.
 */
@ThreadSafe
public final class JobInfo implements Comparable<JobInfo> {
  private final long mId;
  private final String mName;
  private final JobConfig mJobConfig;
  private final Map<Integer, TaskInfo> mTaskIdToInfo;
  private long mLastModifiedMs;
  private String mErrorMessage;
  private Status mStatus;
  private String mResult;

  /**
   * Creates a new instance of {@link JobInfo}.
   *
   * @param id the job id
   * @param name the name of the job
   * @param jobConfig the configuration
   * @param lastModifiedMs the time of last modification (in milliseconds)
   */
  public JobInfo(long id, String name, JobConfig jobConfig, long lastModifiedMs) {
    mId = id;
    mName = Preconditions.checkNotNull(name);
    mJobConfig = Preconditions.checkNotNull(jobConfig);
    mTaskIdToInfo = Maps.newHashMap();
    mLastModifiedMs = lastModifiedMs;
    mErrorMessage = "";
    mStatus = Status.CREATED;
  }

  /**
   * {@inheritDoc}
   *
   * This method orders jobs with respect to their completion and age.
   *
   * In particular, finished jobs are ordered before unfinished jobs and within each category,
   * jobs are ordered increasingly by the time of their last modification.
   */
  @Override
  public int compareTo(JobInfo other) {
    Status status = other.getStatus();
    // this > other if other finished and this has not
    if (!mStatus.isFinished() && status.isFinished()) {
      return 1;
    }
    // this < other if this finished and other has not
    if (!status.isFinished() && mStatus.isFinished()) {
      return -1;
    }
    // this < other if this is older than other
    return Long.compare(mLastModifiedMs, other.getLastModifiedMs());
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
    mLastModifiedMs = CommonUtils.getCurrentMs();
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
   * @return the time of last modification (in milliseconds)
   */
  public synchronized long getLastModifiedMs() {
    return mLastModifiedMs;
  }

  /**
   * @param errorMessage the error message
   */
  public synchronized void setErrorMessage(String errorMessage) {
    mErrorMessage = errorMessage == null ? "" : errorMessage;
    mLastModifiedMs = CommonUtils.getCurrentMs();
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
    mLastModifiedMs = CommonUtils.getCurrentMs();
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
    mLastModifiedMs = CommonUtils.getCurrentMs();
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
    mLastModifiedMs = CommonUtils.getCurrentMs();
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
