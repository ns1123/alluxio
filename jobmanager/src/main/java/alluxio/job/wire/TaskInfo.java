/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.wire;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The task description.
 */
@NotThreadSafe
public class TaskInfo {
  private long mJobId;
  private int mTaskId;
  private Status mStatus;
  private String mErrorMessage;

  /**
   * Default constructor.
   */
  public TaskInfo() {}

  /**
   * Constructs from the thrift format.
   *
   * @param taskInfo the task info in thrift format
   */
  public TaskInfo(alluxio.thrift.TaskInfo taskInfo) {
    mTaskId = taskInfo.getTaskId();
    mStatus = Status.valueOf(taskInfo.getStatus().name());
    mErrorMessage = taskInfo.getErrorMessage();
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @return the task id
   */
  public int getTaskId() {
    return mTaskId;
  }

  /**
   * @return the task status
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @return the error message
   */
  public String getErrorMessage() {
    return mErrorMessage;
  }

  /**
   * @param jobId the job id
   */
  public TaskInfo setJobId(long jobId) {
    mJobId = jobId;
    return this;
  }

  /**
   * @param taskId the task id
   */
  public TaskInfo setTaskId(int taskId) {
    mTaskId = taskId;
    return this;
  }

  /**
   * @param status the task status
   */
  public TaskInfo setStatus(Status status) {
    mStatus = status;
    return this;
  }

  /**
   * @param errorMessage the error message
   */
  public TaskInfo setErrorMessage(String errorMessage) {
    mErrorMessage = errorMessage;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TaskInfo)) {
      return false;
    }
    TaskInfo that = (TaskInfo) o;
    return Objects.equal(mJobId, that.mJobId)
        && Objects.equal(mTaskId, that.mTaskId)
        && Objects.equal(mStatus, that.mStatus)
        && Objects.equal(mErrorMessage, that.mErrorMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobId, mTaskId, mStatus, mErrorMessage);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("jobId", mJobId)
        .add("taskId", mTaskId)
        .add("status", mStatus)
        .add("errorMessage", mErrorMessage).toString();
  }
}
