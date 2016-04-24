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
  private byte[] mResult;

  /**
   * Default constructor.
   */
  public TaskInfo() {}

  /**
   * @param taskInfo the task info in thrift format
   */
  public TaskInfo(long jobId, int taskId, Status status, String errorMessage, byte[] result) {
    mJobId = jobId;
    mTaskId = taskId;
    mStatus = status;
    mErrorMessage = errorMessage;
    mResult = result;
  }


  /**
   * Constructs from the thrift format.
   *
   * @param taskInfo the task info in thrift format
   */
  public TaskInfo(alluxio.thrift.TaskInfo taskInfo) {
    this(taskInfo.getJobId(), taskInfo.getTaskId(), Status.valueOf(taskInfo.getStatus().toString()),
        taskInfo.getErrorMessage(), taskInfo.getResult());
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
   * @return the result
   */
  public byte[] getResult() {
    return mResult;
  }

  /**
   * @param jobId the job id
   * @return the updated task info object
   */
  public TaskInfo setJobId(long jobId) {
    mJobId = jobId;
    return this;
  }

  /**
   * @param taskId the task id
   * @return the updated task info object
   */
  public TaskInfo setTaskId(int taskId) {
    mTaskId = taskId;
    return this;
  }

  /**
   * @param status the task status
   * @return the updated task info object
   */
  public TaskInfo setStatus(Status status) {
    mStatus = status;
    return this;
  }

  /**
   * @param errorMessage the error message
   * @return the updated task info object
   */
  public TaskInfo setErrorMessage(String errorMessage) {
    mErrorMessage = errorMessage;
    return this;
  }

  /**
   * @param result the result
   * @return the updated task info object
   */
  public TaskInfo setResult(byte[] result) {
    mResult = result;
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
        && Objects.equal(mErrorMessage, that.mErrorMessage)
        && Objects.equal(mResult, that.mResult);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobId, mTaskId, mStatus, mErrorMessage, mResult);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("jobId", mJobId)
        .add("taskId", mTaskId)
        .add("status", mStatus)
        .add("errorMessage", mErrorMessage)
        .add("result", mResult)
        .toString();
  }
}
