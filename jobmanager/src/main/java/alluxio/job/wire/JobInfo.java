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
import com.google.common.base.Preconditions;
import jersey.repackaged.com.google.common.collect.Lists;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The job descriptor.
 */
@NotThreadSafe
public final class JobInfo {
  private long mJobId;
  private String mErrorMessage;
  private List<TaskInfo> mTaskInfoList;

  /**
   * Default constructor.
   */
  public JobInfo() {}

  /**
   * Constructs the job info from the thrift {@link alluxio.master.job.meta.JobInfo}.
   *
   * @param jobInfo the job info in thrift format
   */
  public JobInfo(alluxio.master.job.meta.JobInfo jobInfo) {
    mJobId = jobInfo.getId();
    mErrorMessage = jobInfo.getErrorMessage();
    mTaskInfoList = Lists.newArrayList();
    for (alluxio.thrift.TaskInfo taskInfo : jobInfo.getTaskInfoList()) {
      mTaskInfoList.add(new TaskInfo(taskInfo));
    }
  }

  /**
   * @param jobId the job id
   */
  public void setJobId(long jobId) {
    mJobId = jobId;
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @param taskInfoList the list of task descriptors
   */
  public void setTaskInfoList(List<TaskInfo> taskInfoList) {
    mTaskInfoList = Preconditions.checkNotNull(taskInfoList);
  }

  /**
   * @return the list of task descriptors
   */
  public List<TaskInfo> getTaskInfoList() {
    return mTaskInfoList;
  }

  /**
   * @param errorMessage the error message
   */
  public void setErrorMessage(String errorMessage) {
    mErrorMessage = errorMessage;
  }

  /**
   * @return the error message
   */
  public String getErrorMessage() {
    return mErrorMessage;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof JobInfo)) {
      return false;
    }
    JobInfo that = (JobInfo) o;
    return Objects.equal(mJobId, that.mJobId) &&
        Objects.equal(mErrorMessage, that.mErrorMessage) &&
        Objects.equal(mTaskInfoList, that.mTaskInfoList);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobId, mErrorMessage, mTaskInfoList);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("jobId", mJobId).add("errorMessage", mErrorMessage)
        .add("taskInfoList", mTaskInfoList).toString();
  }
}
