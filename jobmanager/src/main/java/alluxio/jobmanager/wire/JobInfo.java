/*************************************************************************
* Copyright (c) 2016 Alluxio, Inc.  All rights reserved.
*
* This software and all information contained herein is confidential and
* proprietary to Alluxio, and is protected by copyright and other
* applicable laws in the United States and other jurisdictions.  You may
* not use, modify, reproduce, distribute, or disclose this software
* without the express written permission of Alluxio.
**************************************************************************/
package alluxio.jobmanager.wire;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * The job info descriptor.
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
   * Constructs the job info from the thrift {@link alluxio.master.jobmanager.job.JobInfo}.
   *
   * @param jobInfo the job info in thrift format
   */
  public JobInfo(alluxio.master.jobmanager.job.JobInfo jobInfo) {
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
   * @param taskInfoList the list of task info
   */
  public void setTaskInfoList(List<TaskInfo> taskInfoList) {
    mTaskInfoList = Preconditions.checkNotNull(taskInfoList);
  }

  /**
   * @return the list of task info
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
}
