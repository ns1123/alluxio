/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

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
