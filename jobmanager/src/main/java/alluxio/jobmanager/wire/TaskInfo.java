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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The task information.
 */
@NotThreadSafe
public class TaskInfo {
  private int mTaskId;
  private Status mStatus;
  private String mErrorMessage;

  /**
   * Default constructor.
   */
  public TaskInfo() {}

  /**
   * Constructs from the thrift format
   *
   * @param taskInfo the task info in thrift format
   */
  public TaskInfo(alluxio.thrift.TaskInfo taskInfo) {
    mTaskId = taskInfo.getTaskId();
    mStatus = Status.valueOf(taskInfo.getStatus().name());
    mErrorMessage = taskInfo.getErrorMessage();
  }

  /**
   * @param taskId the task id
   */
  public void setTaskId(int taskId) {
    mTaskId = taskId;
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
   * @param status the task status
   */
  public void setStatus(Status status) {
    mStatus = status;
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
