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
