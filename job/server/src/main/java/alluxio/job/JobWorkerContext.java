/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.underfs.UfsManager;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The context of worker-side resources and information.
 */
@ThreadSafe
public final class JobWorkerContext {
  private final long mJobId;
  private final int mTaskId;
  private final UfsManager mUfsManager;

  /**
   * Creates a new instance of {@link JobWorkerContext}.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param ufsManager the UFS manager
   */
  public JobWorkerContext(long jobId, int taskId, UfsManager ufsManager) {
    mJobId = jobId;
    mTaskId = taskId;
    mUfsManager = ufsManager;
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
   * @return the UFS manager
   */
  public UfsManager getUfsManager() {
    return mUfsManager;
  }
}
