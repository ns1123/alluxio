/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The context is used by job to access master-side resources and information.
 */
@ThreadSafe
public final class JobMasterContext {
  private final long mJobId;

  /**
   * @param jobId the job id
   */
  public JobMasterContext(long jobId) {
    mJobId = jobId;
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }
}
