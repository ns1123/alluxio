/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.meta;

import alluxio.util.CommonUtils;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class generates unique job ids.
 */
@ThreadSafe
public final class JobIdGenerator {
  private final AtomicLong mNextJobId;

  /**
   * Creates a new instance.
   */
  public JobIdGenerator() {
    // shift by 10,000 to avoid conflicts with worker IDs.
    mNextJobId = new AtomicLong(CommonUtils.getCurrentMs() + 10 * 1000);
  }

  /**
   * @return a new job id
   */
  public long getNewJobId() {
    return mNextJobId.getAndIncrement();
  }
}
