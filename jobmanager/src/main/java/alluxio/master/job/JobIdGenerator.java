/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class generates unique job ids.
 *
 * TODO(yupeng) add journal support
 */
@ThreadSafe
public final class JobIdGenerator {
  private final AtomicLong mNextJobId;

  /**
   * Creates a new instance.
   */
  public JobIdGenerator() {
    mNextJobId = new AtomicLong(0);
  }

  /**
   * @return a new job id
   */
  public synchronized long getNewJobId() {
    return mNextJobId.getAndIncrement();
  }
}
