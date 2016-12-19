/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Job configuration for the sleep job.
 */
@ThreadSafe
public class SleepJobConfig implements JobConfig {
  private static final long serialVersionUID = 43139051130518451L;

  public static final String NAME = "Sleep";

  private final long mTimeMs;

  /**
   * @param timeMs the time to sleep for in milliseconds
   */
  public SleepJobConfig(@JsonProperty("timeMs") long timeMs) {
    mTimeMs = timeMs;
  }

  /**
   * @return the time to sleep for in milliseconds
   */
  public long getTimeMs() {
    return mTimeMs;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SleepJobConfig)) {
      return false;
    }
    SleepJobConfig that = (SleepJobConfig) obj;
    return mTimeMs == that.mTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mTimeMs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("timeMs", mTimeMs).toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
