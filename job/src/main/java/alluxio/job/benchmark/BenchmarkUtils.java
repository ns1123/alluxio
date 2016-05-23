/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.util.CommonUtils;

import com.google.common.base.Function;

/**
 * Utility methods for benchmakrs.
 */
public final class BenchmarkUtils {

  /**
   * Waits for a condition to be satisfied until a timeout occurs.
   *
   * @param condition the condition to wait on
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   */
  public static void waitFor(Function<Void, Boolean> condition, int timeoutMs) {
    long start = System.currentTimeMillis();
    while (!condition.apply(null)) {
      if (System.currentTimeMillis() - start > timeoutMs) {
        throw new RuntimeException("Timed out waiting for condition " + condition);
      }
      CommonUtils.sleepMs(20);
    }
  }

  private BenchmarkUtils() {} // prevent instantiation
}
