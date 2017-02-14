/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.fs;

import alluxio.wire.WorkerInfo;

import java.util.Comparator;

/**
 * Utility class to make it easier to write jobs.
 */
public final class JobUtils {

  /**
   * @return a comparator for WorkerInfo
   */
  public static Comparator<WorkerInfo> createWorkerInfoComparator() {
    return new Comparator<WorkerInfo>() {
      @Override
      public int compare(WorkerInfo o1, WorkerInfo o2) {
        if (o1.getId() > o2.getId()) {
          return 1;
        } else if (o1.getId() == o2.getId()) {
          return 0;
        } else {
          return -1;
        }
      }
    };
  }

  private JobUtils() {} // Utils class not intended for instantiation.
}
