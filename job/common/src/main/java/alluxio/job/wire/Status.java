/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.wire;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The status of a task.
 */
@ThreadSafe
public enum Status {
  CREATED, CANCELED, FAILED, RUNNING, COMPLETED;

  /**
   * @return whether this status represents a finished state, i.e. canceled, failed, or completed
   */
  public boolean isFinished() {
    return this.equals(CANCELED) || this.equals(FAILED) || this.equals(COMPLETED);
  }

  /**
   * @return thrift representation of the status
   */
  public alluxio.thrift.Status toThrift() {
    switch (this) {
      case CREATED:
        return alluxio.thrift.Status.CREATED;
      case CANCELED:
        return alluxio.thrift.Status.CANCELED;
      case FAILED:
        return alluxio.thrift.Status.FAILED;
      case RUNNING:
        return alluxio.thrift.Status.RUNNING;
      case COMPLETED:
        return alluxio.thrift.Status.COMPLETED;
      default:
        return alluxio.thrift.Status.UNKNOWN;
    }
  }
}