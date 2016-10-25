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
   * @param status the status to convert
   * @return the protobuf type representing the given status
   */
  public static alluxio.proto.journal.Job.Status toProto(Status status) {
    return alluxio.proto.journal.Job.Status.valueOf(status.name());
  }

  /**
   * @param status the protobuf type to convert
   * @return the {@link Status} type representing the given status
   */
  public static Status fromProto(alluxio.proto.journal.Job.Status status) {
    return Status.valueOf(status.name());
  }
}
