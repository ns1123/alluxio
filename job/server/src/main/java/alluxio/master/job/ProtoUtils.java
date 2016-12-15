/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.proto.journal.Job;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class ProtoUtils {

  /**
   * @return the protocol buffer version of this task info
   */
  public static alluxio.proto.journal.Job.TaskInfo toProto(TaskInfo taskInfo) {
    Job.TaskInfo.Builder builder = alluxio.proto.journal.Job.TaskInfo.newBuilder()
        .setJobId(taskInfo.getJobId())
        .setTaskId(taskInfo.getTaskId())
        .setStatus(toProto(taskInfo.getStatus()));
    if (taskInfo.getErrorMessage() != null) {
      builder.setErrorMessage(taskInfo.getErrorMessage());
    }
    if (taskInfo.getResult() != null) {
      builder.setResult(ByteString
          .copyFrom(SerializationUtils.serialize(taskInfo.getResult(), "Failed to serialize task result")));
    }
    return builder.build();
  }

  /**
   * @param taskInfo a task info in protocol buffer format
   * @return the {@link TaskInfo} version of the given protocol buffer task info
   */
  public static TaskInfo fromProto(Job.TaskInfo taskInfo) {
    Preconditions.checkState(taskInfo.hasJobId(),
        "Deserializing protocol task info with unset jobId");
    Preconditions.checkState(taskInfo.hasTaskId(),
        "Deserializing protocol task info with unset taskId");
    Preconditions.checkState(taskInfo.hasStatus(),
        "Deserializing protocol task info with unset status");
    TaskInfo info = new TaskInfo()
        .setJobId(taskInfo.getJobId())
        .setTaskId(taskInfo.getTaskId())
        .setStatus(fromProto(taskInfo.getStatus()))
        .setErrorMessage(taskInfo.getErrorMessage());
    if (taskInfo.hasResult()) {
      info.setResult(taskInfo.getResult().toByteArray());
    }
    return info;
  }

  /**
   * @param status the status to convert
   * @return the protocol buffer type representing the given status
   */
  public static alluxio.proto.journal.Job.Status toProto(Status status) {
    return alluxio.proto.journal.Job.Status.valueOf(status.name());
  }

  /**
   * @param status the protocol buffer status to convert
   * @return the {@link Status} type representing the given status
   */
  public static Status fromProto(alluxio.proto.journal.Job.Status status) {
    return Status.valueOf(status.name());
  }

  private ProtoUtils() {} // prevent instantiation
}
