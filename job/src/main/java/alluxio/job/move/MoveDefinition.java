/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.move;

import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.wire.WorkerInfo;

import java.util.List;
import java.util.Map;

/**
 * A job that moves a source file to a destination path.
 */
public final class MoveDefinition
    extends AbstractVoidJobDefinition<MoveConfig, List<MoveCommand>> {

  /**
   * {@inheritDoc}
   *
   * Assigns each worker to move whichever files it has the most blocks for. If the source and
   * destination are under the same mount point, no executors are needed and the metadata move
   * operation is performed.
   */
  @Override
  public Map<WorkerInfo, List<MoveCommand>> selectExecutors(MoveConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   *
   * Moves the file specified in the config to the configured path. If the destination path is a
   * directory, the file is moved inside that directory.
   */
  @Override
  public Void runTask(MoveConfig config, List<MoveCommand> commands,
      JobWorkerContext jobWorkerContext) throws Exception {
    throw new UnsupportedOperationException();
  }
}
