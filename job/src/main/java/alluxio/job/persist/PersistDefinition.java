/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.persist;

import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.wire.WorkerInfo;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job that persists a file into the under storage.
 */
@NotThreadSafe
public final class PersistDefinition extends AbstractVoidJobDefinition<PersistConfig, Void> {

  @Override
  public Map<WorkerInfo, Void> selectExecutors(PersistConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void runTask(PersistConfig config, Void args, JobWorkerContext jobWorkerContext)
      throws Exception {
    throw new UnsupportedOperationException();
  }

}
