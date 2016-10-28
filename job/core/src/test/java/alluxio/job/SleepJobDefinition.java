/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.job.util.SerializableVoid;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The definition for a job which sleeps for the specified number of milliseconds on each worker.
 */
public final class SleepJobDefinition
    extends AbstractVoidJobDefinition<SleepJobConfig, SerializableVoid> {

  /**
   * Constructs a new {@link SleepJobDefinition}.
   */
  public SleepJobDefinition() {}

  @Override
  public Class<SleepJobConfig> getJobConfigClass() {
    return SleepJobConfig.class;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(SleepJobConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Map<WorkerInfo, SerializableVoid> executors = new HashMap<>();
    for (WorkerInfo jobWorker : jobWorkerInfoList) {
      executors.put(jobWorker, null);
    }
    return executors;
  }

  @Override
  public SerializableVoid runTask(SleepJobConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext) throws Exception {
    CommonUtils.sleepMs(config.getTimeMs());
    return null;
  }
}
