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
import alluxio.wire.WorkerInfo;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple dummy job for testing.
 */
@NotThreadSafe
public final class TestJobDefinition
    extends AbstractVoidJobDefinition<TestJobConfig, SerializableVoid> {

  /**
   * Constructs a new {@link TestJobDefinition}.
   */
  public TestJobDefinition() {}

  @Override
  public Class<TestJobConfig> getJobConfigClass() {
    return TestJobConfig.class;
  }

  @Override
  public SerializableVoid runTask(TestJobConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext) throws Exception {
    return null;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(TestJobConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    return null;
  }
}
