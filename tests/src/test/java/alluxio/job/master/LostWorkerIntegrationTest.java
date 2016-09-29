/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.master;

import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.job.SleepJobConfig;
import alluxio.job.SleepJobDefinition;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.util.CommonUtils;
import alluxio.worker.JobWorkerIdRegistry;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests that we properly handle worker heartbeat timeouts and reregistrations.
 */
public class LostWorkerIntegrationTest {
  private static final int WORKER_HEARTBEAT_TIMEOUT_MS = 10;

  @Rule
  public ManuallyScheduleHeartbeat mSchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.JOB_MASTER_LOST_WORKER_DETECTION,
      HeartbeatContext.JOB_WORKER_COMMAND_HANDLING);

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.JOB_MASTER_WORKER_TIMEOUT_MS, Integer.toString(WORKER_HEARTBEAT_TIMEOUT_MS)));

  // We need this because LocalAlluxioJobCluster doesn't work without it.
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Rule
  public JobDefinitionRegistryRule mJobRegistry = new JobDefinitionRegistryRule(
      SleepJobConfig.class, new SleepJobDefinition());

  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
  }

  @Test
  public void lostWorkerReregisters() throws Exception {
    Long initialId = JobWorkerIdRegistry.getWorkerId();
    // Sleep so that the master things the worker has gone too long without a heartbeat.
    CommonUtils.sleepMs(WORKER_HEARTBEAT_TIMEOUT_MS + 1);
    HeartbeatScheduler.execute(HeartbeatContext.JOB_MASTER_LOST_WORKER_DETECTION);
    assertTrue(mLocalAlluxioJobCluster.getMaster().getJobMaster().getWorkerInfoList().isEmpty());

    // Reregister the worker.
    HeartbeatScheduler.execute(HeartbeatContext.JOB_WORKER_COMMAND_HANDLING);
    CommonUtils.waitFor("worker to reregister", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return !mLocalAlluxioJobCluster.getMaster().getJobMaster().getWorkerInfoList().isEmpty();
      }
    }, 10 * Constants.SECOND_MS);
    assertTrue(JobWorkerIdRegistry.getWorkerId() != initialId);
  }
}
