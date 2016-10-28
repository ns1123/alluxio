/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.job.JobDefinitionRegistryRule;
import alluxio.job.SleepJobConfig;
import alluxio.job.SleepJobDefinition;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.Status;
import alluxio.master.job.JobMaster;
import alluxio.master.job.meta.JobInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for job master journaling.
 */
public final class JobMasterJournalIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
      .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL_MS, 20)
      .build();

  @Rule
  public JobDefinitionRegistryRule mRegistryRule =
      new JobDefinitionRegistryRule(SleepJobConfig.class, new SleepJobDefinition());

  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;
  private JobMaster mJobMaster;

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mJobMaster = mLocalAlluxioJobCluster.getMaster().getJobMaster();
  }

  @After
  public void after() throws Exception {
    mLocalAlluxioJobCluster.stop();
  }

  @Test
  public void journalCompleteJob() throws Exception {
    long jobId = mJobMaster.runJob(new SleepJobConfig(Constants.MINUTE_MS));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.RUNNING);
    mJobMaster.cancelJob(jobId);
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.CANCELED);
    JobInfo jobInfo = mJobMaster.getJobInfo(jobId);
    Assert.assertEquals(Status.CANCELED, jobInfo.getStatus());
    mJobMaster.stop();
    mJobMaster.start(true);
    jobInfo = mJobMaster.getJobInfo(jobId);
    Assert.assertEquals(Status.CANCELED, jobInfo.getStatus());
  }

  @Test
  public void journalIncompleteJob() throws Exception {
    long jobId = mJobMaster.runJob(new SleepJobConfig(Constants.MINUTE_MS));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.RUNNING);
    mJobMaster.stop();
    mJobMaster.start(true);
    JobInfo jobInfo = mJobMaster.getJobInfo(jobId);
    Assert.assertEquals(Status.FAILED, jobInfo.getStatus());
    Assert.assertEquals("Job failed: Job master shut down during execution", jobInfo.getErrorMessage());
  }

  @Test
  public void journalOverMultipleRestarts() throws Exception {
    long jobId = mJobMaster.runJob(new SleepJobConfig(1));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.COMPLETED);
    mJobMaster.stop();
    mJobMaster.start(true);
    mJobMaster.stop();
    mJobMaster.start(true);
    JobInfo jobInfo = mJobMaster.getJobInfo(jobId);
    Assert.assertEquals(Status.COMPLETED, jobInfo.getStatus());
  }
}
