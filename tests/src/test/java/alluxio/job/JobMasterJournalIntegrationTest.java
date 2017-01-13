/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;

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
    long jobId = mJobMaster.run(new SleepJobConfig(Constants.MINUTE_MS));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.RUNNING);
    mJobMaster.cancel(jobId);
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.CANCELED);
    JobInfo jobInfo = mJobMaster.getStatus(jobId);
    Assert.assertEquals(Status.CANCELED, jobInfo.getStatus());
    mJobMaster.stop();
    mJobMaster.start(true);
    jobInfo = mJobMaster.getStatus(jobId);
    Assert.assertEquals(Status.CANCELED, jobInfo.getStatus());
  }

  @Test
  public void journalIncompleteJob() throws Exception {
    long jobId = mJobMaster.run(new SleepJobConfig(Constants.MINUTE_MS));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.RUNNING);
    mJobMaster.stop();
    mJobMaster.start(true);
    JobInfo jobInfo = mJobMaster.getStatus(jobId);
    Assert.assertEquals(Status.FAILED, jobInfo.getStatus());
    Assert.assertEquals("Job failed: Job master shut down during execution",
        jobInfo.getErrorMessage());
  }

  @Test
  public void journalOverMultipleRestarts() throws Exception {
    long jobId = mJobMaster.run(new SleepJobConfig(1));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.COMPLETED);
    mJobMaster.stop();
    mJobMaster.start(true);
    mJobMaster.stop();
    mJobMaster.start(true);
    JobInfo jobInfo = mJobMaster.getStatus(jobId);
    Assert.assertEquals(Status.COMPLETED, jobInfo.getStatus());
  }

  @Test
  public void avoidJobIdReuseOnRestart() throws Exception {
    long jobId1 = mJobMaster.run(new SleepJobConfig(1));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId1, Status.COMPLETED);
    mJobMaster.stop();
    mJobMaster.start(true);
    long jobId2 = mJobMaster.run(new SleepJobConfig(1));
    Assert.assertNotEquals(jobId1, jobId2);
  }
}
