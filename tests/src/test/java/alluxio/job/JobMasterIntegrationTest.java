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

import static org.junit.Assert.assertTrue;

import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.util.CommonUtils;
import alluxio.worker.AlluxioJobWorkerService;

import com.google.common.base.Function;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests for the job master.
 */
public final class JobMasterIntegrationTest {
  private static final long WORKER_TIMEOUT = 500;
  private JobMaster mJobMaster;
  private AlluxioJobWorkerService mJobWorker;
  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL_MS, 20)
          .setProperty(PropertyKey.JOB_MASTER_WORKER_TIMEOUT_MS, WORKER_TIMEOUT)
          .setProperty(PropertyKey.JOB_MASTER_LOST_WORKER_INTERVAL_MS, WORKER_TIMEOUT)
          .build();

  @Rule
  public JobDefinitionRegistryRule mJobRule = new JobDefinitionRegistryRule(SleepJobConfig.class,
      new SleepJobDefinition());

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mJobMaster = mLocalAlluxioJobCluster.getMaster().getJobMaster();
    mJobWorker = mLocalAlluxioJobCluster.getWorker();
  }

  @Test
  public void restartAndLoseWorker() throws Exception {
    long jobId = mJobMaster.run(new SleepJobConfig(1));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.COMPLETED);
    mJobMaster.stop();
    mJobMaster.start(true);
    CommonUtils.waitFor("Worker to register with restarted job master", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return !mJobMaster.getWorkerInfoList().isEmpty();
      }
    });
    mJobWorker.stop();
    CommonUtils.sleepMs(2 * WORKER_TIMEOUT);
    assertTrue(mJobMaster.getWorkerInfoList().isEmpty());
  }
}
