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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.WorkerInfo;
import alluxio.worker.AlluxioJobWorkerService;

import com.google.common.base.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for the job master.
 */
public final class JobMasterIntegrationTest {
  private static final long WORKER_TIMEOUT_MS = 500;
  private static final long LOST_WORKER_INTERVAL_MS = 500;
  private JobMaster mJobMaster;
  private AlluxioJobWorkerService mJobWorker;
  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL_MS, 20)
          .setProperty(PropertyKey.JOB_MASTER_WORKER_TIMEOUT_MS, WORKER_TIMEOUT_MS)
          .setProperty(PropertyKey.JOB_MASTER_LOST_WORKER_INTERVAL_MS, LOST_WORKER_INTERVAL_MS)
          .build();

  @Rule
  public JobDefinitionRegistryRule mJobRule =
      new JobDefinitionRegistryRule(SleepJobConfig.class, new SleepJobDefinition());

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mJobMaster = mLocalAlluxioJobCluster.getMaster().getJobMaster();
    mJobWorker = mLocalAlluxioJobCluster.getWorker();
  }

  @After
  public void after() throws Exception {
    mLocalAlluxioJobCluster.stop();
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.JOB_MASTER_CACHE_CAPACITY, "1",
      PropertyKey.Name.JOB_MASTER_CACHE_TIMEOUT_MS, "0"})
  public void flowControl() throws Exception {
    for (int i = 0; i < 10; i++) {
      while (true) {
        try {
          mJobMaster.run(new SleepJobConfig(100));
          break;
        } catch (JobDoesNotExistException e) {
          // sleep for a little before retrying the job
          CommonUtils.sleepMs(100);
        }
      }
    }
  }

  @Test
  public void restartMasterAndLoseWorker() throws Exception {
    long jobId = mJobMaster.run(new SleepJobConfig(1));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.COMPLETED);
    mJobMaster.stop();
    mJobMaster.start(true);
    CommonUtils.waitFor("Worker to register with restarted job master",
        new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            return !mJobMaster.getWorkerInfoList().isEmpty();
          }
        }, WaitForOptions.defaults().setTimeout(10 * Constants.SECOND_MS));
    mJobWorker.stop();
    CommonUtils.sleepMs(WORKER_TIMEOUT_MS + LOST_WORKER_INTERVAL_MS);
    assertTrue(mJobMaster.getWorkerInfoList().isEmpty());
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.JOB_MASTER_LOST_WORKER_INTERVAL_MS, "10000000"})
  public void restartMasterAndReregisterWorker() throws Exception {
    long jobId = mJobMaster.run(new SleepJobConfig(1));
    JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.COMPLETED);
    mJobMaster.stop();
    mJobMaster.start(true);
    CommonUtils.waitFor("Worker to register with restarted job master",
        new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            return !mJobMaster.getWorkerInfoList().isEmpty();
          }
        }, WaitForOptions.defaults().setTimeout(10 * Constants.SECOND_MS));
    final long firstWorkerId = mJobMaster.getWorkerInfoList().get(0).getId();
    mLocalAlluxioJobCluster.restartWorker();
    CommonUtils.waitFor("Restarted worker to register with job master",
        new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            List<WorkerInfo> workerInfo = mJobMaster.getWorkerInfoList();
            return !workerInfo.isEmpty() && workerInfo.get(0).getId() != firstWorkerId;
          }
        }, WaitForOptions.defaults().setTimeout(10 * Constants.SECOND_MS));
    // The restarted worker should replace the original worker since they have the same address.
    assertEquals(1, mJobMaster.getWorkerInfoList().size());
  }
}
