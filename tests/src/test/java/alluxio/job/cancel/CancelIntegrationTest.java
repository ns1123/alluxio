/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job.cancel;

import alluxio.Constants;
import alluxio.job.JobConfig;
import alluxio.job.JobDefinition;
import alluxio.job.JobDefinitionRegistry;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.persist.JobIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import org.junit.Test;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests the cancellation of a job.
 */
public final class CancelIntegrationTest extends JobIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  static class CancelTestConfig implements JobConfig {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
      return "Cancel";
    }
  }

  class CancelTestDefinition implements JobDefinition<CancelTestConfig, Integer> {
    @Override
    public Map<WorkerInfo, Integer> selectExecutors(CancelTestConfig config,
        List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) throws Exception {
      Map<WorkerInfo, Integer> result = new HashMap<>();
      for (WorkerInfo info : workerInfoList) {
        result.put(info, 0);
      }
      return result;
    }

    @Override
    public void runTask(CancelTestConfig config, Integer args, JobWorkerContext jobWorkerContext)
        throws Exception {
      // wait until interruption
      CommonUtils.sleepMs(LOG, 10 * Constants.SECOND_MS, true);
      throw new InterruptedException();
    }
  }

  @Test(timeout = 10000)
  public void cancelTest() throws Exception {
    // register the job
    Whitebox.invokeMethod(JobDefinitionRegistry.INSTANCE, "add", CancelTestConfig.class,
        new CancelTestDefinition());
    long jobId = mJobMaster.runJob(new CancelTestConfig());
    waitForJobRunning(jobId);
    // cancel the job
    mJobMaster.cancelJob(jobId);
    waitForJobCancelled(jobId);
  }
}
