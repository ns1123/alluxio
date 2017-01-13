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

package alluxio.master.job;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
<<<<<<< HEAD
import alluxio.job.load.LoadConfig;
import alluxio.job.meta.JobInfo;
import alluxio.master.AlluxioJobMasterService;
||||||| merged common ancestors
import alluxio.job.load.LoadConfig;
import alluxio.job.meta.JobInfo;
import alluxio.master.AlluxioJobMaster;
=======
import alluxio.job.JobConfig;
import alluxio.job.ServiceConstants;
import alluxio.job.SleepJobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
>>>>>>> enterprise-1.4
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.rest.TestCaseOptions;
import alluxio.security.LoginUserTestUtils;
import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Tests {@link JobMasterClientRestServiceHandler}.
 */
public final class JobMasterClientRestApiTest extends RestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private LocalAlluxioJobCluster mJobCluster;
  private JobMaster mJobMaster;

  @Before
  public void before() throws Exception {
    mJobCluster = new LocalAlluxioJobCluster();
    mJobCluster.start();
<<<<<<< HEAD
    mJobMaster = PowerMockito.mock(JobMaster.class);
    // Replace the job master created by LocalAlluxioJobCluster with a mock.
    AlluxioJobMasterService alluxioJobMaster = mJobCluster.getMaster();
    Whitebox.setInternalState(alluxioJobMaster, "mJobMaster", mJobMaster);
||||||| merged common ancestors
    mJobMaster = PowerMockito.mock(JobMaster.class);
    // Replace the job master created by LocalAlluxioJobCluster with a mock.
    AlluxioJobMaster alluxioJobMaster = mJobCluster.getMaster();
    Whitebox.setInternalState(alluxioJobMaster, "mJobMaster", mJobMaster);
=======
    mJobMaster = mJobCluster.getMaster().getJobMaster();
>>>>>>> enterprise-1.4
    mHostname = mJobCluster.getHostname();
<<<<<<< HEAD
    mPort = mJobCluster.getMaster().getWebAddress().getPort();
    mServicePrefix = ServiceConstants.SERVICE_PREFIX;
||||||| merged common ancestors
    mPort = mJobCluster.getMaster().getWebLocalPort();
    mServicePrefix = ServiceConstants.SERVICE_PREFIX;
=======
    mPort = mJobCluster.getMaster().getWebLocalPort();
    mServicePrefix = ServiceConstants.MASTER_SERVICE_PREFIX;
>>>>>>> enterprise-1.4
  }

  @After
  public void after() throws Exception {
    mJobCluster.stop();
    LoginUserTestUtils.resetLoginUser();
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void serviceName() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.SERVICE_NAME),
        NO_PARAMS, HttpMethod.GET, Constants.JOB_MASTER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersion() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.SERVICE_VERSION),
        NO_PARAMS, HttpMethod.GET, Constants.JOB_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
<<<<<<< HEAD
  public void runJob() throws Exception {
    LoadConfig config = new LoadConfig("/test", null);

    long jobId = 1;
    Mockito.when(mJobMaster.runJob(config)).thenReturn(jobId);

    TestCaseOptions options = TestCaseOptions.defaults().setBody(config);
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.RUN_JOB),
        NO_PARAMS, HttpMethod.POST, jobId, options).run();
||||||| merged common ancestors
  public void runJobTest() throws Exception {
    LoadConfig config = new LoadConfig("/test", null);

    long jobId = 1;
    Mockito.when(mJobMaster.runJob(config)).thenReturn(jobId);

    TestCaseOptions options = TestCaseOptions.defaults().setBody(config);
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.RUN_JOB),
        NO_PARAMS, HttpMethod.POST, jobId, options).run();
=======
  public void run() throws Exception {
    final long jobId = startJob(new SleepJobConfig(Constants.SECOND_MS));
    Assert.assertEquals(0, jobId);
    Assert.assertEquals(1, mJobMaster.list().size());
    waitForStatus(jobId, Status.COMPLETED);
>>>>>>> enterprise-1.4
  }

  @Test
<<<<<<< HEAD
  public void cancelJob() throws Exception {
||||||| merged common ancestors
  public void cancelJobTest() throws Exception {
=======
  public void cancel() throws Exception {
    long jobId = startJob(new SleepJobConfig(10 * Constants.SECOND_MS));
    // Sleep to make sure the run request and the cancel request are separated by a job worker
    // heartbeat. If not, job service will not handle that case correctly.
    CommonUtils.sleepMs(Constants.SECOND_MS);
>>>>>>> enterprise-1.4
    Map<String, String> params = Maps.newHashMap();
    params.put("jobId", Long.toString(jobId));
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.CANCEL),
        params, HttpMethod.POST, null).run();
    waitForStatus(jobId, Status.CANCELED);

  }

  @Test
<<<<<<< HEAD
  public void listJobs() throws Exception {
||||||| merged common ancestors
  public void listJobsTest() throws Exception {
=======
  public void list() throws Exception {
>>>>>>> enterprise-1.4
    List<Long> empty = Lists.newArrayList();
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.LIST), NO_PARAMS,
        HttpMethod.GET, empty).run();
  }

  @Test
  public void getStatus() throws Exception {
    JobConfig config = new SleepJobConfig(Constants.SECOND_MS);
    final long jobId = startJob(config);
    Map<String, String> params = Maps.newHashMap();
    params.put("jobId", Long.toString(jobId));

    TestCaseOptions options = TestCaseOptions.defaults().setPrettyPrint(true);
    String result = new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.GET_STATUS),
        params, HttpMethod.GET, null, options).call();
    JobInfo jobInfo = new ObjectMapper().readValue(result, JobInfo.class);
    Assert.assertEquals(jobId, jobInfo.getJobId());
    Assert.assertEquals(1, jobInfo.getTaskInfoList().size());
  }

  private int startJob(JobConfig config) throws Exception {
    TestCaseOptions options = TestCaseOptions.defaults().setBody(config);
    String result = new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.RUN),
        NO_PARAMS, HttpMethod.POST, null, options).call();
    return new ObjectMapper().readValue(result, Integer.TYPE);
  }

  private void waitForStatus(final long jobId, final Status status) {
    CommonUtils.waitFor("Waiting for job status", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        try {
          return mJobMaster.getStatus(jobId).getStatus() == status;
        } catch (Exception e) {
          Throwables.propagate(e);
        }
        return null;
      }
    }, 10 * Constants.SECOND_MS);
  }
}
