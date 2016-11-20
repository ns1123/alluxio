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
import alluxio.job.load.LoadConfig;
import alluxio.master.AlluxioJobMaster;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.meta.JobInfo;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.rest.TestCaseOptions;
import alluxio.security.LoginUserTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Tests {@link JobMasterClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobMaster.class)
@PowerMockIgnore("javax.net.ssl.*")
public final class JobMasterClientRestApiTest extends RestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private LocalAlluxioJobCluster mJobCluster;
  private JobMaster mJobMaster;

  @Before
  public void before() throws Exception {
    mJobCluster = new LocalAlluxioJobCluster();
    mJobCluster.start();
    mJobMaster = PowerMockito.mock(JobMaster.class);
    // Replace the job master created by LocalAlluxioJobCluster with a mock.
    AlluxioJobMaster alluxioJobMaster = mJobCluster.getMaster();
    Whitebox.setInternalState(alluxioJobMaster, "mJobMaster", mJobMaster);
    mHostname = mJobCluster.getHostname();
    mPort = mJobCluster.getMaster().getWebLocalPort();
    mServicePrefix = JobMasterClientRestServiceHandler.SERVICE_PREFIX;
  }

  @After
  public void after() throws Exception {
    mJobCluster.stop();
    LoginUserTestUtils.resetLoginUser();
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void serviceNameTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(JobMasterClientRestServiceHandler.SERVICE_NAME),
        NO_PARAMS, HttpMethod.GET, Constants.JOB_MASTER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(JobMasterClientRestServiceHandler.SERVICE_VERSION),
        NO_PARAMS, HttpMethod.GET, Constants.JOB_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void runJobTest() throws Exception {
    LoadConfig config = new LoadConfig("/test", null);
    String jsonString = new ObjectMapper().writeValueAsString(config);

    long jobId = 1;
    Mockito.when(mJobMaster.runJob(config)).thenReturn(jobId);

    TestCaseOptions options = TestCaseOptions.defaults().setJsonString(jsonString);
    new TestCase(mHostname, mPort, getEndpoint(JobMasterClientRestServiceHandler.RUN_JOB),
        NO_PARAMS, HttpMethod.POST, jobId, options).run();
  }

  @Test
  public void cancelJobTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    long jobId = 1;
    params.put("jobId", "1");

    new TestCase(mHostname, mPort, getEndpoint(JobMasterClientRestServiceHandler.CANCEL_JOB),
        params, HttpMethod.POST, null).run();

    Mockito.verify(mJobMaster).cancelJob(jobId);
  }

  @Test
  public void listJobsTest() throws Exception {
    List<Long> empty = Lists.newArrayList();

    new TestCase(mHostname, mPort, getEndpoint(JobMasterClientRestServiceHandler.LIST), NO_PARAMS,
        HttpMethod.GET, empty).run();

    Mockito.verify(mJobMaster).listJobs();
  }

  @Test
  public void listJobStatus() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    long jobId = 1L;
    params.put("jobId", "1");
    LoadConfig config = new LoadConfig("/test", null);
    JobInfo jobInfo = new JobInfo(jobId, "job", config);
    Mockito.when(mJobMaster.getJobInfo(jobId)).thenReturn(jobInfo);

    TestCaseOptions options = TestCaseOptions.defaults().setPrettyPrint(true);
    new TestCase(mHostname, mPort, getEndpoint(JobMasterClientRestServiceHandler.LIST_STATUS),
        params, HttpMethod.GET, new alluxio.job.wire.JobInfo(jobInfo), options).run();
  }
}
