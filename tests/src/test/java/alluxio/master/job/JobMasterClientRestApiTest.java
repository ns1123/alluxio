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

package alluxio.master.job;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.job.load.LoadConfig;
import alluxio.master.AlluxioMaster;
import alluxio.master.Master;
import alluxio.master.job.meta.JobInfo;
import alluxio.rest.TestCaseFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;

/**
 * Tests {@link JobMasterClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobMaster.class)
@Ignore // TODO(jiri): Remove when Alluxio job cluster resource is created
public final class JobMasterClientRestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private JobMaster mJobMaster;

  @Rule
  private LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  @Before
  public void before() throws Exception {
    AlluxioMaster alluxioMaster = mResource.get().getMaster().getInternalMaster();
    mJobMaster = PowerMockito.mock(JobMaster.class);
    // Replace the job manager master created by LocalAlluxioClusterResource with a mock.
    List<Master> additionalMasters = Whitebox.getInternalState(alluxioMaster, "mAdditionalMasters");
    Assert.assertEquals(1, additionalMasters.size());
    additionalMasters.get(0).stop();
    Whitebox.setInternalState(alluxioMaster, "mAdditionalMasters",
        Lists.newArrayList(mJobMaster));
  }

  private String getEndpoint(String suffix) {
    return JobMasterClientRestServiceHandler.SERVICE_PREFIX + "/" + suffix;
  }

  @Test
  public void serviceNameTest() throws Exception {
    TestCaseFactory.newMasterTestCase(getEndpoint(JobMasterClientRestServiceHandler.SERVICE_NAME),
        NO_PARAMS, "GET", Constants.JOB_MASTER_CLIENT_SERVICE_NAME, mResource).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    TestCaseFactory
        .newMasterTestCase(getEndpoint(JobMasterClientRestServiceHandler.SERVICE_VERSION),
            NO_PARAMS, "GET", Constants.JOB_MASTER_CLIENT_SERVICE_VERSION, mResource).run();
  }

  @Test
  public void runJobTest() throws Exception {
    LoadConfig config = new LoadConfig("/test");
    String jsonString = new ObjectMapper().writeValueAsString(config);

    long jobId = 1;
    Mockito.when(mJobMaster.runJob(config)).thenReturn(jobId);

    TestCaseFactory.newMasterTestCase(getEndpoint(JobMasterClientRestServiceHandler.RUN_JOB),
        NO_PARAMS, "POST", jobId, mResource, jsonString, false).run();
  }

  @Test
  public void cancelJobTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    long jobId = 1;
    params.put("jobId", "1");
    TestCaseFactory.newMasterTestCase(getEndpoint(JobMasterClientRestServiceHandler.CANCEL_JOB),
        params, "POST", null, mResource).run();

    Mockito.verify(mJobMaster).cancelJob(jobId);
  }

  @Test
  public void listJobsTest() throws Exception {
    List<Long> empty = Lists.newArrayList();
    TestCaseFactory.newMasterTestCase(getEndpoint(JobMasterClientRestServiceHandler.LIST),
        NO_PARAMS, "GET", empty, mResource).run();
    Mockito.verify(mJobMaster).listJobs();
  }

  @Test
  public void listJobStatus() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    long jobId = 1L;
    params.put("jobId", "1");
    LoadConfig config = new LoadConfig("/test");
    JobInfo jobInfo = new JobInfo(jobId, "job", config);
    Mockito.when(mJobMaster.getJobInfo(jobId)).thenReturn(jobInfo);
    TestCaseFactory.newMasterTestCase(getEndpoint(JobMasterClientRestServiceHandler.LIST_STATUS),
        params, "GET", new alluxio.job.wire.JobInfo(jobInfo), mResource, null, true).run();
  }
}
