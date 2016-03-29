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
import alluxio.job.load.DistributedSingleFileLoadingConfig;
import alluxio.master.AlluxioMaster;
import alluxio.master.job.meta.JobInfo;
import alluxio.rest.TestCaseFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.Map;

/**
 * Tests {@link JobManagerClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobManagerMaster.class)
public class JobManagerMasterClientRestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private JobManagerMaster mJobManagerMaster;

  @Rule
  private LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  @Before
  public void beforeClass() {
    AlluxioMaster alluxioMaster = mResource.get().getMaster().getInternalMaster();
    mJobManagerMaster = PowerMockito.mock(JobManagerMaster.class);
    Whitebox.setInternalState(alluxioMaster, "mAdditionalMasters",
        Lists.newArrayList(mJobManagerMaster));
  }

  private String getEndpoint(String suffix) {
    return JobManagerClientRestServiceHandler.SERVICE_PREFIX + "/" + suffix;
  }

  @Test
  public void serviceNameTest() throws Exception {
    TestCaseFactory.newMasterTestCase(getEndpoint(JobManagerClientRestServiceHandler.SERVICE_NAME),
        NO_PARAMS, "GET", Constants.JOB_MANAGER_MASTER_CLIENT_SERVICE_NAME, mResource).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    TestCaseFactory
        .newMasterTestCase(getEndpoint(JobManagerClientRestServiceHandler.SERVICE_VERSION),
            NO_PARAMS, "GET", Constants.JOB_MANAGER_MASTER_CLIENT_SERVICE_VERSION, mResource)
        .run();
  }

  @Test
  public void runJobTest() throws Exception {
    DistributedSingleFileLoadingConfig config = new DistributedSingleFileLoadingConfig("/test");
    String jsonString = new ObjectMapper().writeValueAsString(config);

    long jobId = 1;
    Mockito.when(mJobManagerMaster.runJob(config)).thenReturn(jobId);

    TestCaseFactory.newMasterTestCase(getEndpoint(JobManagerClientRestServiceHandler.RUN_JOB),
        NO_PARAMS, "POST", jobId, mResource, jsonString).run();
  }

  @Test
  public void cancelJobTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    long jobId = 1;
    params.put("jobId", "1");
    TestCaseFactory.newMasterTestCase(getEndpoint(JobManagerClientRestServiceHandler.CANCEL_JOB),
        params, "POST", "", mResource).run();

    Mockito.verify(mJobManagerMaster).cancelJob(jobId);
  }

  @Test
  public void listJobsTest() throws Exception {
    TestCaseFactory.newMasterTestCase(getEndpoint(JobManagerClientRestServiceHandler.LIST),
        NO_PARAMS, "GET", "[]", mResource).run();
    Mockito.verify(mJobManagerMaster).listJobs();
  }

  @Test
  public void listJobStatus() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    long jobId = 1L;
    params.put("jobId", "1");
    DistributedSingleFileLoadingConfig config = new DistributedSingleFileLoadingConfig("/test");
    JobInfo jobInfo = new JobInfo(jobId, "job", config);
    Mockito.when(mJobManagerMaster.getJobInfo(jobId)).thenReturn(jobInfo);
    TestCaseFactory.newMasterTestCase(getEndpoint(JobManagerClientRestServiceHandler.LIST_STATUS),
        params, "GET", new alluxio.job.wire.JobInfo(jobInfo), mResource).run();
  }
}
