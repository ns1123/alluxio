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

package alluxio.job.client;

import static org.junit.Assert.assertEquals;

import alluxio.LocalAlluxioClusterResource;
import alluxio.job.wire.JobMasterInfo;
import alluxio.job.wire.JobMasterInfo.JobMasterInfoField;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.worker.job.MetaJobMasterClient;
import alluxio.worker.job.RetryHandlingMetaJobMasterClient;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Integration tests for the meta job master service.
 */
public final class MetaJobMasterIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource.Builder().build();

  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
  }

  @Test
  public void getInfoAllFields() throws Exception {
    try (MetaJobMasterClient client = new RetryHandlingMetaJobMasterClient(
        mLocalAlluxioJobCluster.getMaster().getMasterAddress())) {
      int webPort = mLocalAlluxioJobCluster.getMaster().getWebLocalPort();
      JobMasterInfo info = client.getInfo(null);
      assertEquals(webPort, info.getWebPort());
    }
  }

  @Test
  public void getInfoWebPort() throws Exception {
    try (MetaJobMasterClient client = new RetryHandlingMetaJobMasterClient(
        mLocalAlluxioJobCluster.getMaster().getMasterAddress())) {
      int webPort = mLocalAlluxioJobCluster.getMaster().getWebLocalPort();
      JobMasterInfo info =
          client.getInfo(new HashSet<>(Arrays.asList(JobMasterInfoField.WEB_PORT)));
      assertEquals(webPort, info.getWebPort());
    }
  }
}
