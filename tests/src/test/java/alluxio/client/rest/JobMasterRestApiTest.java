/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.client.rest;

import alluxio.ConfigurationTestUtils;
import alluxio.master.AlluxioJobMasterRestServiceHandler;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.security.LoginUserTestUtils;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Tests for {@link AlluxioJobMasterRestServiceHandler}.
 */
public final class JobMasterRestApiTest extends RestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private LocalAlluxioJobCluster mJobCluster;

  @Before
  public void before() throws Exception {
    mJobCluster = new LocalAlluxioJobCluster();
    mJobCluster.start();
    mHostname = mJobCluster.getHostname();
    mPort = mJobCluster.getMaster().getWebAddress().getPort();
    mServicePrefix = AlluxioJobMasterRestServiceHandler.SERVICE_PREFIX;
  }

  @After
  public void after() throws Exception {
    mJobCluster.stop();
    LoginUserTestUtils.resetLoginUser();
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void getInfo() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(AlluxioJobMasterRestServiceHandler.GET_INFO),
        NO_PARAMS, HttpMethod.GET, null).call();
  }
}
