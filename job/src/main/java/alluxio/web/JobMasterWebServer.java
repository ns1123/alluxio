/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.web;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;

import java.net.InetSocketAddress;
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Job master web server.
 */
@NotThreadSafe
public final class JobMasterWebServer extends UIWebServer {

  /**
   * Creates a new instance of {@link JobMasterWebServer}. It pairs URLs with servlets and sets
   * the webapp folder.
   *
   * @param service name of the web service
   * @param address address of the server
   */
  public JobMasterWebServer(NetworkAddressUtils.ServiceType service, InetSocketAddress address) {
    super(service, address);

    // REST configuration
    mWebAppContext.setOverrideDescriptors(Arrays.asList(Configuration.get(PropertyKey.WEB_RESOURCES)
        + "/WEB-INF/job_master.xml"));
  }
}
