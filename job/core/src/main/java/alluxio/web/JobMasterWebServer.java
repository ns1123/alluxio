/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.web;

import alluxio.Constants;
import alluxio.util.io.PathUtils;

import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.net.InetSocketAddress;

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
   * @param serviceName name of the web service
   * @param address address of the server
   */
  public JobMasterWebServer(String serviceName, InetSocketAddress address) {
    super(serviceName, address);

    // REST configuration
    ResourceConfig config = new ResourceConfig().packages("alluxio.master.job");
    ServletContainer servlet = new ServletContainer(config);

    ServletHolder servletHolder = new ServletHolder("Alluxio Job Master Web Service", servlet);
    mWebAppContext.addServlet(servletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));
  }
}
