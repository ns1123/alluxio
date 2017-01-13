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
<<<<<<< HEAD
import alluxio.master.AlluxioJobMasterService;
||||||| merged common ancestors
=======
import alluxio.master.job.JobMaster;
>>>>>>> enterprise-1.4
import alluxio.util.io.PathUtils;

import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletException;

/**
 * Job master web server.
 */
@NotThreadSafe
public final class JobMasterWebServer extends WebServer {

<<<<<<< HEAD
  public static final String ALLUXIO_JOB_MASTER_SERVLET_RESOURCE_KEY = "Alluxio Job Master";

||||||| merged common ancestors
=======
  public static final String JOB_MASTER_SERVLET_RESOURCE_KEY = "Job Master";

>>>>>>> enterprise-1.4
  /**
   * Creates a new instance of {@link JobMasterWebServer}. It pairs URLs with servlets.
   *
   * @param serviceName name of the web service
   * @param address address of the server
   * @param jobMaster the job master
   */
<<<<<<< HEAD
  public JobMasterWebServer(String serviceName, InetSocketAddress address,
      final AlluxioJobMasterService jobMaster) {
||||||| merged common ancestors
  public JobMasterWebServer(String serviceName, InetSocketAddress address) {
=======
  public JobMasterWebServer(String serviceName, InetSocketAddress address, final JobMaster jobMaster) {
>>>>>>> enterprise-1.4
    super(serviceName, address);

    // REST configuration
<<<<<<< HEAD
    ResourceConfig config = new ResourceConfig().packages("alluxio.master", "alluxio.master.job");
    // Override the init method to inject a reference to AlluxioJobMaster into the servlet context.
    // ServletContext may not be modified until after super.init() is called.
    ServletContainer servlet = new ServletContainer(config) {
      private static final long serialVersionUID = 7756010860672831556L;

      @Override
      public void init() throws ServletException {
        super.init();
        getServletContext().setAttribute(ALLUXIO_JOB_MASTER_SERVLET_RESOURCE_KEY, jobMaster);
      }
    };
||||||| merged common ancestors
    ResourceConfig config = new ResourceConfig().packages("alluxio.master.job");
    ServletContainer servlet = new ServletContainer(config);
=======
    ResourceConfig config = new ResourceConfig().packages("alluxio.master.job");
    // Override the init method to inject a reference to JobMaster into the servlet context.
    // ServletContext may not be modified until after super.init() is called.
    ServletContainer servlet = new ServletContainer(config) {
      private static final long serialVersionUID = 7756010860672831556L;

      @Override
      public void init() throws ServletException {
        super.init();
        getServletContext().setAttribute(JOB_MASTER_SERVLET_RESOURCE_KEY, jobMaster);
      }
    };
>>>>>>> enterprise-1.4

    ServletHolder servletHolder = new ServletHolder("Alluxio Job Master Web Service", servlet);
    mWebAppContext.addServlet(servletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));
  }
}
