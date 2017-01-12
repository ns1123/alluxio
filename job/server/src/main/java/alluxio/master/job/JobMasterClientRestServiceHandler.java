/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.Constants;
import alluxio.RestUtils;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.web.JobMasterWebServer;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.annotation.JacksonFeatures;

import java.util.List;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * The REST service handler for job master.
 */
@Path(ServiceConstants.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class JobMasterClientRestServiceHandler {
  private final JobMaster mJobMaster;

  /**
   * Creates a new instance of {@link JobMasterClientRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public JobMasterClientRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mJobMaster = (JobMaster) context
        .getAttribute(JobMasterWebServer.JOB_MASTER_SERVLET_RESOURCE_KEY);

  }

  /**
   * @return the service name
   */
  @GET
  @Path(ServiceConstants.SERVICE_NAME)
  public Response getServiceName() {
    // Need to encode the string as JSON because Jackson will not do it automatically.
    return RestUtils.call(new RestUtils.RestCallable<String>() {
      @Override
      public String call() throws Exception {
        return Constants.JOB_MASTER_CLIENT_SERVICE_NAME;
      }
    });
  }

  /**
   * @return the service version
   */
  @GET
  @Path(ServiceConstants.SERVICE_VERSION)
  public Response getServiceVersion() {
    return RestUtils.call(new RestUtils.RestCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return Constants.JOB_MASTER_CLIENT_SERVICE_VERSION;
      }
    });
  }

  /**
   * Runs a job.
   *
   * @param jobConfig the configuration of the job
   * @return the job id that tracks the job
   */
  @POST
  @Path(ServiceConstants.RUN_JOB)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response runJob(final JobConfig jobConfig) {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mJobMaster.runJob(jobConfig);
      }
    });
  }

  /**
   * Cancels a job.
   *
   * @param jobId the id of the job to cancel
   * @return the response
   */
  @POST
  @Path(ServiceConstants.CANCEL_JOB)
  public Response cancelJob(@QueryParam("jobId") final long jobId) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        mJobMaster.cancelJob(jobId);
        return null;
      }
    });
  }

  /**
   * Lists all the jobs in the history.
   *
   * @return the response of the names of all the jobs
   */
  @GET
  @Path(ServiceConstants.LIST)
  public Response listJobs() {
    return RestUtils.call(new RestUtils.RestCallable<List<Long>>() {
      @Override
      public List<Long> call() throws Exception {
        return mJobMaster.listJobs();
      }
    });
  }

  /**
   * Lists the status of a job.
   *
   * @param jobId the job id
   * @return the response of the job status
   */
  @GET
  @Path(ServiceConstants.LIST_STATUS)
  @JacksonFeatures(serializationEnable = {SerializationFeature.INDENT_OUTPUT})
  public Response listJobStatus(@QueryParam("jobId") final long jobId) {
    return RestUtils.call(new RestUtils.RestCallable<JobInfo>() {
      @Override
      public JobInfo call() throws Exception {
        return new JobInfo(mJobMaster.getJobInfo(jobId));
      }
    });
  }
}
