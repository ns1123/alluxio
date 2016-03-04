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
import alluxio.job.JobConfig;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.wire.JobInfo;
import alluxio.master.AlluxioMaster;
import alluxio.master.Master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * The REST service handler for job mananager.
 */
@Path(JobManagerClientRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class JobManagerClientRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static final String SERVICE_PREFIX = "job";
  public static final String SERVICE_NAME = "job/service_name";
  public static final String SERVICE_VERSION = "job/service_version";
  public static final String CANCEL_JOB = "job/cancel";
  public static final String LIST = "job/list";
  public static final String LIST_STATUS = "job/list_status";
  public static final String RUN_JOB = "job/run";

  private JobManagerMaster mJobManagerMaster = getJobManagerMaster();

  private JobManagerMaster getJobManagerMaster() {
    for (Master master : AlluxioMaster.get().getAdditionalMasters()) {
      if (master instanceof JobManagerMaster) {
        return (JobManagerMaster) master;
      }
    }
    LOG.error("JobManagerMaster is not registerd in Alluxio Master");
    return null;
  }

  /**
   * @return the service name
   */
  @GET
  @Path(SERVICE_NAME)
  public Response getServiceName() {
    return Response.ok(Constants.JOB_MANAGER_MASTER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the service version
   */
  @GET
  @Path(SERVICE_VERSION)
  public Response getServiceVersion() {
    return Response.ok(Constants.JOB_MANAGER_MASTER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * Runs a job.
   *
   * @param jobConfig the configuration of the job
   * @return the job id that tracks the job
   */
  @POST
  @Path(RUN_JOB)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response runJob(JobConfig jobConfig) {
    long jobId;
    try {
      jobId = mJobManagerMaster.runJob(jobConfig);
    } catch (JobDoesNotExistException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
    return Response.ok(jobId).build();
  }

  /**
   * Cancels a job.
   *
   * @param jobId the id of the job to cancel
   * @return the response
   */
  @PUT
  @Path(CANCEL_JOB)
  public Response cancelJob(@QueryParam("jobId") long jobId) {
    try {
      mJobManagerMaster.cancelJob(jobId);
    } catch (JobDoesNotExistException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
    return Response.ok().build();
  }

  /**
   * Lists all the jobs in the history.
   *
   * @return the response of the names of all the jobs
   */
  @GET
  @Path(LIST)
  public Response listJobs() {
    return Response.ok(mJobManagerMaster.listJobs()).build();
  }

  /**
   * Lists the status of a job.
   *
   * @param jobId the job id
   * @return the response of the job status
   */
  @GET
  @Path(LIST_STATUS)
  public Response listJobStatus(@QueryParam("jobId") long jobId) {
    return Response.ok(new JobInfo(mJobManagerMaster.getJobInfo(jobId))).build();
  }
}
