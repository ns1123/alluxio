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
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.wire.JobInfo;
import alluxio.master.AlluxioJobManagerMaster;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.annotation.JacksonFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * The REST service handler for job manager.
 */
@Path(JobManagerMasterClientRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class JobManagerMasterClientRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static final String SERVICE_PREFIX = "master/job";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String CANCEL_JOB = "cancel";
  public static final String LIST = "list";
  public static final String LIST_STATUS = "list_status";
  public static final String RUN_JOB = "run";

  private JobManagerMaster mJobManagerMaster = AlluxioJobManagerMaster.get().getJobManagerMaster();

  /**
   * @return the service name
   */
  @GET
  @Path(SERVICE_NAME)
  public Response getServiceName() {
    // Need to encode the string as JSON because Jackson will not do it automatically.
    return RestUtils.createResponse(Constants.JOB_MANAGER_MASTER_CLIENT_SERVICE_NAME);
  }

  /**
   * @return the service version
   */
  @GET
  @Path(SERVICE_VERSION)
  public Response getServiceVersion() {
    return RestUtils.createResponse(Constants.JOB_MANAGER_MASTER_CLIENT_SERVICE_VERSION);
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
      return RestUtils.createErrorResponse(e.getMessage());
    }
    return RestUtils.createResponse(jobId);
  }

  /**
   * Cancels a job.
   *
   * @param jobId the id of the job to cancel
   * @return the response
   */
  @POST
  @Path(CANCEL_JOB)
  public Response cancelJob(@QueryParam("jobId") long jobId) {
    try {
      mJobManagerMaster.cancelJob(jobId);
    } catch (JobDoesNotExistException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
    return RestUtils.createResponse();
  }

  /**
   * Lists all the jobs in the history.
   *
   * @return the response of the names of all the jobs
   */
  @GET
  @Path(LIST)
  public Response listJobs() {
    return RestUtils.createResponse(mJobManagerMaster.listJobs());
  }

  /**
   * Lists the status of a job.
   *
   * @param jobId the job id
   * @return the response of the job status
   */
  @GET
  @Path(LIST_STATUS)
  @JacksonFeatures(serializationEnable = {SerializationFeature.INDENT_OUTPUT})
  public Response listJobStatus(@QueryParam("jobId") long jobId) {
    try {
      return RestUtils.createResponse(new JobInfo(mJobManagerMaster.getJobInfo(jobId)));
    } catch (JobDoesNotExistException e) {
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }
}
