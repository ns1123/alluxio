/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.util;

import alluxio.Constants;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.job.JobMasterClientRestServiceHandler;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utils for interacting with the job service through a REST client.
 */
public final class JobRestClientUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Runs the job specified by the config.
   *
   * @param config a {@link JobConfig} describing the job to run
   * @return the job ID for the created job
   */
  public static long runJob(JobConfig config) {
    String payload;
    try {
      payload = new ObjectMapper().writeValueAsString(config);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    HttpURLConnection connection = null;
    try (Closer closer = Closer.create()) {
      URL url = new URL(
          getJobServiceBaseURL().toString() + "/" + JobMasterClientRestServiceHandler.RUN_JOB);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("Content-Length", Integer.toString(payload.length()));
      connection.setRequestProperty("Content-Language", "en-US");
      connection.setUseCaches(false);
      connection.setDoOutput(true);
      DataOutputStream outputStream =
          closer.register(new DataOutputStream(connection.getOutputStream()));
      outputStream.writeChars(payload);
      if (connection.getResponseCode() < 300) {
        InputStream is = closer.register(connection.getInputStream());
        String response = IOUtils.toString(is);
        try {
          return Long.parseLong(response);
        } catch (NumberFormatException e) {
          throw new RuntimeException("Job submission failed. Response: " + response);
        }
      } else {
        InputStream es = closer.register(connection.getErrorStream());
        throw new RuntimeException("Job submission failed. Response: " + IOUtils.toString(es));
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  /**
   * Runs the specified job and waits for it to finish, throwing an exception if the job fails.
   *
   * @param config configuration for the job to run
   */
  public static void runAndWaitForJob(JobConfig config) {
    runAndWaitForJob(config, 1);
  }

  /**
   * Runs the specified job and waits for it to finish, throwing an exception if the job fails after
   * the specified number of attempts have failed to complete.
   *
   * @param config configuration for the job to run
   * @param attempts number of times to try running the job before giving up
   */
  public static void runAndWaitForJob(JobConfig config, int attempts) {
    long completedAttempts = 0;
    while (true) {
      long jobId = runJob(config);
      JobInfo jobInfo = waitForJobResult(jobId);
      if (jobInfo.getStatus() == Status.COMPLETED) {
        return;
      }
      completedAttempts++;
      if (completedAttempts >= attempts) {
        throw new RuntimeException(
            "Failed to successfully complete job: " + jobInfo.getErrorMessage());
      }
      LOG.warn("Job {} failed to complete. Error message: {}", jobId, jobInfo.getErrorMessage());
    }
  }

  /**
   * @param jobId the ID of the job to wait for
   * @return the job info for the job once it finishes
   */
  private static JobInfo waitForJobResult(final long jobId) {
    final AtomicReference<JobInfo> finishedJobInfo = new AtomicReference<>();
    CommonUtils.waitFor("Job to finish", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        JobInfo jobInfo = getJobInfo(jobId);
        switch (jobInfo.getStatus()) {
          case FAILED:
          case CANCELED:
          case COMPLETED:
            finishedJobInfo.set(jobInfo);
            return true;
          case RUNNING:
          case CREATED:
            return false;
          default:
            throw new IllegalStateException("Unrecognized job status: " + jobInfo.getStatus());
        }
      }
    });
    return finishedJobInfo.get();
  }

  /**
   * @param jobId the ID for the job to query
   * @return JobInfo describing the job
   */
  private static JobInfo getJobInfo(long jobId) {
    HttpURLConnection connection = null;
    try (Closer closer = Closer.create()) {
      URL url = new URL(getJobServiceBaseURL().toString() + "/"
          + JobMasterClientRestServiceHandler.LIST_STATUS + "?jobId=" + jobId);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.setUseCaches(false);
      connection.setDoOutput(true);
      if (connection.getResponseCode() < 300) {
        InputStream is = closer.register(connection.getInputStream());
        JobInfo jobInfo = new ObjectMapper().readValue(is, JobInfo.class);
        return jobInfo;
      } else {
        InputStream es = closer.register(connection.getErrorStream());
        throw new RuntimeException("Job submission failed. Response: " + IOUtils.toString(es));
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  /**
   * @return the base URL for the job service
   */
  private static URL getJobServiceBaseURL() {
    NetworkAddressUtils.ServiceType service = NetworkAddressUtils.ServiceType.JOB_MASTER_WEB;
    String host = NetworkAddressUtils.getConnectHost(service);
    int port = NetworkAddressUtils.getPort(service);
    try {
      return new URL("http://" + host + ":" + port + Constants.REST_API_PREFIX + "/"
          + JobMasterClientRestServiceHandler.SERVICE_PREFIX);
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
  }

  private JobRestClientUtils() {} // Not intended for instantiation.
}
