/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.util;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.ConnectionFailedException;
import alluxio.job.JobConfig;
import alluxio.job.RetryHandlingMetaJobMasterClient;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobMasterInfo.JobMasterInfoField;
import alluxio.job.wire.Status;
import alluxio.master.job.ServiceContants;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

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
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
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
      URL url = getRunJobURL();
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
   * Convenience method for calling {@link #createProgressThread(long, PrintStream)} with an
   * interval of 2 seconds.
   *
   * @param stream the print stream to write to
   * @return the thread
   */
  public static Thread createProgressThread(final PrintStream stream) {
    return createProgressThread(2 * Constants.SECOND_MS, stream);
  }

  /**
   * Creates a thread which will write "." to the given print stream at the given interval. The
   * created thread is not started by this method. The created thread will be daemonic and will
   * halt when interrupted.
   *
   * @param intervalMs the time interval in milliseconds between writes
   * @param stream the print stream to write to
   * @return the thread
   */
  public static Thread createProgressThread(final long intervalMs, final PrintStream stream) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          CommonUtils.sleepMs(intervalMs);
          if (Thread.interrupted()) {
            return;
          }
          stream.print(".");
        }
      }
    });
    thread.setDaemon(true);
    return thread;
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
          + ServiceContants.LIST_STATUS + "?jobId=" + jobId);
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
   * @return the url for running jobs
   */
  public static URL getRunJobURL() {
    try {
      return new URL(
          getJobServiceBaseURL().toString() + "/" + ServiceContants.RUN_JOB);
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return the base URL for the job service
   */
  private static URL getJobServiceBaseURL() {
    InetSocketAddress address = getJobMasterWebAddress();
    String host = address.getHostName();
    int port = address.getPort();
    try {
      return new URL("http://" + host + ":" + port + Constants.REST_API_PREFIX + "/"
          + ServiceContants.SERVICE_PREFIX);
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return the web address of the leader job master
   */
  public static InetSocketAddress getJobMasterWebAddress() {
    if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      String jobLeaderZkPath = Configuration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH);
      try (RetryHandlingMetaJobMasterClient client =
          new RetryHandlingMetaJobMasterClient(jobLeaderZkPath)) {
        int webPort =
            client.getInfo(new HashSet<>(Arrays.asList(JobMasterInfoField.WEB_PORT))).getWebPort();
        return new InetSocketAddress(client.getAddress().getHostName(), webPort);
      } catch (ConnectionFailedException e) {
        throw Throwables.propagate(e);
      }
    }
    return NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_WEB);
  }

  private JobRestClientUtils() {} // Not intended for instantiation.
}
