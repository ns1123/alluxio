/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.client.job;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utils for interacting with the job service through a Thrift client.
 */
public final class JobThriftClientUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Runs the job specified by the config.
   *
   * @param config a {@link JobConfig} describing the job to run
   * @return the job ID for the created job
   * @throws AlluxioException if Alluxio error occurs
   * @throws IOException if non-Alluxio error occurs
   */
  public static long start(JobConfig config) throws AlluxioException, IOException {
    return createClient().run(config);
  }

  /**
   * Runs the specified job and waits for it to finish, throwing an exception if the job fails.
   *
   * @param config configuration for the job to run
   * @throws AlluxioException if Alluxio error occurs
   * @throws IOException if non-Alluxio error occurs
   */
  public static void run(JobConfig config) throws AlluxioException, IOException {
    run(config, 1);
  }

  /**
   * Runs the specified job and waits for it to finish, throwing an exception if the job fails after
   * the specified number of attempts have failed to complete.
   *
   * @param config configuration for the job to run
   * @param attempts number of times to try running the job before giving up
   * @throws AlluxioException if Alluxio error occurs
   * @throws IOException if non-Alluxio error occurs
   */
  public static void run(JobConfig config, int attempts)
      throws AlluxioException, IOException {
    long completedAttempts = 0;
    while (true) {
      long jobId = start(config);
      JobInfo jobInfo = waitFor(jobId);
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
  public static JobInfo waitFor(final long jobId) {
    final AtomicReference<JobInfo> finishedJobInfo = new AtomicReference<>();
    CommonUtils.waitFor("Job to finish", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        try {
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
        } catch (AlluxioException | IOException e) {
          LOG.warn("Failed to fetch job status", e);
          return false;
        }
      }
    });
    return finishedJobInfo.get();
  }

  /**
   * @param jobId the ID for the job to query
   * @return JobInfo describing the job
   * @throws AlluxioException if Alluxio error occurs
   * @throws IOException if non-Alluxio error occurs
   */
  public static JobInfo getJobInfo(long jobId) throws AlluxioException, IOException {
    return createClient().getStatus(jobId);
  }

  /**
   * Creates a new instance of {@link JobMasterClient}.
   *
   * @return the job master client
   */
  private static JobMasterClient createClient() {
    if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      return new RetryHandlingJobMasterClient(
          Configuration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH));
    } else {
      return new RetryHandlingJobMasterClient(
          NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC));
    }
  }

  private JobThriftClientUtils() {} // prevent instantiation
}
