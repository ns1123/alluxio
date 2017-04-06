/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.client.job;

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.retry.CountingRetry;
import alluxio.util.CommonUtils;

import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utils for interacting with the job service through a Thrift client.
 */
@ThreadSafe
public final class JobThriftClientUtils {
  private static final Logger LOG = LoggerFactory.getLogger(JobThriftClientUtils.class);
  private static final JobMasterClientPool CLIENT_POOL = new JobMasterClientPool();

  /**
   * Starts the specified job.
   *
   * @param config a {@link JobConfig} describing the job to run
   * @return the job ID for the created job
   * @throws AlluxioException if Alluxio error occurs
   * @throws IOException if non-Alluxio error occurs
   */
  public static long start(JobConfig config) throws AlluxioException, IOException {
    JobMasterClient client = CLIENT_POOL.acquire();
    try {
      return client.run(config);
    } finally {
      CLIENT_POOL.release(client);
    }
  }

  /**
   * Runs the specified job and waits for it to finish.
   *
   * @param config configuration for the job to run
   * @throws AlluxioException if Alluxio error occurs
   * @throws IOException if non-Alluxio error occurs
   */
  public static void run(JobConfig config) throws AlluxioException, IOException {
    run(config, 1);
  }

  /**
   * Runs the specified job and waits for it to finish. If the job fails, it is retried the given
   * number of times. If the job does not complete in the given number of attempts, an exception
   * is thrown.
   *
   * @param config configuration for the job to run
   * @param attempts number of times to try running the job before giving up
   */
  public static void run(JobConfig config, int attempts) {
    CountingRetry retryPolicy = new CountingRetry(attempts);
    while (retryPolicy.attemptRetry()) {
      long jobId;
      try {
        jobId = start(config);
      } catch (Exception e) {
        // job could not be started, retry
        LOG.warn("Exception encountered when starting a job.", e);
        continue;
      }
      JobInfo jobInfo = waitFor(jobId);
      if (jobInfo == null) {
        // job status could not be fetched, give up
        break;
      }
      if (jobInfo.getStatus() == Status.COMPLETED || jobInfo.getStatus() == Status.CANCELED) {
        return;
      }
      LOG.warn("Job {} failed to complete: {}", jobId, jobInfo.getErrorMessage());
    }
    throw new RuntimeException("Failed to successfully complete the job.");
  }

  /**
   * Convenience method for calling {@link #createProgressThread(long, PrintStream)} with an
   * interval of 2 seconds.
   *
   * @param stream the print stream to write to
   * @return the thread
   */
  public static Thread createProgressThread(PrintStream stream) {
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
   * @return the job info for the job once it finishes or null if the job status cannot be fetched
   */
  private static JobInfo waitFor(final long jobId) {
    final AtomicReference<JobInfo> finishedJobInfo = new AtomicReference<>();
    CommonUtils.waitFor("Job to finish", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        JobInfo jobInfo;
        try {
          jobInfo = getStatus(jobId);
        } catch (Exception e) {
          LOG.warn("Failed to get status for job (jobId={})", jobId, e);
          finishedJobInfo.set(null);
          return true;
        }
        switch (jobInfo.getStatus()) {
          case FAILED: // fall through
          case CANCELED: // fall through
          case COMPLETED:
            finishedJobInfo.set(jobInfo);
            return true;
          case RUNNING: // fall through
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
   * Gets the status for the given job.
   *
   * @param jobId the ID for the job to query
   * @return JobInfo describing the job
   * @throws AlluxioException if Alluxio error occurs
   * @throws IOException if non-Alluxio error occurs
   */
  public static JobInfo getStatus(long jobId) throws AlluxioException, IOException {
    JobMasterClient client = CLIENT_POOL.acquire();
    try {
      return client.getStatus(jobId);
    } finally {
      CLIENT_POOL.release(client);
    }
  }

  /**
   * Cancels the given job.
   *
   * @param jobId the ID for the job to cancel
   * @throws AlluxioException if Alluxio error occurs
   * @throws IOException if non-Alluxio error occurs
   */
  public static void cancel(long jobId) throws AlluxioException, IOException {
    JobMasterClient client = CLIENT_POOL.acquire();
    try {
      client.cancel(jobId);
    } finally {
      CLIENT_POOL.release(client);
    }
  }

  private JobThriftClientUtils() {} // prevent instantiation
}
