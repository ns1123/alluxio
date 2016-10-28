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
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.wire.Status;
import alluxio.master.job.JobMaster;
import alluxio.master.job.meta.JobInfo;
import alluxio.util.CommonUtils;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

/**
 * Utility methods for tests related to the job service.
 */
public final class JobTestUtils {

  /**
   * Waits for the job with the given job ID to be in the given state.
   *
   * @param jobMaster the job master running the job
   * @param jobId the ID of the job
   * @param status the status to wait for
   */
  public static void waitForJobStatus(final JobMaster jobMaster, final long jobId,
      final Status status) {
    CommonUtils.waitFor(String.format("job %d to be in status %s", jobId, status.toString()),
        new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            JobInfo info;
            try {
              info = jobMaster.getJobInfo(jobId);
              return info.getStatus().equals(status);
            } catch (JobDoesNotExistException e) {
              throw Throwables.propagate(e);
            }
          }
        }, 30 * Constants.SECOND_MS);
  }

  private JobTestUtils() {} // Not intended for instatiation.
}
