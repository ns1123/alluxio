/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
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
