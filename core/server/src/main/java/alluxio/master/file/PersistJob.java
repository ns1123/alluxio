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

package alluxio.master.file;

import alluxio.Constants;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import com.google.common.base.Objects;

/**
 * Represents a persist job.
 */
public final class PersistJob {
  /** The id of the file that is persisted. */
  private long mFileId;
  /** The id of the persist job. */
  private long mJobId;
  /** The temporary UFS path the file is persisted to. */
  private String mTempUfsPath;
  /** The retry policy. */
  private RetryPolicy mRetry;

  /**
   * Creates a new instance of {@link PersistJob}.
   *
   * @param fileId the file id to use
   * @param jobId the job id to use
   * @param tempUfsPath the temporary UFS path to use
   */
  public PersistJob(long fileId, long jobId, String tempUfsPath) {
    mFileId = fileId;
    mJobId = jobId;
    mTempUfsPath = tempUfsPath;
    mRetry = new CountingRetry(Constants.PERSISTENCE_MAX_RETRIES);
  }

  /**
   * @return the file id
   */
  public long getFileId() {
    return mFileId;
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @return the temporary UFS path
   */
  public String getTempUfsPath() {
    return mTempUfsPath;
  }

  /**
   * @return the retry policy
   */
  public RetryPolicy getRetry() {
    return mRetry;
  }

  /**
   * @param retry the retry policy to use
   * @return the updated object
   */
  public PersistJob setRetry(RetryPolicy retry) {
    mRetry = retry;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PersistJob)) {
      return false;
    }
    PersistJob that = (PersistJob) o;
    return Objects.equal(mFileId, that.mFileId) && Objects.equal(mJobId, that.mJobId)
        && Objects.equal(mTempUfsPath, that.mTempUfsPath) && Objects.equal(mRetry, that.mRetry);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFileId, mJobId, mTempUfsPath, mRetry);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("fileId", mFileId).add("jobId", mJobId)
        .add("tempUfsPath", mTempUfsPath).add("retry", mRetry).toString();
  }
}
