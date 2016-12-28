package alluxio.master.file;

import alluxio.Constants;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import com.google.common.base.Objects;

/**
 * Represents a persist request.
 */
public final class PersistRequest {
  /** The id of the file to persist. */
  private long mFileId;
  /** The retry policy. */
  private RetryPolicy mRetry;

  /**
   * Constructs a new instance of {@link PersistRequest}.
   *
   * @param fileId the file id to use
   */
  public PersistRequest(long fileId) {
    mFileId = fileId;
    mRetry = new CountingRetry(Constants.PERSISTENCE_MAX_RETRIES);
  }

  /**
   * @return the file id
   */
  public long getFileId() {
    return mFileId;
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
  public PersistRequest setRetry(RetryPolicy retry) {
    mRetry = retry;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PersistRequest)) {
      return false;
    }
    PersistRequest that = (PersistRequest) o;
    return Objects.equal(mFileId, that.mFileId) && Objects.equal(mRetry, that.mRetry);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFileId, mRetry);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("fileId", mFileId).add("retry", mRetry).toString();
  }
}
