package alluxio.master.file;

import alluxio.Constants;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

public final class PersistRequest {
  private long mFileId;
  private RetryPolicy mRetry;

  public PersistRequest(long fileId) {
    mFileId = fileId;
    mRetry = new CountingRetry(Constants.PERSISTENCE_MAX_RETRIES);
  }

  public long getFileId() {
    return mFileId;
  }

  public RetryPolicy getRetry() {
    return mRetry;
  }

  public PersistRequest setRetry(RetryPolicy retry) {
    mRetry = retry;
    return this;
  }
}
