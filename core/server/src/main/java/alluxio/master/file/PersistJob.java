package alluxio.master.file;

import alluxio.Constants;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

public final class PersistJob {
  private long mFileId;
  private long mJobId;
  private String mTempUfsPath;
  private RetryPolicy mRetry;

  public PersistJob(long fileId, long jobId, String tempUfsPath) {
    mFileId = fileId;
    mJobId = jobId;
    mTempUfsPath = tempUfsPath;
    mRetry = new CountingRetry(Constants.PERSISTENCE_MAX_RETRIES);
  }

  public long getFileId() {
    return mFileId;
  }

  public long getJobId() {
    return mJobId;
  }

  public String getTempUfsPath() {
    return mTempUfsPath;
  }

  public RetryPolicy getRetry() {
    return mRetry;
  }

  public PersistJob setRetry(RetryPolicy retry) {
    mRetry = retry;
    return this;
  }
}
