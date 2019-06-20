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

package alluxio.master.policy.action;

import alluxio.client.job.JobMasterClient;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.master.policy.meta.InodeState;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * Base class for ActionExecution implementations based on job service.
 */
public abstract class JobServiceActionExecution extends AbstractActionExecution {
  private static final long INVALID_JOB_ID = -1;
  private static final ObjectMapper JSON_ENCODER = new ObjectMapper();

  private volatile long mJobId = INVALID_JOB_ID;

  /**
   * @param ctx the execution context
   * @param path the Alluxio path
   * @param inode the inode
   */
  public JobServiceActionExecution(ActionExecutionContext ctx, String path, InodeState inode) {
    super(ctx, path, inode);
  }

  /**
   * @return the logger
   */
  protected abstract Logger getLogger();

  /**
   * @return a new job config to be run by the job service
   */
  protected abstract JobConfig createJobConfig();

  @Override
  public synchronized ActionStatus start() {
    super.start();
    JobMasterClient client = mContext.getJobMasterClientPool().acquire();
    try {
      mJobId = client.run(createJobConfig());
      mStatus = ActionStatus.IN_PROGRESS;
    } catch (IOException e) {
      mStatus = ActionStatus.FAILED;
      mException = e;
    } finally {
      mContext.getJobMasterClientPool().release(client);
    }
    return mStatus;
  }

  @Override
  public ActionStatus update() throws IOException {
    if (mStatus != ActionStatus.IN_PROGRESS) {
      return mStatus;
    }
    JobInfo jobInfo;
    JobMasterClient client = mContext.getJobMasterClientPool().acquire();
    try {
      jobInfo = client.getStatus(mJobId);
      getLogger().debug("Job info: {}", jobInfo);
    } finally {
      mContext.getJobMasterClientPool().release(client);
    }
    switch (jobInfo.getStatus()) {
      case CANCELED:
        // fall through.
      case FAILED:
        mStatus = ActionStatus.FAILED;
        String err = String.format("Job (id=%d, configuration=%s) failed: %s",
            jobInfo.getJobId(), JSON_ENCODER.writeValueAsString(jobInfo.getJobConfig()),
            jobInfo.getErrorMessage());
        getLogger().error(err);
        mException = new IOException(err);
        break;
      case COMPLETED:
        mStatus = ActionStatus.PREPARED;
        break;
      default:
        // Still IN_PROGRESS.
        break;
    }
    return mStatus;
  }

  @Override
  public synchronized ActionStatus commit() {
    super.commit();
    mStatus = ActionStatus.COMMITTED;
    return mStatus;
  }

  @Override
  public synchronized void close() throws IOException {
    if (update().isTerminal()) {
      // The job must have succeeded, failed, or been cancelled.
      return;
    }
    JobMasterClient client = mContext.getJobMasterClientPool().acquire();
    try {
      client.cancel(mJobId);
    } finally {
      mContext.getJobMasterClientPool().release(client);
    }
  }
}
