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

package alluxio.job;

import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.StreamOptionUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.master.job.meta.JobInfo;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

/**
 * Prepares the environment for the job manager integration tests.
 */
public abstract class JobIntegrationTest {
  protected static final int BUFFER_BYTES = 100;
  protected static final long WORKER_CAPACITY_BYTES = Constants.GB;
  protected static final int BLOCK_SIZE_BYTES = 128;

  protected final CreateFileOptions mWriteAlluxio =
      StreamOptionUtils.getCreateFileOptionsMustCache();
  protected final CreateFileOptions mWriteUnderStore =
      StreamOptionUtils.getCreateFileOptionsThrough();

  protected JobMaster mJobMaster;
  protected Configuration mTestConf;
  protected FileSystem mFileSystem = null;
  protected LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(WORKER_CAPACITY_BYTES, BLOCK_SIZE_BYTES,
          Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES));

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster =
        new LocalAlluxioJobCluster(mLocalAlluxioClusterResource.get().getWorkerConf());
    mLocalAlluxioJobCluster.start();
    mTestConf = mLocalAlluxioJobCluster.getTestConf();
    mJobMaster = mLocalAlluxioJobCluster.getMaster().getJobMaster();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() throws Exception {
    mLocalAlluxioJobCluster.stop();
  }

  protected void waitForJobToFinish(final long jobId) {
    waitForJobStatus(jobId, Status.COMPLETED);
  }

  protected void waitForJobFailure(final long jobId) {
    waitForJobStatus(jobId, Status.FAILED);
  }

  protected void waitForJobCancelled(final long jobId) {
    waitForJobStatus(jobId, Status.CANCELED);
  }

  protected void waitForJobRunning(final long jobId) {
    waitForJobStatus(jobId, Status.RUNNING);
  }

  private void waitForJobStatus(final long jobId, final Status status) {
    CommonTestUtils.waitFor(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        JobInfo info;
        try {
          info = mJobMaster.getJobInfo(jobId);
          return info.getStatus().equals(status);
        } catch (JobDoesNotExistException e) {
          throw Throwables.propagate(e);
        }
      }
    }, 10 * Constants.SECOND_MS);
  }
}
