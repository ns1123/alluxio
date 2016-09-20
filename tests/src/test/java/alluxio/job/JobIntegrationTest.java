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

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.master.job.meta.JobInfo;
import alluxio.util.CommonUtils;

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
      CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
  protected final CreateFileOptions mWriteUnderStore =
      CreateFileOptions.defaults().setWriteType(WriteType.THROUGH);

  protected JobMaster mJobMaster;
  protected FileSystem mFileSystem = null;
  protected LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, WORKER_CAPACITY_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES))
          .build();

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
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
    CommonUtils.waitFor("waiting for the job status", new Function<Void, Boolean>() {
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
    }, 60 * Constants.SECOND_MS);
  }
}
