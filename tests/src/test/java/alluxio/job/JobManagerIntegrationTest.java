/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.master.AlluxioMaster;
import alluxio.master.Master;
import alluxio.master.job.JobManagerMaster;
import alluxio.master.job.meta.JobInfo;
import alluxio.thrift.Status;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.junit.Before;
import org.junit.Rule;

/**
 * Prepares the environment for the job manager integration tests.
 */
public abstract class JobManagerIntegrationTest {
  protected static final int BUFFER_BYTES = 100;
  protected static final long WORKER_CAPACITY_BYTES = Constants.GB;
  protected static final int BLOCK_SIZE_BYTES = 128;

  protected final CreateFileOptions mWriteAlluxio =
      StreamOptionUtils.getCreateFileOptionsMustCache(new Configuration());
  protected final CreateFileOptions mWriteUnderStore =
      StreamOptionUtils.getCreateFileOptionsThrough(new Configuration());

  protected JobManagerMaster mJobManagerMaster;
  protected Configuration mTestConf;
  protected FileSystem mFileSystem = null;

  @Rule
  public LocalAlluxioClusterResource mResource =
      new LocalAlluxioClusterResource(WORKER_CAPACITY_BYTES, BLOCK_SIZE_BYTES,
          Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES));

  @Before
  public void before() throws Exception {
    mTestConf = mResource.get().getWorkerConf();
    mJobManagerMaster = getJobManagerMaster();
    mFileSystem = mResource.get().getClient();
  }

  private JobManagerMaster getJobManagerMaster() {
    for (Master master : AlluxioMaster.get().getAdditionalMasters()) {
      if (master instanceof JobManagerMaster) {
        return (JobManagerMaster) master;
      }
    }
    throw new RuntimeException("JobManagerMaster is not registerd in Alluxio Master");
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
          info = mJobManagerMaster.getJobInfo(jobId);
          return info.getTaskInfoList().get(0).getStatus().equals(status);
        } catch (JobDoesNotExistException e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }
}
