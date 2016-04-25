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

package alluxio.job.persist;

import alluxio.AlluxioURI;
import alluxio.IntegrationTestUtils;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.job.JobIntegrationTest;
import alluxio.master.file.meta.PersistenceState;

import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for {@link PersistDefinition}.
 */
public final class PersistIntegrationTest extends JobIntegrationTest {
  private static final String TEST_URI = "/test";

  /**
   * Tests persisting a file.
   */
  @Test
  public void persistTest() throws Exception {
    // write a file in alluxio only
    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath, mWriteAlluxio);
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    // run the persist job
    waitForJobToFinish(mJobMaster.runJob(new PersistConfig("/test", true)));
    IntegrationTestUtils.waitForPersist(mResource, filePath);

    // a second persist call with overwrite flag off fails
    final long jobId = mJobMaster.runJob(new PersistConfig("/test", false));
    waitForJobFailure(jobId);

    Assert.assertTrue(mJobMaster.getJobInfo(jobId).getTaskInfoList().get(0).getErrorMessage()
        .contains("File /test is already persisted, "
            + "to overwrite the file, please set the overwrite flag in the config"));
  }
}
