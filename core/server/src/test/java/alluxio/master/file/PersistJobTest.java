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

import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for {@link PersistJob}.
 */
public final class PersistJobTest {

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long fileId = random.nextLong();
    long jobId = random.nextLong();
    String tempUfsPath = CommonUtils.randomAlphaNumString(random.nextInt(10));
    RetryPolicy retry = new CountingRetry(1);

    PersistJob persistJob = new PersistJob(fileId, jobId, tempUfsPath).setRetryPolicy(retry);

    Assert.assertEquals(fileId, persistJob.getFileId());
    Assert.assertEquals(jobId, persistJob.getJobId());
    Assert.assertEquals(tempUfsPath, persistJob.getTempUfsPath());
    Assert.assertEquals(retry, persistJob.getRetryPolicy());
  }
}
