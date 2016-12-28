/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.file;

import alluxio.CommonTestUtils;
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
    RetryPolicy retry = new CountingRetry(0);

    PersistJob persistJob = new PersistJob(fileId, jobId, tempUfsPath).setRetry(retry);

    Assert.assertEquals(fileId, persistJob.getFileId());
    Assert.assertEquals(jobId, persistJob.getJobId());
    Assert.assertEquals(tempUfsPath, persistJob.getTempUfsPath());
    Assert.assertEquals(retry, persistJob.getRetry());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(PersistJob.class, "mRetry");
  }
}
