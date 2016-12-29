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
public final class PersistRequestTest {

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long fileId = random.nextLong();
    RetryPolicy retry = new CountingRetry(1);

    PersistRequest persistRequest = new PersistRequest(fileId).setRetry(retry);

    Assert.assertEquals(fileId, persistRequest.getFileId());
    Assert.assertEquals(retry, persistRequest.getRetry());
  }
}
