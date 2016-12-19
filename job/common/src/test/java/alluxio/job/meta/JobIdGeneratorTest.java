/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.meta;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link JobIdGenerator}.
 */
public final class JobIdGeneratorTest {
  @Test
  public void newIdTest() {
    JobIdGenerator generator = new JobIdGenerator();
    Assert.assertNotEquals(generator.getNewJobId(), generator.getNewJobId());
  }
}
