/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.client.job;

import static org.junit.Assert.assertEquals;

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link JobContext}.
 */
public final class JobContextTest {
  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.MASTER_HOSTNAME, "host1",
      PropertyKey.JOB_MASTER_HOSTNAME, "host2"
  ));

  @Test
  public void getAddress() throws Exception {
    try (JobContext context = JobContext.create()) {
      assertEquals("host2", context.getJobMasterAddress().getHostName());
    }
  }
}
