/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.replicate;

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Test {@link ReplicateConfig}.
 */
public final class ReplicateConfigTest {
  @Test
  public void json() throws Exception {
    ReplicateConfig config = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    ReplicateConfig other =
        mapper.readValue(mapper.writeValueAsString(config), ReplicateConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void negativeReplicateNumber() {
    try {
      new ReplicateConfig("", 0, -1);
      Assert.fail("Cannot create ReplicateConfig with negative replicateNumber");
    } catch (IllegalArgumentException exception) {
      // expected exception thrown. test passes
    }
  }

  public void checkEquality(ReplicateConfig a, ReplicateConfig b) {
    Assert.assertEquals(a.getBlockId(), b.getBlockId());
    Assert.assertEquals(a.getReplicas(), b.getReplicas());
    Assert.assertEquals(a, b);
  }

  public static ReplicateConfig createRandom() {
    Random random = new Random();
    String path = "/" + CommonUtils.randomAlphaNumString(random.nextInt(10) + 1);
    ReplicateConfig config =
        new ReplicateConfig(path, random.nextLong(), random.nextInt(Integer.MAX_VALUE) + 1);
    return config;
  }
}
