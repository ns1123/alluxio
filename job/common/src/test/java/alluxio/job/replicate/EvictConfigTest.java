/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.replicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Test {@link EvictConfig}.
 */
public final class EvictConfigTest {
  @Test
  public void json() throws Exception {
    EvictConfig config = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    EvictConfig other = mapper.readValue(mapper.writeValueAsString(config), EvictConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void negativeReplicas() {
    try {
      new EvictConfig(0, -1);
      Assert.fail("Cannot create EvictConfig with negative replicas");
    } catch (IllegalArgumentException e) {
      // expected exception thrown. test passes
    }
  }

  @Test
  public void zeroReplicas() {
    try {
      new EvictConfig(0, 0);
      Assert.fail("Cannot create EvictConfig with zero replicas");
    } catch (IllegalArgumentException e) {
      // expected exception thrown. test passes
    }
  }

  public void checkEquality(EvictConfig a, EvictConfig b) {
    Assert.assertEquals(a.getBlockId(), b.getBlockId());
    Assert.assertEquals(a.getReplicas(), b.getReplicas());
    Assert.assertEquals(a, b);
  }

  public static EvictConfig createRandom() {
    Random random = new Random();
    EvictConfig config = new EvictConfig(random.nextLong(), random.nextInt(Integer.MAX_VALUE) + 1);
    return config;
  }
}
