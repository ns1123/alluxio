/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.adjust;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link EvictConfig}.
 */
public final class EvictConfigTest {
  private static final long TEST_BLOCK_ID = 1L;

  @Test
  public void json() throws Exception {
    EvictConfig config = new EvictConfig(TEST_BLOCK_ID, 1);
    ObjectMapper mapper = new ObjectMapper();
    EvictConfig other = mapper.readValue(mapper.writeValueAsString(config), EvictConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void negativeReplicas() {
    try {
      new EvictConfig(TEST_BLOCK_ID, -1);
      Assert.fail("Cannot create EvictConfig with negative replicas");
    } catch (IllegalArgumentException e) {
      // expected exception thrown. test passes
    }
  }

  @Test
  public void zeroReplicas() {
    try {
      new EvictConfig(TEST_BLOCK_ID, 0);
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
}
