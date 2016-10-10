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
 * Test {@link ReplicateConfig}.
 */
public final class ReplicateConfigTest {
  private static final long TEST_BLOCK_ID = 1L;

  @Test
  public void json() throws Exception {
    ReplicateConfig config = new ReplicateConfig(TEST_BLOCK_ID, 1);
    ObjectMapper mapper = new ObjectMapper();
    ReplicateConfig other =
        mapper.readValue(mapper.writeValueAsString(config), ReplicateConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void negativeReplicateNumber() {
    try {
      new ReplicateConfig(TEST_BLOCK_ID, -1);
      Assert.fail("Cannot create ReplicateConfig with negative replicateNumber");
    } catch (IllegalArgumentException exception) {
      // expected exception thrown. test passes
    }
  }

  public void checkEquality(ReplicateConfig a, ReplicateConfig b) {
    Assert.assertEquals(a.getBlockId(), b.getBlockId());
    Assert.assertEquals(a.getReplicateNumber(), b.getReplicateNumber());
    Assert.assertEquals(a, b);
  }
}
