/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.load;

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests {@link DistributedSingleFileLoadConfig}.
 */
public final class DistributedSingleFileLoadConfigTest {
  @Test
  public void jsonTest() throws Exception {
    DistributedSingleFileLoadConfig config = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    DistributedSingleFileLoadConfig other =
        mapper.readValue(mapper.writeValueAsString(config), DistributedSingleFileLoadConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void nullTest() {
    try {
      new DistributedSingleFileLoadConfig(null);
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("The file path cannot be null", exception.getMessage());
    }
  }

  public static DistributedSingleFileLoadConfig createRandom() {
    Random random = new Random();
    String path = "/" + CommonUtils.randomString(random.nextInt(10));
    DistributedSingleFileLoadConfig config = new DistributedSingleFileLoadConfig(path);
    return config;
  }

  public void checkEquality(DistributedSingleFileLoadConfig a, DistributedSingleFileLoadConfig b) {
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a, b);
  }
}
