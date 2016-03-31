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
 * Tests {@link LoadConfig}.
 */
public final class LoadConfigTest {
  @Test
  public void jsonTest() throws Exception {
    LoadConfig config = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    LoadConfig other = mapper.readValue(mapper.writeValueAsString(config), LoadConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void nullTest() {
    try {
      new LoadConfig(null);
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("The file path cannot be null", exception.getMessage());
    }
  }

  public static LoadConfig createRandom() {
    Random random = new Random();
    String path = "/" + CommonUtils.randomString(random.nextInt(10));
    LoadConfig config = new LoadConfig(path);
    return config;
  }

  public void checkEquality(LoadConfig a, LoadConfig b) {
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a, b);
  }
}
