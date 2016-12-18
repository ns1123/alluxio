/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.persist;

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Test {@link PersistConfig}.
 */
public final class PersistConfigTest {
  @Test
  public void jsonTest() throws Exception {
    PersistConfig config = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    PersistConfig other = mapper.readValue(mapper.writeValueAsString(config), PersistConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void nullTest() {
    try {
      new PersistConfig(null, true);
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("The file path cannot be null", exception.getMessage());
    }
  }

  public static PersistConfig createRandom() {
    Random random = new Random();
    String path = "/" + CommonUtils.randomAlphaNumString(random.nextInt(10));
    PersistConfig config = new PersistConfig(path, random.nextBoolean());
    return config;
  }

  public void checkEquality(PersistConfig a, PersistConfig b) {
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a.isOverwrite(), b.isOverwrite());
    Assert.assertEquals(a, b);
  }
}
