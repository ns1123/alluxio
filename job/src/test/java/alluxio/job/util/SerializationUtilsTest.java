/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.util;

import alluxio.job.load.LoadConfig;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the utility class {@link SerializationUtils}.
 */
public final class SerializationUtilsTest {
  @Test
  public void basicTest() throws Exception {
    LoadConfig config = new LoadConfig("test");
    byte[] bytes = SerializationUtils.serialize(config);
    Object deserialized = SerializationUtils.deserialize(bytes);
    Assert.assertTrue(deserialized instanceof LoadConfig);
    Assert.assertEquals(config, deserialized);
  }
}
