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
 * Tests {@link DistributedSingleFileLoadingConfig}.
 */
public final class DistributedSingleFileLoadingConfigTest {
  @Test
  public void jsonTest() throws Exception {
    DistributedSingleFileLoadingConfig config = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    DistributedSingleFileLoadingConfig other = mapper.readValue(mapper.writeValueAsString(config),
        DistributedSingleFileLoadingConfig.class);
    checkEquality(config, other);
  }

  public static DistributedSingleFileLoadingConfig createRandom() {
    Random random = new Random();
    String path = "/" + CommonUtils.randomString(random.nextInt(10));
    DistributedSingleFileLoadingConfig config = new DistributedSingleFileLoadingConfig(path);
    return config;
  }

  public void checkEquality(DistributedSingleFileLoadingConfig a,
      DistributedSingleFileLoadingConfig b) {
    Assert.assertEquals(a.getFilePathAsString(), b.getFilePathAsString());
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a, b);
  }
}
