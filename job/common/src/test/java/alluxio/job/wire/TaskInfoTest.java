/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.wire;

import alluxio.util.CommonUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests the wire format {@link TaskInfo}.
 */
public final class TaskInfoTest {

  @Test
  public void jsonTest() throws Exception {
    TaskInfo taskInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    TaskInfo other = mapper.readValue(mapper.writeValueAsBytes(taskInfo), TaskInfo.class);
    checkEquality(taskInfo, other);
  }

  public static TaskInfo createRandom() {
    TaskInfo result = new TaskInfo();
    Random random = new Random();

    result.setErrorMessage(CommonUtils.randomAlphaNumString(random.nextInt(10)));
    result.setStatus(Status.values()[random.nextInt(Status.values().length)]);
    result.setTaskId(random.nextInt());

    return result;
  }

  public void checkEquality(TaskInfo a, TaskInfo b) {
    Assert.assertEquals(a.getErrorMessage(), b.getErrorMessage());
    Assert.assertEquals(a.getStatus(), b.getStatus());
    Assert.assertEquals(a.getTaskId(), b.getTaskId());
    Assert.assertEquals(a, b);
  }
}
