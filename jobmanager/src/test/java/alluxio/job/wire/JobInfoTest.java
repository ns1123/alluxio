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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * Tests the wire format of {@link JobInfo}.
 */
public final class JobInfoTest {
  @Test
  public void jsonTest() throws Exception {
    JobInfo jobInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    JobInfo other = mapper.readValue(mapper.writeValueAsBytes(jobInfo), JobInfo.class);
    checkEquality(jobInfo, other);
  }

  public static JobInfo createRandom() {
    JobInfo result = new JobInfo();
    Random random = new Random();

    List<TaskInfo> taskInfoList = Lists.newArrayList();
    for (int i = 0; i < random.nextInt(10); i++) {
      taskInfoList.add(TaskInfoTest.createRandom());
    }
    result.setTaskInfoList(taskInfoList);
    result.setErrorMessage(CommonUtils.randomString(random.nextInt(10)));
    result.setJobId(random.nextLong());
    result.setResult(CommonUtils.randomString(random.nextInt(10)));
    result.setStatus(Status.values()[random.nextInt(Status.values().length)]);
    return result;
  }

  public void checkEquality(JobInfo a, JobInfo b) {
    Assert.assertEquals(a.getErrorMessage(), b.getErrorMessage());
    Assert.assertEquals(a.getJobId(), b.getJobId());
    Assert.assertEquals(a.getTaskInfoList(), b.getTaskInfoList());
    Assert.assertEquals(a.getResult(), b.getResult());
    Assert.assertEquals(a.getStatus(), b.getStatus());
  }
}
