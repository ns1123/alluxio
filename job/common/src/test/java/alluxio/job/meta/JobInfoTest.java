/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.meta;

import alluxio.job.JobConfig;
import alluxio.job.TestJobConfig;
import alluxio.job.wire.Status;
import alluxio.util.CommonUtils;

import com.google.common.base.Function;
import org.junit.Assert;
import org.junit.Test;

public final class JobInfoTest {
  @Test
  public void compare() {
    JobConfig jobConfig = new TestJobConfig("unused");
    JobInfo a = new JobInfo(0L, jobConfig, null);
    CommonUtils.sleepMs(1);
    JobInfo b = new JobInfo(0L, jobConfig, null);
    Assert.assertEquals(-1, a.compareTo(b));
    b.setStatus(Status.RUNNING);
    CommonUtils.sleepMs(1);
    a.setStatus(Status.RUNNING);
    Assert.assertEquals(1, a.compareTo(b));
    a.setStatus(Status.COMPLETED);
    CommonUtils.sleepMs(1);
    b.setStatus(Status.COMPLETED);
    Assert.assertEquals(-1, a.compareTo(b));
  }

  @Test
  public void callback() {
    final String result = "I was here!";
    JobConfig jobConfig = new TestJobConfig("unused");
    JobInfo a = new JobInfo(0L, jobConfig, new Function<JobInfo, Void>() {
      @Override
      public Void apply(JobInfo jobInfo) {
        jobInfo.setResult(result);
        return null;
      }
    });
    a.setStatus(Status.COMPLETED);
    Assert.assertEquals(result, a.getResult());
  }
}
