/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job.meta;

import alluxio.job.JobConfig;
import alluxio.master.job.JobCoordinator;
import alluxio.thrift.Status;
import alluxio.thrift.TaskInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests {@link JobInfo}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JobCoordinator.class})
public final class JobInfoTest {
  private int mJobId;
  private JobInfo mJobInfo;
  private JobCoordinator mJobCoordinator;

  @Before
  public void before() {
    mJobId = 1;
    JobConfig jobConfig = Mockito.mock(JobConfig.class, Mockito.withSettings().serializable());
    mJobInfo = new JobInfo(mJobId, "test", jobConfig);
    mJobCoordinator = Mockito.mock(JobCoordinator.class);
    mJobInfo.setJobCoordinator(mJobCoordinator);
    // register tasks
    for (int i = 0; i < 3; i++) {
      mJobInfo.addTask(i);
    }
  }

  @Test
  public void failureTest() {
    // set task
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.RUNNING, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.FAILED, "failed", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
    Assert.assertEquals("The task execution failed", mJobInfo.getErrorMessage());
  }

  @Test
  public void failureOverCancelTest() {
    // set task
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.CANCELED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.FAILED, "failed", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
  }

  @Test
  public void cancelTest() {
    // set task
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.CANCELED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.RUNNING, "", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));

    Assert.assertEquals(Status.CANCELED, mJobInfo.getStatus());
  }

  @Test
  public void runningTest() {
    // set task
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.RUNNING, "", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));

    Assert.assertEquals(Status.RUNNING, mJobInfo.getStatus());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void completedTest() throws Exception {
    // set task
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));

    Assert.assertEquals(Status.COMPLETED, mJobInfo.getStatus());
    Mockito.verify(mJobCoordinator).join(Mockito.anyMap());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void joinFailureTest() throws Exception {
    Mockito.when(mJobCoordinator.join(Mockito.anyMap()))
        .thenThrow(new UnsupportedOperationException("test exception"));
    // set task
    mJobInfo.setTaskInfo(0, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(1, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));
    mJobInfo.setTaskInfo(2, new TaskInfo(mJobId, 0, Status.COMPLETED, "", null));

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
    Assert.assertEquals("test exception", mJobInfo.getErrorMessage());
  }
}
