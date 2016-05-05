/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.exception.ExceptionMessage;
import alluxio.job.JobConfig;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.load.LoadConfig;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.Journal;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

/**
 * Tests {@link JobMaster}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class, BlockMaster.class, JobCoordinator.class})
public final class JobMasterTest {
  private JobMaster mJobMaster;

  @Before
  public void before() {
    mJobMaster = new JobMaster(Mockito.mock(Journal.class));
  }

  @Test
  public void runNonExistingJobConfigTest() {
    try {
      mJobMaster.runJob(new DummyJobConfig());
      Assert.fail("cannot run non-existing job");
    } catch (JobDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage("dummy"),
          e.getMessage());
    }
  }

  @Test
  public void runJobTest() throws Exception {
    LoadConfig jobConfig = new LoadConfig("/test");
    long jobId = mJobMaster.runJob(jobConfig);
    Assert.assertEquals(Lists.newArrayList(jobId), mJobMaster.listJobs());
  }

  @Test
  public void cancelNonExistingJobTest() {
    try {
      mJobMaster.cancelJob(1);
      Assert.fail("cannot cancel non-existing job");
    } catch (JobDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(1), e.getMessage());
    }
  }

  @Test
  public void cancelJobTest() throws Exception {
    JobCoordinator coordinator = Mockito.mock(JobCoordinator.class);
    Map<Long, JobCoordinator> map = Maps.newHashMap();
    long jobId = 1L;
    map.put(jobId, coordinator);
    Whitebox.setInternalState(mJobMaster, "mIdToJobCoordinator", map);
    mJobMaster.cancelJob(jobId);
    Mockito.verify(coordinator).cancel();
  }

  class DummyJobConfig implements JobConfig {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
      return "dummy";
    }
  }
}
