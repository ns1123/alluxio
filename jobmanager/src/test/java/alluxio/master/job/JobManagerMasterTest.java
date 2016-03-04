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
import alluxio.job.load.DistributedSingleFileLoadingConfig;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.Journal;

import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Maps;
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
 * Tests {@link JobManagerMaster}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class, BlockMaster.class, JobCoordinator.class})
public final class JobManagerMasterTest {
  private JobManagerMaster mJobManagerMaster;
  private FileSystemMaster mFileSystemMaster;
  private BlockMaster mBlockMaster;

  @Before
  public void before() {
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mBlockMaster = Mockito.mock(BlockMaster.class);
    mJobManagerMaster =
        new JobManagerMaster(mFileSystemMaster, mBlockMaster, Mockito.mock(Journal.class));
  }

  @Test
  public void runNonExistingJobConfigTest() {
    try {
      mJobManagerMaster.runJob(new DummyJobConfig());
      Assert.fail("cannot run non-existing job");
    } catch (JobDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage("dummy"),
          e.getMessage());
    }
  }

  @Test
  public void runJobTest() throws Exception {
    DistributedSingleFileLoadingConfig jobConfig = new DistributedSingleFileLoadingConfig("/test");
    long jobId = mJobManagerMaster.runJob(jobConfig);
    Assert.assertEquals(Lists.newArrayList(jobId), mJobManagerMaster.listJobs());
    // job coordinator calls blocks to dispatch workers
    Mockito.verify(mBlockMaster).getWorkerInfoList();
  }

  @Test
  public void cancelNonExistingJobTest() {
    try {
      mJobManagerMaster.cancelJob(1);
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
    Whitebox.setInternalState(mJobManagerMaster, "mIdToJobCoordinator", map);
    mJobManagerMaster.cancelJob(jobId);
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
