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
import alluxio.job.TestJobConfig;
import alluxio.job.exception.JobDoesNotExistException;
import alluxio.job.meta.JobInfo;
import alluxio.master.job.command.CommandManager;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.MutableJournal;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;
import java.util.Map;

/**
 * Tests {@link JobMaster}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JobCoordinator.class})
public final class JobMasterTest {
  private JobMaster mJobMaster;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    JournalFactory journalFactory =
        new MutableJournal.Factory(new URI(mTestFolder.newFolder().getAbsolutePath()));
    mJobMaster = new JobMaster(journalFactory);
    mJobMaster.start(true);
  }

  @After
  public void after() throws Exception {
    mJobMaster.stop();
  }

  @Test
  public void runNonExistingJobConfig() {
    try {
      mJobMaster.run(new DummyJobConfig());
      Assert.fail("cannot run non-existing job");
    } catch (JobDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage("dummy"),
          e.getMessage());
    }
  }

  @Test
  public void runJob() throws Exception {
    JobCoordinator coordinator = PowerMockito.mock(JobCoordinator.class);
    PowerMockito.mockStatic(JobCoordinator.class);
    Mockito.when(JobCoordinator.create(Mockito.any(CommandManager.class), Mockito.anyList(),
            Mockito.any(JobInfo.class), Mockito.any(JournalEntryWriter.class)))
        .thenReturn(coordinator);
    Map<Long, JobCoordinator> map = Maps.newHashMap();
    long jobId = 0L;
    map.put(jobId, coordinator);
    Whitebox.setInternalState(mJobMaster, "mIdToJobCoordinator", map);

    TestJobConfig jobConfig = new TestJobConfig("/test");
    mJobMaster.run(jobConfig);
    Assert.assertEquals(Lists.newArrayList(jobId), mJobMaster.list());
  }

  @Test
  public void cancelNonExistingJob() {
    try {
      mJobMaster.cancel(1);
      Assert.fail("cannot cancel non-existing job");
    } catch (JobDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(1), e.getMessage());
    }
  }

  @Test
  public void cancelJob() throws Exception {
    JobCoordinator coordinator = Mockito.mock(JobCoordinator.class);
    Map<Long, JobCoordinator> map = Maps.newHashMap();
    long jobId = 1L;
    map.put(jobId, coordinator);
    Whitebox.setInternalState(mJobMaster, "mIdToJobCoordinator", map);
    mJobMaster.cancel(jobId);
    Mockito.verify(coordinator).cancel();
  }

  static class DummyJobConfig implements JobConfig {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
      return "dummy";
    }
  }
}
