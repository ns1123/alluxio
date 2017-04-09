/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.client.job.JobThriftClientUtils;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.MutableJournal;
import alluxio.master.privilege.PrivilegeMaster;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.wire.FileInfo;

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

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Random;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JobThriftClientUtils.class)
public final class PersistenceTest {
  private File mJournalFolder;
  private MasterRegistry mRegistry;
  private FileSystemMaster mFileSystemMaster;

  @Rule
  public ManuallyScheduleHeartbeat mManualScheduler =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_PERSISTENCE_CHECKER,
          HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);

  @Before
  public void before() throws Exception {
    AuthenticatedClientUser.set(LoginUser.get().getName());
    TemporaryFolder tmpFolder = new TemporaryFolder();
    tmpFolder.create();
    File ufsRoot = tmpFolder.newFolder();
    Configuration.set(PropertyKey.UNDERFS_ADDRESS, ufsRoot.getAbsolutePath());
    mJournalFolder = tmpFolder.newFolder();
    startServices();
  }

  @After
  public void after() throws Exception {
    stopServices();
    ConfigurationTestUtils.resetConfiguration();
    AuthenticatedClientUser.remove();
  }

  @Test
  public void empty() throws Exception {
    checkEmpty();
  }

  @Test
  public void heartbeatEmpty() throws Exception {
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
    checkEmpty();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
    checkEmpty();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
    checkEmpty();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
    checkEmpty();
  }

  /**
   * Tests the progression of a successful persist job.
   */
  @Test
  public void successfulAsyncPersistence() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    PowerMockito.mockStatic(JobThriftClientUtils.class);
    PowerMockito.doReturn(jobId)
        .when(JobThriftClientUtils.class, "start", Mockito.any(JobConfig.class));

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    JobInfo jobInfo = new JobInfo();
    jobInfo.setStatus(Status.CREATED);
    PowerMockito.doReturn(jobInfo).when(JobThriftClientUtils.class, "getStatus", Mockito.anyLong());

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    jobInfo.setStatus(Status.RUNNING);
    PowerMockito.doReturn(jobInfo).when(JobThriftClientUtils.class, "getStatus", Mockito.anyLong());

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction and create the temporary UFS file.
    {
      jobInfo.setStatus(Status.COMPLETED);
      PowerMockito.doReturn(jobInfo)
          .when(JobThriftClientUtils.class, "getStatus", Mockito.anyLong());
      fileInfo = mFileSystemMaster.getFileInfo(testFile);
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      PersistJob job = persistJobs.get(fileInfo.getFileId());
      UnderFileSystemUtils.touch(job.getTempUfsPath());
    }

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceCompleted(testFile);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceCompleted(testFile);
    }
  }

  /**
   * Tests that a canceled persist job is not retried.
   */
  @Test
  public void noRetryCanceled() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    PowerMockito.mockStatic(JobThriftClientUtils.class);
    PowerMockito.doReturn(jobId)
        .when(JobThriftClientUtils.class, "start", Mockito.any(JobConfig.class));

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    JobInfo jobInfo = new JobInfo();
    jobInfo.setStatus(Status.CANCELED);
    PowerMockito.doReturn(jobInfo).when(JobThriftClientUtils.class, "getStatus", Mockito.anyLong());

    // Execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkEmpty();
    }
  }

  /**
   * Tests that a failed persist job is retried multiple times.
   */
  @Test
  public void retryFailed() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    PowerMockito.mockStatic(JobThriftClientUtils.class);
    PowerMockito.doReturn(jobId)
        .when(JobThriftClientUtils.class, "start", Mockito.any(JobConfig.class));

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    JobInfo jobInfo = new JobInfo();
    jobInfo.setStatus(Status.FAILED);
    PowerMockito.doReturn(jobInfo).when(JobThriftClientUtils.class, "getStatus", Mockito.anyLong());

    // Repeatedly execute the persistence checker and scheduler heartbeats, checking the internal
    // state.
    for (int i = 0; i < 10; i++) {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceRequested(testFile);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
    }
  }

  /**
   * Tests that persist file requests are not forgotten across restarts.
   */
  @Test
  public void replayPersistRequest() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Simulate restart.
    stopServices();
    startServices();

    checkPersistenceRequested(testFile);
  }

  /**
   * Tests that persist file jobs are not forgotten across restarts.
   */
  @Test
  public void replayPersistJob() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());

    // Repeatedly schedule the async persistence, checking the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      checkPersistenceRequested(testFile);
    }

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    PowerMockito.mockStatic(JobThriftClientUtils.class);
    PowerMockito.doReturn(jobId)
        .when(JobThriftClientUtils.class, "start", Mockito.any(JobConfig.class));

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Simulate restart.
    stopServices();
    startServices();

    checkPersistenceInProgress(testFile, jobId);
  }

  private AlluxioURI createTestFile() throws Exception {
    AlluxioURI path = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));
    String owner = SecurityUtils.getOwnerFromThriftClient();
    String group = SecurityUtils.getGroupFromThriftClient();
    mFileSystemMaster.createFile(path, CreateFileOptions.defaults()
        .setOwner(owner).setGroup(group).setMode(Mode.createFullAccess()));
    mFileSystemMaster.completeFile(path, CompleteFileOptions.defaults());
    return path;
  }

  private void checkEmpty() {
    Assert.assertEquals(0, getPersistRequests().size());
    Assert.assertEquals(0, getPersistJobs().size());
  }

  private void checkPersistenceCompleted(AlluxioURI testFile) throws Exception {
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
    Map<Long, PersistJob> persistJobs = getPersistJobs();
    Assert.assertEquals(0, getPersistRequests().size());
    Assert.assertEquals(0, persistJobs.size());
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  private void checkPersistenceInProgress(AlluxioURI testFile, long jobId) throws Exception {
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
    Map<Long, PersistJob> persistJobs = getPersistJobs();
    Assert.assertEquals(0, getPersistRequests().size());
    Assert.assertEquals(1, persistJobs.size());
    Assert.assertTrue(persistJobs.containsKey(fileInfo.getFileId()));
    PersistJob job = persistJobs.get(fileInfo.getFileId());
    Assert.assertEquals(fileInfo.getFileId(), job.getFileId());
    Assert.assertEquals(jobId, job.getJobId());
    Assert.assertTrue(job.getTempUfsPath().contains(testFile.getPath()));
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  private void checkPersistenceRequested(AlluxioURI testFile) throws Exception {
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
    Set<Long> persistRequests = getPersistRequests();
    Assert.assertEquals(1, persistRequests.size());
    Assert.assertEquals(0, getPersistJobs().size());
    Assert.assertTrue(persistRequests.contains(fileInfo.getFileId()));
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  private Set<Long> getPersistRequests() {
    return (Set<Long>) Whitebox.getInternalState(mFileSystemMaster, "mPersistRequests");
  }

  private Map<Long, PersistJob> getPersistJobs() {
    return (Map<Long, PersistJob>) Whitebox.getInternalState(mFileSystemMaster, "mPersistJobs");
  }

  private void startServices() throws Exception {
    mRegistry = new MasterRegistry();
    JournalFactory journalFactory =
        new MutableJournal.Factory(new URI(mJournalFolder.getAbsolutePath()));
    new PrivilegeMaster(mRegistry, journalFactory);
    new BlockMaster(mRegistry, journalFactory);
    mFileSystemMaster = new FileSystemMaster(mRegistry, journalFactory);
    mRegistry.start(true);
  }

  private void stopServices() throws Exception {
    mRegistry.stop();
  }
}
