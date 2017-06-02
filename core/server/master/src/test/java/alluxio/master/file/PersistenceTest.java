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
import alluxio.client.job.JobMasterClient;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.GetStatusOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;
import alluxio.master.privilege.PrivilegeMasterFactory;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.time.ExponentialTimer;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.FileInfo;

import com.google.common.base.Function;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JobMasterClient.Factory.class)
public final class PersistenceTest {
  private File mJournalFolder;
  private MasterRegistry mRegistry;
  private FileSystemMaster mFileSystemMaster;
  private JobMasterClient mMockJobMasterClient;
  private static final GetStatusOptions GET_STATUS_OPTIONS = GetStatusOptions.defaults();

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
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, ufsRoot.getAbsolutePath());
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS, 0);
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_MAX_INTERVAL_MS, 1000);
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_INITIAL_WAIT_TIME_MS, 0);
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS, 1000);
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
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
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
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

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
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    jobInfo.setStatus(Status.RUNNING);
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceInProgress(testFile, jobId);
    }

    // Mock the job service interaction.
    jobInfo.setStatus(Status.COMPLETED);
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    {
      // Create the temporary UFS file.
      fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      PersistJob job = persistJobs.get(fileInfo.getFileId());
      UnderFileSystem ufs = UnderFileSystem.Factory.create(job.getTempUfsPath());
      UnderFileSystemUtils.touch(ufs, job.getTempUfsPath());
    }

    // Repeatedly execute the persistence checker heartbeat, checking the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      waitUntilPersisted(testFile);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      waitUntilPersisted(testFile);
    }
  }

  /**
   * Tests that a canceled persist job is not retried.
   */
  @Test
  public void noRetryCanceled() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
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
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

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
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

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
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
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
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

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
    Mockito.when(mMockJobMasterClient.getStatus(Mockito.anyLong())).thenReturn(jobInfo);

    // Repeatedly execute the persistence checker and scheduler heartbeats, checking the internal
    // state. After the internal timeout associated with the operation expires, check the operation
    // has been cancelled.
    while (true) {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      checkPersistenceRequested(testFile);
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      if (getPersistJobs().size() != 0) {
        checkPersistenceInProgress(testFile, jobId);
      } else {
        checkEmpty();
        break;
      }
      CommonUtils.sleepMs(100);
    }
    fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  /**
   * Tests that persist file requests are not forgotten across restarts.
   */
  @Test
  public void replayPersistRequest() throws Exception {
    // Create a file and check the internal state.
    AlluxioURI testFile = createTestFile();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
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
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
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
    Mockito.when(mMockJobMasterClient.run(Mockito.any(JobConfig.class))).thenReturn(jobId);

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

  private void waitUntilPersisted(final AlluxioURI testFile) throws Exception {
    // Persistence completion is asynchronous, so waiting is necessary.
    CommonUtils.waitFor("async persistence is completed for file", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        try {
          FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
          return fileInfo.getPersistenceState() == PersistenceState.PERSISTED.toString();
        } catch (FileDoesNotExistException | InvalidPathException | AccessControlException e) {
          return false;
        }
      }
    }, WaitForOptions.defaults().setTimeoutMs(30000));

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
    Map<Long, PersistJob> persistJobs = getPersistJobs();
    Assert.assertEquals(0, getPersistRequests().size());
    Assert.assertEquals(0, persistJobs.size());
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  private void checkPersistenceInProgress(AlluxioURI testFile, long jobId) throws Exception {
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
    Map<Long, PersistJob> persistJobs = getPersistJobs();
    Assert.assertEquals(0, getPersistRequests().size());
    Assert.assertEquals(1, persistJobs.size());
    Assert.assertTrue(persistJobs.containsKey(fileInfo.getFileId()));
    PersistJob job = persistJobs.get(fileInfo.getFileId());
    Assert.assertEquals(fileInfo.getFileId(), job.getFileId());
    Assert.assertEquals(jobId, job.getId());
    Assert.assertTrue(job.getTempUfsPath().contains(testFile.getPath()));
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  private void checkPersistenceRequested(AlluxioURI testFile) throws Exception {
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile, GET_STATUS_OPTIONS);
    Map<Long, ExponentialTimer> persistRequests = getPersistRequests();
    Assert.assertEquals(1, persistRequests.size());
    Assert.assertEquals(0, getPersistJobs().size());
    Assert.assertTrue(persistRequests.containsKey(fileInfo.getFileId()));
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), fileInfo.getPersistenceState());
  }

  private Map<Long, ExponentialTimer> getPersistRequests() {
    FileSystemMaster nestedFileSystemMaster =
        Whitebox.getInternalState(mFileSystemMaster, "mFileSystemMaster");
    return Whitebox.getInternalState(nestedFileSystemMaster, "mPersistRequests");
  }

  private Map<Long, PersistJob> getPersistJobs() {
    FileSystemMaster nestedFileSystemMaster =
        Whitebox.getInternalState(mFileSystemMaster, "mFileSystemMaster");
    return Whitebox.getInternalState(nestedFileSystemMaster, "mPersistJobs");
  }

  private void startServices() throws Exception {
    mRegistry = new MasterRegistry();
    JournalFactory journalFactory = new Journal.Factory(new URI(mJournalFolder.getAbsolutePath()));
    new PrivilegeMasterFactory().create(mRegistry, journalFactory);
    new BlockMasterFactory().create(mRegistry, journalFactory);
    mFileSystemMaster = new FileSystemMasterFactory().create(mRegistry, journalFactory);
    mRegistry.start(true);
    mMockJobMasterClient = Mockito.mock(JobMasterClient.class);
    PowerMockito.mockStatic(JobMasterClient.Factory.class);
    Mockito.when(JobMasterClient.Factory.create()).thenReturn(mMockJobMasterClient);
  }

  private void stopServices() throws Exception {
    mRegistry.stop();
  }
}
