package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.job.JobConfig;
import alluxio.job.util.JobRestClientUtils;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Permission;
import alluxio.util.CommonUtils;
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
import java.util.Map;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JobRestClientUtils.class)
public class PersistenceTest {
  private File mUfsRoot;
  private BlockMaster mBlockMaster;
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
    mUfsRoot = tmpFolder.newFolder();
    File journalFolder = tmpFolder.newFolder();
    Configuration.set(PropertyKey.UNDERFS_ADDRESS, mUfsRoot.getAbsolutePath());
    JournalFactory journalFactory = new JournalFactory.ReadWrite(journalFolder.getAbsolutePath());
    mBlockMaster = new BlockMaster(journalFactory);
    mBlockMaster.start(true);
    mFileSystemMaster = new FileSystemMaster(mBlockMaster, journalFactory);
    mFileSystemMaster.start(true);
  }

  @After
  public void after() throws Exception {
    mFileSystemMaster.stop();
    mBlockMaster.stop();
    ConfigurationTestUtils.resetConfiguration();
    AuthenticatedClientUser.remove();
  }

  @Test
  public void empty() throws Exception {
    Assert.assertEquals(0, getPersistRequests().size());
    Assert.assertEquals(0, getPersistJobs().size());
  }

  @Test
  public void successfulAsyncPersistence() throws Exception {
    // Create a file and request its persistence.
    AlluxioURI testFile = createTestFile();

    // Check the internal state.
    {
      FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
      Assert
          .assertEquals(PersistenceState.NOT_PERSISTED.toString(), fileInfo.getPersistenceState());
    }

    // Schedule async persistence and check the internal state.
    {
      mFileSystemMaster.scheduleAsyncPersistence(testFile);
      FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
      Map<Long, PersistRequest> persistRequests = getPersistRequests();
      Assert.assertEquals(1, persistRequests.size());
      Assert.assertEquals(0, getPersistJobs().size());
      Assert.assertTrue(persistRequests.containsKey(fileInfo.getFileId()));
      PersistRequest request = persistRequests.get(fileInfo.getFileId());
      Assert.assertEquals(fileInfo.getFileId(), request.getFileId());
      Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
          fileInfo.getPersistenceState());
    }

    // Mock the job service interaction.
    Random random = new Random();
    long jobId = random.nextLong();
    PowerMockito.mockStatic(JobRestClientUtils.class);
    PowerMockito.doReturn(jobId)
        .when(JobRestClientUtils.class, "runJob", Mockito.any(JobConfig.class));

    // Execute the persistence scheduler heartbeat and check the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      Assert.assertEquals(0, getPersistRequests().size());
      Assert.assertEquals(1, persistJobs.size());
      Assert.assertTrue(persistJobs.containsKey(fileInfo.getFileId()));
      PersistJob job = persistJobs.get(fileInfo.getFileId());
      Assert.assertEquals(fileInfo.getFileId(), job.getFileId());
      Assert.assertEquals(jobId, job.getJobId());
      Assert.assertTrue(job.getTempUfsPath().contains(testFile.getPath()));
      Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
          fileInfo.getPersistenceState());
    }

    // Execute the persistence scheduler heartbeat again and check the internal state (no change).
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
      FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      Assert.assertEquals(0, getPersistRequests().size());
      Assert.assertEquals(1, persistJobs.size());
      Assert.assertTrue(persistJobs.containsKey(fileInfo.getFileId()));
      PersistJob job = persistJobs.get(fileInfo.getFileId());
      Assert.assertEquals(fileInfo.getFileId(), job.getFileId());
      Assert.assertEquals(jobId, job.getJobId());
      Assert.assertTrue(job.getTempUfsPath().contains(testFile.getPath()));
      Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
          fileInfo.getPersistenceState());
    }

    // Mock the job service interaction.
    JobInfo jobInfo = new JobInfo();
    jobInfo.setStatus(Status.CREATED);
    PowerMockito.doReturn(jobInfo)
        .when(JobRestClientUtils.class, "getJobInfo", Mockito.anyLong());

    // Execute the persistence checker heartbeat and check the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      Assert.assertEquals(0, getPersistRequests().size());
      Assert.assertEquals(1, persistJobs.size());
      Assert.assertTrue(persistJobs.containsKey(fileInfo.getFileId()));
      PersistJob job = persistJobs.get(fileInfo.getFileId());
      Assert.assertEquals(fileInfo.getFileId(), job.getFileId());
      Assert.assertEquals(jobId, job.getJobId());
      Assert.assertTrue(job.getTempUfsPath().contains(testFile.getPath()));
      Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
          fileInfo.getPersistenceState());
    }

    // Mock the job service interaction.
    jobInfo.setStatus(Status.RUNNING);
    PowerMockito.doReturn(jobInfo)
        .when(JobRestClientUtils.class, "getJobInfo", Mockito.anyLong());

    // Execute the persistence checker heartbeat and check the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      Assert.assertEquals(0, getPersistRequests().size());
      Assert.assertEquals(1, persistJobs.size());
      Assert.assertTrue(persistJobs.containsKey(fileInfo.getFileId()));
      PersistJob job = persistJobs.get(fileInfo.getFileId());
      Assert.assertEquals(fileInfo.getFileId(), job.getFileId());
      Assert.assertEquals(jobId, job.getJobId());
      Assert.assertTrue(job.getTempUfsPath().contains(testFile.getPath()));
      Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
          fileInfo.getPersistenceState());
    }

    // Mock the job service interaction.
    jobInfo.setStatus(Status.COMPLETED);
    PowerMockito.doReturn(jobInfo)
        .when(JobRestClientUtils.class, "getJobInfo", Mockito.anyLong());

    // Create the temporary UFS file.
    {
      FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      PersistJob job = persistJobs.get(fileInfo.getFileId());
      UnderFileSystemUtils.touch(job.getTempUfsPath());
    }

    // Execute the persistence checker heartbeat and check the internal state.
    {
      HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_CHECKER);
      FileInfo fileInfo = mFileSystemMaster.getFileInfo(testFile);
      Map<Long, PersistJob> persistJobs = getPersistJobs();
      Assert.assertEquals(0, getPersistRequests().size());
      Assert.assertEquals(0, persistJobs.size());
      Assert.assertEquals(PersistenceState.PERSISTED.toString(), fileInfo.getPersistenceState());
    }
  }

  @Test
  public void heartbeatWhenEmpty() throws Exception {
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER);
    Assert.assertEquals(0, getPersistRequests().size());
    Assert.assertEquals(0, getPersistJobs().size());
  }

  private AlluxioURI createTestFile() throws Exception {
    AlluxioURI path = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));
    mFileSystemMaster.createFile(path, CreateFileOptions.defaults()
        .setPermission(Permission.defaults().setOwnerFromThriftClient()));
    mFileSystemMaster.completeFile(path, CompleteFileOptions.defaults());
    return path;
  }

  private Map<Long, PersistRequest> getPersistRequests() {
    return (Map<Long, PersistRequest>) Whitebox
        .getInternalState(mFileSystemMaster, "mPersistRequests");
  }

  private Map<Long, PersistJob> getPersistJobs() {
    return (Map<Long, PersistJob>) Whitebox.getInternalState(mFileSystemMaster, "mPersistJobs");
  }
}
