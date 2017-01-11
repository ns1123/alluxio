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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.IntegrationTestUtils;
import alluxio.PersistenceTestUtils;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.wire.TtlAction;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for {@link FileOutStream} of under storage type being async persist.
 */
public final class FileOutStreamAsyncWriteJobIntegrationTest
    extends AbstractFileOutStreamIntegrationTest {
  private static final int LEN = 1024;
  private static final SetAttributeOptions TEST_OPTIONS =
      SetAttributeOptions.defaults().setMode(new Mode((short) 0555)).setTtl(12345678L)
          .setTtlAction(TtlAction.DELETE);

  private AlluxioURI mUri = new AlluxioURI(PathUtils.uniqPath());

  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  /**
   * Helper function to create a file of length LEN with {@link WriteType#ASYNC_THROUGH}.
   *
   * @return ths URIStatus of this file after creation
   */
  private URIStatus createAsyncFile() throws Exception {
    writeIncreasingByteArrayToFile(mUri, LEN,
        CreateFileOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH));
    return mFileSystem.getStatus(mUri);
  }

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

  @Before
  @Override
  public void before() throws Exception {
    super.before();
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
  }

  @After
  public void after() throws Exception {
    mLocalAlluxioJobCluster.stop();
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void simpleDurableWrite() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);

    URIStatus status = createAsyncFile();
    // check the file is completed but not persisted
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void exists() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    createAsyncFile();
    Assert.assertTrue(mFileSystem.exists(mUri));

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    Assert.assertTrue(mFileSystem.exists(mUri));

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    Assert.assertTrue(mFileSystem.exists(mUri));
  }

  @Test
  public void deleteBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    mFileSystem.delete(mUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobComplete(mLocalAlluxioClusterResource, status.getFileId());
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void deleteAfterJobScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    mFileSystem.delete(mUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobComplete(mLocalAlluxioClusterResource, status.getFileId());
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void freeBeforeJobScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    free(mUri);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(100, status.getInMemoryPercentage());
    checkFileInAlluxio(mUri, LEN);
    checkFileNotInUnderStorage(status.getUfsPath());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(100, status.getInMemoryPercentage());
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void freeAfterJobScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    free(mUri);
    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void getStatus() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus statusBefore = createAsyncFile();
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
        statusBefore.getPersistenceState());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
        statusBefore.getPersistenceState());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    URIStatus statusAfter = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), statusAfter.getPersistenceState());
  }

  @Test
  public void openFile() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    createAsyncFile();
    checkFileInAlluxio(mUri, LEN);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    checkFileInAlluxio(mUri, LEN);

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void renameBeforeScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
    checkFileNotInUnderStorage(mFileSystem.getStatus(newUri).getUfsPath());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void renameAfterScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
    checkFileNotInUnderStorage(mFileSystem.getStatus(newUri).getUfsPath());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(status.getUfsPath());
  }

  @Test
  public void setAttributeBeforeScheduled() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
    mFileSystem.setAttribute(mUri, TEST_OPTIONS);
    checkFileInAlluxio(mUri, LEN);
    checkFileNotInUnderStorage(status.getUfsPath());
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(TEST_OPTIONS.getMode().toShort(), status.getMode());
    Assert.assertEquals(TEST_OPTIONS.getTtl().longValue(), status.getTtl());
    Assert.assertEquals(TEST_OPTIONS.getTtlAction(), status.getTtlAction());

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(TEST_OPTIONS.getMode().toShort(), status.getMode());
    Assert.assertEquals(TEST_OPTIONS.getTtl().longValue(), status.getTtl());
    Assert.assertEquals(TEST_OPTIONS.getTtlAction(), status.getTtlAction());
    Assert.assertEquals(TEST_OPTIONS.getMode().toShort(), ufs.getMode(ufsPath));
  }

  @Test
  public void setAttributeAfterScheduled() throws Exception {
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
    mFileSystem.setAttribute(mUri, TEST_OPTIONS);
    checkFileInAlluxio(mUri, LEN);
    checkFileNotInUnderStorage(status.getUfsPath());
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(TEST_OPTIONS.getMode().toShort(), status.getMode());
    Assert.assertEquals(TEST_OPTIONS.getTtl().longValue(), status.getTtl());
    Assert.assertEquals(TEST_OPTIONS.getTtlAction(), status.getTtlAction());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(TEST_OPTIONS.getMode().toShort(), status.getMode());
    Assert.assertEquals(TEST_OPTIONS.getTtl().longValue(), status.getTtl());
    Assert.assertEquals(TEST_OPTIONS.getTtlAction(), status.getTtlAction());
    Assert.assertEquals(TEST_OPTIONS.getMode().toShort(), ufs.getMode(ufsPath));
  }

  @Test
  public void renameScheduleRename() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    AlluxioURI newUri1 = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri1.getParent());
    mFileSystem.rename(mUri, newUri1);
    String ufsPath1 = mFileSystem.getStatus(newUri1).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri1, LEN);
    checkFileNotInUnderStorage(ufsPath1);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    AlluxioURI newUri2 = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.rename(newUri1, newUri2);
    String ufsPath2 = mFileSystem.getStatus(newUri2).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(newUri1);
    checkFileNotInUnderStorage(ufsPath1);
    checkFileInAlluxio(newUri2, LEN);
    checkFileNotInUnderStorage(ufsPath2);

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri2);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(newUri1);
    checkFileNotInUnderStorage(ufsPath1);
    checkFileInAlluxio(newUri2, LEN);
    checkFileInUnderStorage(newUri2, LEN);
  }

  @Test
  public void renameScheduleFree() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    String newUfsPath = mFileSystem.getStatus(newUri).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    free(newUri);  // Expected to be a no-op
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);
    Assert.assertEquals(100, mFileSystem.getStatus(newUri).getInMemoryPercentage());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
    Assert.assertEquals(100, mFileSystem.getStatus(newUri).getInMemoryPercentage());
  }

  @Test
  public void renameScheduleSetAttribute() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    String newUfsPath = mFileSystem.getStatus(newUri).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    mFileSystem.setAttribute(newUri, TEST_OPTIONS);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);
    status = mFileSystem.getStatus(newUri);
    Assert.assertEquals(TEST_OPTIONS.getMode().toShort(), status.getMode());
    Assert.assertEquals(TEST_OPTIONS.getTtl().longValue(), status.getTtl());
    Assert.assertEquals(TEST_OPTIONS.getTtlAction(), status.getTtlAction());

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
    status = mFileSystem.getStatus(newUri);
    Assert.assertEquals(TEST_OPTIONS.getMode().toShort(), status.getMode());
    Assert.assertEquals(TEST_OPTIONS.getTtl().longValue(), status.getTtl());
    Assert.assertEquals(TEST_OPTIONS.getTtlAction(), status.getTtlAction());
    Assert.assertEquals(TEST_OPTIONS.getMode().toShort(), ufs.getMode(newUfsPath));
  }

  @Test
  public void renameScheduleDelete() throws Exception {
    PersistenceTestUtils.pauseScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.pauseChecker(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    String newUfsPath = mFileSystem.getStatus(newUri).getUfsPath();
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileInAlluxio(newUri, LEN);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeScheduler(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobScheduled(mLocalAlluxioClusterResource, status.getFileId());
    mFileSystem.delete(newUri);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(newUfsPath);

    PersistenceTestUtils.resumeChecker(mLocalAlluxioClusterResource);
    PersistenceTestUtils.waitForJobComplete(mLocalAlluxioClusterResource, status.getFileId());
    checkFileNotInAlluxio(mUri);
    checkFileNotInUnderStorage(ufsPath);
    checkFileNotInAlluxio(newUri);
    checkFileNotInUnderStorage(newUfsPath);
  }

  /**
   * Checks the given file exists in Alluxio storage and expects its content to be an increasing
   * array of the given length.
   *
   * @param filePath path of the tmp file
   */
  private void checkFileNotInAlluxio(AlluxioURI filePath) throws Exception {
    Assert.assertFalse(mFileSystem.exists(filePath));
  }

  /**
   * Checks the given file exists in Alluxio storage and expects its content to be an increasing
   * array of the given length.
   *
   * @param ufsPath path of the tmp file
   */
  private void checkFileNotInUnderStorage(String ufsPath) throws Exception {
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
    Assert.assertFalse(ufs.exists(ufsPath));
  }

  /**
   * Helper method to ensure an Alluxio file is freed.
   *
   * @param uri Alluxio Uri to free
   */
  private void free(AlluxioURI uri) throws Exception {
    mFileSystem.free(uri); // Expected to be a no-op
    IntegrationTestUtils
        .waitForBlocksToBeFreed(mLocalAlluxioClusterResource.get().getWorker().getBlockWorker());
  }
}
