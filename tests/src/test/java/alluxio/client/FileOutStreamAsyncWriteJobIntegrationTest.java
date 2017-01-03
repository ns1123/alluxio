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
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.TtlAction;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration tests for {@link FileOutStream} of under storage type being async persist.
 */
public final class FileOutStreamAsyncWriteJobIntegrationTest
    extends AbstractFileOutStreamIntegrationTest {
  private static final int LEN = 1024;

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
    PersistenceTestUtils.pauseAsyncPersist(mLocalAlluxioClusterResource);

    URIStatus status = createAsyncFile();
    // check the file is completed but not persisted
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    PersistenceTestUtils.resumeAsyncPersist(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void exists() throws Exception {
    PersistenceTestUtils.pauseAsyncPersist(mLocalAlluxioClusterResource);
    createAsyncFile();
    Assert.assertTrue(mFileSystem.exists(mUri));
    PersistenceTestUtils.resumeAsyncPersist(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    Assert.assertTrue(mFileSystem.exists(mUri));
  }

  @Test
  public void deleteBeforePersisted() throws Exception {
    PersistenceTestUtils.pauseAsyncPersist(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
    mFileSystem.delete(mUri);
    Assert.assertFalse(mFileSystem.exists(mUri));
    Assert.assertFalse(ufs.exists(ufsPath));
    PersistenceTestUtils.resumeAsyncPersist(mLocalAlluxioClusterResource);
    CommonUtils.sleepMs(1000);
    Assert.assertFalse(mFileSystem.exists(mUri));
    Assert.assertFalse(ufs.exists(ufsPath));
  }

  @Test
  public void deleteAfterPersisted() throws Exception {
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    mFileSystem.delete(mUri);
    Assert.assertFalse(mFileSystem.exists(mUri));
    Assert.assertFalse(ufs.exists(ufsPath));
  }

  @Test
  @Ignore
  public void freeBeforePersisted() throws Exception {
    PersistenceTestUtils.pauseAsyncPersist(mLocalAlluxioClusterResource);
    createAsyncFile();
    mFileSystem.free(mUri); // Expected to be a no-op
    PersistenceTestUtils.resumeAsyncPersist(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void freeAfterPersisted() throws Exception {
    URIStatus status = createAsyncFile();
    long blockId = status.getBlockIds().get(0);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    mFileSystem.free(mUri);
    IntegrationTestUtils
        .waitForBlocksToBeFreed(mLocalAlluxioClusterResource.get().getWorker().getBlockWorker(), blockId);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertEquals(0, status.getInMemoryPercentage());
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void getStatus() throws Exception {
    PersistenceTestUtils.pauseAsyncPersist(mLocalAlluxioClusterResource);
    URIStatus statusBefore = createAsyncFile();
    Assert
        .assertNotEquals(PersistenceState.PERSISTED.toString(), statusBefore.getPersistenceState());
    Assert.assertTrue(statusBefore.isCompleted());
    PersistenceTestUtils.resumeAsyncPersist(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    URIStatus statusAfter = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), statusAfter.getPersistenceState());
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void openFile() throws Exception {
    PersistenceTestUtils.pauseAsyncPersist(mLocalAlluxioClusterResource);
    createAsyncFile();
    checkFileInAlluxio(mUri, LEN);
    PersistenceTestUtils.resumeAsyncPersist(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    checkFileInAlluxio(mUri, LEN);
    checkFileInUnderStorage(mUri, LEN);
  }

  @Test
  public void renameBeforePersisted() throws Exception {
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    PersistenceTestUtils.pauseAsyncPersist(mLocalAlluxioClusterResource);
    createAsyncFile();
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    PersistenceTestUtils.resumeAsyncPersist(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
  }

  @Test
  public void renameAfterDurableWrite() throws Exception {
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());
    createAsyncFile();
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    mFileSystem.createDirectory(newUri.getParent());
    mFileSystem.rename(mUri, newUri);
    URIStatus status = mFileSystem.getStatus(newUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    checkFileInAlluxio(newUri, LEN);
    checkFileInUnderStorage(newUri, LEN);
  }

  @Test
  public void setAttributeBeforePersisted() throws Exception {
    Mode mode = new Mode((short) 0555);
    long ttl = 12345678L;
    TtlAction ttlAction = TtlAction.DELETE;
    SetAttributeOptions options =
        SetAttributeOptions.defaults().setMode(mode).setTtl(ttl).setTtlAction(ttlAction);
    PersistenceTestUtils.pauseAsyncPersist(mLocalAlluxioClusterResource);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
    mFileSystem.setAttribute(mUri, options);
    PersistenceTestUtils.resumeAsyncPersist(mLocalAlluxioClusterResource);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(mode.toShort(), status.getMode());
    Assert.assertEquals(ttl, status.getTtl());
    Assert.assertEquals(ttlAction, status.getTtlAction());
    Assert.assertEquals(mode.toShort(), ufs.getMode(ufsPath));
  }

  @Test
  public void setAttributeAfterPersisted() throws Exception {
    Mode mode = new Mode((short) 0555);
    long ttl = 12345678L;
    TtlAction ttlAction = TtlAction.DELETE;
    SetAttributeOptions options =
        SetAttributeOptions.defaults().setMode(mode).setTtl(ttl).setTtlAction(ttlAction);
    URIStatus status = createAsyncFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);
    mFileSystem.setAttribute(mUri, options);
    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(mode.toShort(), status.getMode());
    Assert.assertEquals(ttl, status.getTtl());
    Assert.assertEquals(ttlAction, status.getTtlAction());
    Assert.assertEquals(mode.toShort(), ufs.getMode(ufsPath));
  }
}
