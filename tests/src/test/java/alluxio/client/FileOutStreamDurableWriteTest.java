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
import alluxio.IntegrationTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.TtlAction;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration tests on writing a file using {@link WriteType#DURABLE} by Alluxio Client.
 */
@Ignore
public final class FileOutStreamDurableWriteTest extends AbstractFileOutStreamIntegrationTest {
  private static final int LEN = 1024;

  private AlluxioURI mUri = new AlluxioURI(PathUtils.uniqPath());

  /**
   * Helper function to create a file of length LEN with {@link WriteType#DURABLE}.
   *
   * @return ths URIStatus of this file after creation
   */
  private URIStatus createDurableFile() throws Exception {
    writeIncreasingByteArrayToFile(mUri, LEN,
        CreateFileOptions.defaults().setWriteType(WriteType.DURABLE));

    return mFileSystem.getStatus(mUri);
  }

  @Test
  public void simpleDurableWrite() throws Exception {
    URIStatus status = createDurableFile();

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);

    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    checkFile(mUri, true, true, LEN);
  }

  @Test
  public void exists() throws Exception {
    IntegrationTestUtils.pauseAsyncPersist();

    createDurableFile();
    Assert.assertTrue(mFileSystem.exists(mUri));

    IntegrationTestUtils.resumeAsyncPersist();

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);

    Assert.assertTrue(mFileSystem.exists(mUri));
  }

  @Test
  public void deleteBeforePersisted() throws Exception {
    IntegrationTestUtils.pauseAsyncPersist();

    URIStatus status = createDurableFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
    mFileSystem.delete(mUri);

    Assert.assertFalse(mFileSystem.exists(mUri));
    Assert.assertFalse(ufs.exists(ufsPath));

    IntegrationTestUtils.resumeAsyncPersist();

    CommonUtils.sleepMs(1000);
    Assert.assertFalse(mFileSystem.exists(mUri));
    Assert.assertFalse(ufs.exists(ufsPath));
  }

  @Test
  public void deleteAfterPersisted() throws Exception {
    URIStatus status = createDurableFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);

    mFileSystem.delete(mUri);

    Assert.assertFalse(mFileSystem.exists(mUri));
    Assert.assertFalse(ufs.exists(ufsPath));
  }

  @Test
  public void freeBeforePersisted() throws Exception {
    IntegrationTestUtils.pauseAsyncPersist();

    createDurableFile();
    mFileSystem.free(mUri); // Expected to be a no-op

    IntegrationTestUtils.resumeAsyncPersist();
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);

    checkFile(mUri, true, true, LEN);
  }

  @Test
  public void freeAfterPersisted() throws Exception {
    URIStatus status = createDurableFile();
    long blockId = status.getBlockIds().get(0);

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);

    mFileSystem.free(mUri);

    IntegrationTestUtils
        .waitForBlocksToBeFreed(mLocalAlluxioClusterResource.get().getWorker().getBlockWorker(),
            blockId);

    status = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertEquals(0, status.getInMemoryPercentage());
    checkFile(mUri, false, true, LEN);
  }

  @Test
  public void getStatus() throws Exception {
    IntegrationTestUtils.pauseAsyncPersist();

    URIStatus statusBefore = createDurableFile();
    Assert
        .assertNotEquals(PersistenceState.PERSISTED.toString(), statusBefore.getPersistenceState());
    Assert.assertTrue(statusBefore.isCompleted());

    IntegrationTestUtils.resumeAsyncPersist();

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);

    URIStatus statusAfter = mFileSystem.getStatus(mUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), statusAfter.getPersistenceState());
    checkFile(mUri, true, true, LEN);
  }

  @Test
  public void openFile() throws Exception {
    IntegrationTestUtils.pauseAsyncPersist();

    createDurableFile();

    checkFile(mUri, true, false, LEN);

    IntegrationTestUtils.resumeAsyncPersist();

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, mUri);

    checkFile(mUri, true, true, LEN);
  }

  @Test
  public void renameBeforePersisted() throws Exception {
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());

    IntegrationTestUtils.pauseAsyncPersist();

    createDurableFile();
    mFileSystem.rename(mUri, newUri);

    IntegrationTestUtils.resumeAsyncPersist();

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);

    checkFile(newUri, true, true, LEN);
  }

  @Test
  public void renameAfterDurableWrite() throws Exception {
    AlluxioURI newUri = new AlluxioURI(PathUtils.uniqPath());

    createDurableFile();

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, newUri);

    mFileSystem.rename(mUri, newUri);

    URIStatus status = mFileSystem.getStatus(newUri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    checkFile(newUri, true, true, LEN);
  }

  @Test
  public void setAttributeBeforePersisted() throws Exception {
    Mode mode = new Mode((short) 0555);
    long ttl = 12345678L;
    TtlAction ttlAction = TtlAction.DELETE;

    SetAttributeOptions options =
        SetAttributeOptions.defaults().setMode(mode).setTtl(ttl).setTtlAction(ttlAction);

    IntegrationTestUtils.pauseAsyncPersist();

    URIStatus status = createDurableFile();
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);

    mFileSystem.setAttribute(mUri, options);

    IntegrationTestUtils.resumeAsyncPersist();

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

    URIStatus status = createDurableFile();
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
