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

package alluxio.master.policy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.policy.PolicyMasterClient;
import alluxio.grpc.AddPolicyPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListPolicyPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.PolicyInfo;
import alluxio.grpc.WritePType;
import alluxio.job.JobIntegrationTest;
import alluxio.master.MasterClientContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.xattr.ExtendedAttribute;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for the ufsMigrate policy in policy engine.
 * It mounts a union UFS backed by two directories in local UFS.
 * Then tests whether the policy will migrate files from one local directory to the other after
 * a specified time.
 */
public class UfsMigratePolicyIntegrationTest extends JobIntegrationTest {
  private static final AlluxioURI MOUNT_POINT = new AlluxioURI("/union");
  private static final AlluxioURI UNION_UFS = new AlluxioURI("union:///");
  private static final String UFS_A_ALIAS = "a";
  private static final String UFS_B_ALIAS = "b";
  private static final String UFS_A_XATTR_KEY =
      ExtendedAttribute.PERSISTENCE_STATE.forId(UFS_A_ALIAS);
  private static final String UFS_B_XATTR_KEY =
      ExtendedAttribute.PERSISTENCE_STATE.forId(UFS_B_ALIAS);
  private static final String UNION_URI_A_KEY = "alluxio-union.a.uri";
  private static final String UNION_URI_B_KEY = "alluxio-union.b.uri";
  private static final String UNION_READ_PRIORITY_KEY = "alluxio-union.priority.read";
  private static final String UNION_READ_PRIORITY_VALUE = "a,b";
  private static final String UNION_CREATE_COLLECTION_KEY = "alluxio-union.collection.create";
  private static final String UNION_CREATE_COLLECTION_VALUE = "a";
  private static final int UFS_MIGRATE_WAIT_TIME_SECONDS = 1;
  private static final String UFS_MIGRATE_SHORTCUT = String.format(
      "ufsMigrate(%ds, UFS[a]:REMOVE, UFS[b]:STORE)", UFS_MIGRATE_WAIT_TIME_SECONDS);
  private static final String UFS_MIGRATE_ACTION = "DATA(UFS[a]:REMOVE, UFS[b]:STORE)";
  private static final CreateFilePOptions WRITE_THROUGH = CreateFilePOptions.newBuilder()
      .setWriteType(WritePType.THROUGH).build();
  private static final byte[] TEST_BYTES = "hello".getBytes();

  private FileSystemMaster mFileSystemMaster;
  private PolicyMaster mPolicyMaster;
  private PolicyMasterClient mPolicyClient;
  private File mSubUfsA;
  private File mSubUfsB;

  @Rule
  public final TemporaryFolder mTempFolder = new TemporaryFolder();

  /**
   * Creates a union UFS mount point containing two sub local UFSes.
   */
  private void createUnionUfsMountPoint() throws Exception {
    mSubUfsA = mTempFolder.newFolder();
    mSubUfsB = mTempFolder.newFolder();
    mFileSystem.mount(MOUNT_POINT, UNION_UFS, MountPOptions.newBuilder()
        .putProperties(UNION_URI_A_KEY, mSubUfsA.getPath())
        .putProperties(UNION_URI_B_KEY, mSubUfsB.getPath())
        .putProperties(UNION_READ_PRIORITY_KEY, UNION_READ_PRIORITY_VALUE)
        .putProperties(UNION_CREATE_COLLECTION_KEY, UNION_CREATE_COLLECTION_VALUE)
        .build());
  }

  /**
   * Creates a ufs migration policy for the mount point.
   */
  private void createPolicy() throws Exception {
    mPolicyClient.addPolicy(MOUNT_POINT.getPath(), UFS_MIGRATE_SHORTCUT,
        AddPolicyPOptions.getDefaultInstance());
  }

  /**
   * Checks that the policy metadata is correct.
   */
  private void checkPolicy() throws Exception {
    List<PolicyInfo> policies = mPolicyClient.listPolicy(ListPolicyPOptions.getDefaultInstance());
    assertEquals(1, policies.size());
    PolicyInfo policy = policies.get(0);
    assertEquals(MOUNT_POINT.getPath(), policy.getPath());
    assertEquals(UFS_MIGRATE_ACTION, policy.getAction());
  }

  @Before
  public void before() throws Exception {
    super.before();
    mFileSystemMaster = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
        .getMasterProcess().getMaster(FileSystemMaster.class);
    mPolicyMaster = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
        .getMasterProcess().getMaster(PolicyMaster.class);
    mPolicyClient = PolicyMasterClient.Factory.create(
        MasterClientContext.newBuilder(mFsContext.getClientContext()).build());
    createUnionUfsMountPoint();
  }

  @After
  public void after() throws Exception {
    mPolicyClient.close();
    super.after();
  }

  /**
   * Tests that the union UFS mount point setup is correct.
   */
  @Test
  public void setup() throws Exception {
    // Check mount table.
    Map<String, MountPointInfo> mountTable = mFileSystem.getMountTable();
    assertTrue(MOUNT_POINT + " should exist in mount table",
        mountTable.containsKey(MOUNT_POINT.getPath()));

    // Check mount point info.
    MountPointInfo mountPointInfo = mountTable.get(MOUNT_POINT.getPath());
    assertEquals(UNION_UFS.toString(), mountPointInfo.getUfsUri());
    assertEquals(4, mountPointInfo.getProperties().size());
    assertEquals(mSubUfsA.getPath(), mountPointInfo.getProperties().get(UNION_URI_A_KEY));
    assertEquals(mSubUfsB.getPath(), mountPointInfo.getProperties().get(UNION_URI_B_KEY));
    assertEquals(UNION_READ_PRIORITY_VALUE,
        mountPointInfo.getProperties().get(UNION_READ_PRIORITY_KEY));
    assertEquals(UNION_CREATE_COLLECTION_VALUE,
        mountPointInfo.getProperties().get(UNION_CREATE_COLLECTION_KEY));

    // Check file info.
    long fileId = mFileSystem.getStatus(MOUNT_POINT).getFileId();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(UNION_UFS.toString(), fileInfo.getUfsPath());
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(fileInfo, UFS_A_XATTR_KEY));
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(fileInfo, UFS_B_XATTR_KEY));
  }

  /**
   * Tests the policy to migrate a file from one sub UFS to another sub UFS under a union UFS mount
   * point.
   */
  @Test
  public void ufsMigrateFile() throws Exception {
    // Check the mount point's persistence state.
    long fileId = mFileSystem.getStatus(MOUNT_POINT).getFileId();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertTrue(MOUNT_POINT + " should be persisted", fileInfo.isPersisted());

    // Check the mount point's persistence state for sub UFS a.
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(fileInfo, UFS_A_XATTR_KEY));

    // Check the mount point's persistence state for sub UFS b.
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(fileInfo, UFS_B_XATTR_KEY));

    // Create a file in the mount point with THROUGH.
    AlluxioURI file = MOUNT_POINT.join("file");
    try (FileOutStream out = mFileSystem.createFile(file, WRITE_THROUGH)) {
      out.write(TEST_BYTES);
    }

    // Check that the file exists in sub UFS a.
    File[] files = mSubUfsA.listFiles();
    assertEquals(1, files.length);
    assertEquals(mSubUfsA.getPath() + "/" + file.getName(), files[0].getPath());

    // Check that the file does not exist in sub UFS b.
    files = mSubUfsB.listFiles();
    assertEquals(0, files.length);

    // Check the file's persistence state.
    fileId = mFileSystem.getStatus(file).getFileId();
    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertTrue(file + " should be persisted", fileInfo.isPersisted());

    // After deleting the inode from Alluxio, getStatus will reload metadata from UFS,
    // then xattr will appear in inode.
    // Since we don't set xattr in completeFile, without this hack, xattr will be empty in fileInfo.
    mFileSystem.delete(file, DeletePOptions.newBuilder().setAlluxioOnly(true).build());
    fileId = mFileSystem.getStatus(file).getFileId();
    fileInfo = mFileSystemMaster.getFileInfo(fileId);

    // Check the file's persistence state for sub UFS a.
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(fileInfo, UFS_A_XATTR_KEY));

    // Check the file's persistence state for sub UFS b.
    assertFalse("There should be no xattr for sub UFS b for file " + file,
        fileInfo.getXAttr().containsKey(UFS_B_XATTR_KEY));

    createPolicy();
    checkPolicy();
    mPolicyMaster.scanInodes();

    // Wait until inode scan triggers the ufs migration action.
    CommonUtils.waitFor(String.format("UFS migration to be started for file %s", file.getPath()),
        () -> mJobMaster.list().size() == 1,
        WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS));
    long sinceFileCreationTimeMs = System.currentTimeMillis() - fileInfo.getCreationTimeMs();
    assertTrue(String.format("UFS migration is started after the file exists for %d seconds",
        UFS_MIGRATE_WAIT_TIME_SECONDS),
        sinceFileCreationTimeMs >= UFS_MIGRATE_WAIT_TIME_SECONDS * Constants.SECOND_MS);

    // Wait until the persist job succeeds.
    long jobId = mJobMaster.list().get(0);
    waitForJobToFinish(jobId);

    // Wait for the ufs migration action to finish (commit).
    CommonUtils.waitFor(String.format("UFS migration for file %s to finish", file.getPath()),
        () -> mPolicyMaster.getActionScheduler().getExecutingActionsSize() == 0,
        WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS));

    // Check that the file does not exist in sub UFS a.
    files = mSubUfsA.listFiles();
    assertEquals(0, files.length);

    // Check that the file exists in sub UFS b.
    files = mSubUfsB.listFiles();
    assertEquals(1, files.length);
    assertEquals(mSubUfsB.getPath() + "/" + file.getName(), files[0].getPath());

    // Check the file's persistence state.
    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertTrue(file + " should be persisted", fileInfo.isPersisted());

    // Check the file's persistence state for sub UFS a.
    assertEquals(PersistenceState.NOT_PERSISTED, getPersistenceState(fileInfo, UFS_A_XATTR_KEY));

    // Check the file's persistence state for sub UFS b.
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(fileInfo, UFS_B_XATTR_KEY));
  }

  private PersistenceState getPersistenceState(FileInfo fileInfo, String xAttrKey) {
    return ExtendedAttribute.PERSISTENCE_STATE.decode(fileInfo.getXAttr().get(xAttrKey));
  }
}
