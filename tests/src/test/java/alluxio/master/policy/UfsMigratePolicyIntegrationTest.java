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
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.policy.PolicyMasterClient;
import alluxio.grpc.AddPolicyPOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListPolicyPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.PolicyInfo;
import alluxio.grpc.WritePType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
  private static final CreateDirectoryPOptions DIR_WRITE_THROUGH = CreateDirectoryPOptions
      .newBuilder().setWriteType(WritePType.THROUGH).build();
  private static final CreateFilePOptions FILE_WRITE_THROUGH = CreateFilePOptions.newBuilder()
      .setWriteType(WritePType.THROUGH).build();
  private static final byte[] TEST_BYTES = "hello".getBytes();

  private FileSystemMaster mFileSystemMaster;
  private PolicyMaster mPolicyMaster;
  private PolicyMasterClient mPolicyClient;
  private File mSubUfsA;
  private File mSubUfsB;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_POLICY_ACTION_SCHEDULER);

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

  /**
   * Triggers the policy by scan.
   */
  private void triggerPolicy() {
    mPolicyMaster.scanInodes();
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
    FileInfo fileInfo = getFileInfo(MOUNT_POINT);
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
    FileInfo fileInfo = getFileInfo(MOUNT_POINT);
    assertTrue(MOUNT_POINT + " should be persisted", fileInfo.isPersisted());
    // Check the mount point's persistence state for sub UFS a.
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(fileInfo, UFS_A_XATTR_KEY));
    // Check the mount point's persistence state for sub UFS b.
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(fileInfo, UFS_B_XATTR_KEY));

    AlluxioURI file = MOUNT_POINT.join("file");
    createFile(file);
    testUfsMigratePath(file);
  }

  /**
   * Tests the policy to migrate one directory from one sub UFS to another sub UFS under a union
   * UFS mount point.
   */
  @Test
  public void ufsMigrateDirectory() throws Exception {
    AlluxioURI dir = MOUNT_POINT.join("dir");
    createDirectory(dir);
    testUfsMigratePath(dir);
  }

  /**
   * Tests that a nested directory with files can be migrated from one sub UFS to another one under
   * a union UFS mount point.
   */
  @Test
  public void ufsMigrateNestedDirectory() throws Exception {
    // Directory structure:
    // /union/file
    // /union/dir
    // /union/dir/file
    // /union/dir/empty-dir
    AlluxioURI[] uris = new AlluxioURI[] {
        MOUNT_POINT.join("file"),
        MOUNT_POINT.join("dir"),
        MOUNT_POINT.join("dir/file"),
        MOUNT_POINT.join("dir/empty-dir")
    };
    createFile(uris[0]);
    createDirectory(uris[1]);
    createFile(uris[2]);
    createDirectory(uris[3]);

    // Check persistence state.
    for (AlluxioURI uri : uris) {
      FileInfo info = getFileInfo(uri);
      assertTrue(info.isPersisted());
      assertEquals(PersistenceState.PERSISTED, getPersistenceState(info, UFS_A_XATTR_KEY));
      assertEquals(PersistenceState.NOT_PERSISTED, getPersistenceState(info, UFS_B_XATTR_KEY));
    }

    createPolicy();
    triggerPolicy();
    waitForUfsMigration(5, mFileSystem.getStatus(uris[uris.length - 1])
        .getCreationTimeMs());

    // All files and empty directories should have been removed from sub UFS A.
    // /union/dir might be removed if /union/dir/file and /union/dir/empty-dir are removed first.
    File[] aPaths = mSubUfsA.listFiles();
    if (aPaths.length > 0) {
      assertTrue("Only /union/dir might exist", aPaths.length == 1);
      assertEquals(mSubUfsA.getAbsolutePath() + "/dir", aPaths[0].getAbsolutePath());
    }

    // All files and directories should have been migrated to sub UFS B.
    Set<Path> bPaths = Files.walk(mSubUfsB.toPath()).collect(Collectors.toSet());
    assertEquals(5, bPaths.size());
    assertTrue(bPaths.contains(mSubUfsB.toPath()));
    assertTrue(bPaths.contains(mSubUfsB.toPath().resolve("file")));
    assertTrue(bPaths.contains(mSubUfsB.toPath().resolve("dir")));
    assertTrue(bPaths.contains(mSubUfsB.toPath().resolve("dir/file")));
    assertTrue(bPaths.contains(mSubUfsB.toPath().resolve("dir/empty-dir")));

    if (aPaths.length > 0) {
      // /union/dir should be removed.
      triggerPolicy();
      waitForUfsMigration(2, mFileSystem.getStatus(MOUNT_POINT.join("dir")).getCreationTimeMs());
      assertEquals(0, mSubUfsA.listFiles().length);
    }

    // Check persistence state.
    for (AlluxioURI uri : uris) {
      FileInfo info = getFileInfo(uri);
      assertTrue(info.isPersisted());
      assertEquals(PersistenceState.NOT_PERSISTED, getPersistenceState(info, UFS_A_XATTR_KEY));
      assertEquals(PersistenceState.PERSISTED, getPersistenceState(info, UFS_B_XATTR_KEY));
    }
  }

  private FileInfo getFileInfo(AlluxioURI path) throws Exception {
    long fileId = mFileSystem.getStatus(path).getFileId();
    return mFileSystemMaster.getFileInfo(fileId);
  }

  private void createFile(AlluxioURI path) throws Exception {
    try (FileOutStream out = mFileSystem.createFile(path, FILE_WRITE_THROUGH)) {
      out.write(TEST_BYTES);
    }
    // After deleting the inode from Alluxio, getStatus will reload metadata from UFS,
    // then xattr will appear in inode.
    // Since we don't set xattr in completeFile, without this hack, xattr will be empty in fileInfo.
    mFileSystem.delete(path, DeletePOptions.newBuilder().setAlluxioOnly(true).build());
    getFileInfo(path);
  }

  private void createDirectory(AlluxioURI path) throws Exception {
    mFileSystem.createDirectory(path, DIR_WRITE_THROUGH);
    // Similar reasoning as createFile.
    mFileSystem.delete(path, DeletePOptions.newBuilder().setAlluxioOnly(true).build());
    getFileInfo(path);
  }

  /**
   * @param numActions the expected number of actions
   * @param creationTimeMs creation time (in milliseconds) for the inode that is expected to be
   *    executed last
   */
  private void waitForUfsMigration(int numActions, long creationTimeMs) throws Exception {
    // Wait until inode scan triggers the ufs migration action.
    CommonUtils.waitFor("UFS migration to start",
        () -> mPolicyMaster.getActionScheduler().getExecutingActionsSize() == numActions,
        WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS).setInterval(1));

    long sincePathCreationTimeMs = System.currentTimeMillis() - creationTimeMs;
    assertTrue(String.format("UFS migration is started after the path exists for %d milliseconds, "
        + "expect to be >= %d seconds", sincePathCreationTimeMs, UFS_MIGRATE_WAIT_TIME_SECONDS),
        sincePathCreationTimeMs >= UFS_MIGRATE_WAIT_TIME_SECONDS * Constants.SECOND_MS);

    // Wait for the ufs migration action to finish (commit).
    CommonUtils.waitFor("UFS migration to finish", () -> {
      try {
        HeartbeatScheduler.execute(HeartbeatContext.MASTER_POLICY_ACTION_SCHEDULER);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return mPolicyMaster.getActionScheduler().getExecutingActionsSize() == 0;
    }, WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS).setInterval(1));
  }

  private void testUfsMigratePath(AlluxioURI path) throws Exception {
    // Check that the path exists in sub UFS a.
    File[] paths = mSubUfsA.listFiles();
    assertEquals(1, paths.length);
    assertEquals(mSubUfsA.getPath() + "/" + path.getName(), paths[0].getPath());

    // Check that the path does not exist in sub UFS b.
    paths = mSubUfsB.listFiles();
    assertEquals(0, paths.length);

    // Check the path's persistence state.
    FileInfo pathInfo = getFileInfo(path);
    assertTrue(path + " should be persisted", pathInfo.isPersisted());

    // Check the path's persistence state for sub UFS a.
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(pathInfo, UFS_A_XATTR_KEY));

    // Check the path's persistence state for sub UFS b.
    assertEquals(PersistenceState.NOT_PERSISTED, getPersistenceState(pathInfo, UFS_B_XATTR_KEY));

    createPolicy();
    checkPolicy();
    triggerPolicy();
    waitForUfsMigration(2, pathInfo.getCreationTimeMs());

    // Check that the path does not exist in sub UFS a.
    paths = mSubUfsA.listFiles();
    assertEquals(0, paths.length);

    // Check that the path exists in sub UFS b.
    paths = mSubUfsB.listFiles();
    assertEquals(1, paths.length);
    assertEquals(mSubUfsB.getPath() + "/" + path.getName(), paths[0].getPath());

    // Check the path's persistence state.
    pathInfo = getFileInfo(path);
    assertTrue(path + " should be persisted", pathInfo.isPersisted());

    // Check the path's persistence state for sub UFS a.
    assertEquals(PersistenceState.NOT_PERSISTED, getPersistenceState(pathInfo, UFS_A_XATTR_KEY));

    // Check the path's persistence state for sub UFS b.
    assertEquals(PersistenceState.PERSISTED, getPersistenceState(pathInfo, UFS_B_XATTR_KEY));
  }

  private PersistenceState getPersistenceState(FileInfo fileInfo, String xAttrKey) {
    Map<String, byte[]> xattr = fileInfo.getXAttr();
    if (xattr == null || !xattr.containsKey(xAttrKey)) {
      return PersistenceState.NOT_PERSISTED;
    }
    return ExtendedAttribute.PERSISTENCE_STATE.decode(xattr.get(xAttrKey));
  }
}
