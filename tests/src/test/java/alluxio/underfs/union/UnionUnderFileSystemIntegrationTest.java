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

package alluxio.underfs.union;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.MountTable;
import alluxio.resource.CloseableResource;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class UnionUnderFileSystemIntegrationTest {
  private static final CreateDirectoryPOptions CREATE_DIR_THROUGH = CreateDirectoryPOptions
      .newBuilder().setWriteType(WritePType.THROUGH).build();

  @Rule
  public TemporaryFolder mUfsFolderA = new TemporaryFolder();
  @Rule
  public TemporaryFolder mUfsFolderB = new TemporaryFolder();
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  private String mUfsPathA;
  private String mUfsPathB;
  private FileSystem mFileSystem;
  private FileSystemMaster mFileSystemMaster;

  @Before
  public void before() throws Exception {
    mUfsPathA = mUfsFolderA.getRoot().getAbsolutePath();
    mUfsPathB = mUfsFolderB.getRoot().getAbsolutePath();
    mFileSystem = FileSystem.Factory.create(ServerConfiguration.global());
    mFileSystemMaster = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
        .getMasterProcess().getMaster(FileSystemMaster.class);
  }

  @Test
  public void exists() throws Exception {
    String mountPoint = "/union";
    allUfsMount(mountPoint);
    URIStatus mountStatus = mFileSystem.getStatus(new AlluxioURI(mountPoint));

    AtomicReference<MountTable.Resolution> resolution = new AtomicReference<>();
    mFileSystemMaster.exec(mountStatus.getFileId(), LockPattern.READ, ctx ->
        resolution.set(ctx.getMountInfo()));
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.get().acquireUfsResource()) {
      assertFalse(ufsResource.get().exists("union:///not-exist"));
      assertFalse(ufsResource.get().exists("union://(A)/not-exist"));
      assertFalse(ufsResource.get().exists("union://(B)/not-exist"));
    }

    FileSystemTestUtils
        .createByteFile(mFileSystem, new AlluxioURI("/union/exist"),
            CreateFilePOptions.newBuilder()
                .setWriteType(WritePType.THROUGH)
                .setRecursive(true)
                .build(),
            1000);

    mFileSystemMaster.exec(mountStatus.getFileId(), LockPattern.READ, ctx ->
        resolution.set(ctx.getMountInfo()));
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.get().acquireUfsResource()) {
      assertTrue(ufsResource.get().exists("union:///exist"));
      assertTrue(ufsResource.get().exists("union://(A)/exist"));
      assertTrue(ufsResource.get().exists("union://(B)/exist"));
    }
  }

  @Test
  public void dualBasicFileLifeBoth() throws Exception {
    allUfsMount("/mnt/union");
    testDualFileLifeCycle("/mnt/union");
  }

  @Test
  public void freeThenReadBoth() throws Exception {
    String mountPath = "/mnt/union/";
    allUfsMount(mountPath);
    String filename = "/test/file";
    String fullPath = PathUtils.concatPath(mountPath, filename);
    FileSystemTestUtils
        .createByteFile(mFileSystem, new AlluxioURI(fullPath),
            CreateFilePOptions.newBuilder()
                .setWriteType(WritePType.THROUGH)
                .setRecursive(true)
                .build(),
            1000);
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathA, filename)));
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathB, filename)));
    freeFile(fullPath);
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathA, filename)));
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathB, filename)));
    readFile(fullPath, 1000);
  }

  @Test
  public void freeThenReadSingle() throws Exception {
    String mountPath = "/mnt/union/";
    lowPriorityCreateMount(mountPath);
    String filename = "/test/file";
    String fullPath = PathUtils.concatPath(mountPath, filename);
    FileSystemTestUtils
        .createByteFile(mFileSystem, new AlluxioURI(fullPath),
            CreateFilePOptions.newBuilder()
                .setWriteType(WritePType.THROUGH)
                .setRecursive(true)
                .build(),
            1000);
    assertFalse(localUfsExists(PathUtils.concatPath(mUfsPathA, filename)));
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathB, filename)));
    mFileSystem.free(new AlluxioURI(fullPath));
    URIStatus s = mFileSystem.getStatus(new AlluxioURI(fullPath));
    assertEquals(0, s.getInAlluxioPercentage());
    assertFalse(localUfsExists(PathUtils.concatPath(mUfsPathA, filename)));
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathB, filename)));
    readFile(fullPath, 1000);
  }

  @Test
  public void lifecycleBoth() throws Exception {
    allUfsMount("/mnt/union");
    List<String> paths = new ArrayList<>();
    paths.add(mUfsPathA);
    paths.add(mUfsPathB);
    fileAndDirLifecycle("/mnt/union", paths);
  }

  @Test
  public void lifecycleSingle() throws Exception {
    lowPriorityCreateMount("/mnt/single");
    fileAndDirLifecycle("/mnt/single", Collections.singletonList(mUfsPathB));
  }

  // Can't mount multiple unions
  @Test
  public void multiMount() throws Exception {
    allUfsMount("/mnt/union1", "union://ufs1/");
    allUfsMount("/mnt/union2", "union://ufs2/");
    testDualFileLifeCycle("/mnt/union1");
    testDualFileLifeCycle("/mnt/union2");
  }

  @Test
  public void deleteNonEmptyDirectory() throws Exception {
    String mountPoint = "/union";
    allUfsMount(mountPoint);
    mFileSystem.createDirectory(new AlluxioURI("/union/non-empty-dir"), CREATE_DIR_THROUGH);
    mFileSystem.createDirectory(new AlluxioURI("/union/non-empty-dir/dir"), CREATE_DIR_THROUGH);
    URIStatus mountStatus = mFileSystem.getStatus(new AlluxioURI(mountPoint));

    AtomicReference<MountTable.Resolution> resolution = new AtomicReference<>();
    mFileSystemMaster.exec(mountStatus.getFileId(), LockPattern.READ, ctx ->
        resolution.set(ctx.getMountInfo()));
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.get().acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();

      assertFalse("Cannot delete non-empty directory",
          ufs.deleteExistingDirectory("union://(A)/non-empty-dir"));
      assertFalse("Cannot delete non-empty directory",
          ufs.deleteDirectory("union://(A)/non-empty-dir"));

      assertTrue("Can delete non-empty directory recursively",
          ufs.deleteExistingDirectory("union://(A)/non-empty-dir",
              DeleteOptions.defaults().setRecursive(true)));
      assertTrue("Can delete non-empty directory recursively",
          ufs.deleteDirectory("union://(B)/non-empty-dir",
              DeleteOptions.defaults().setRecursive(true)));
    }
  }

  @Test
  public void deleteNonExistentDirectory() throws Exception {
    String mountPoint = "/union";
    allUfsMount(mountPoint);
    URIStatus mountStatus = mFileSystem.getStatus(new AlluxioURI(mountPoint));

    AtomicReference<MountTable.Resolution> resolution = new AtomicReference<>();
    mFileSystemMaster.exec(mountStatus.getFileId(), LockPattern.READ, ctx ->
        resolution.set(ctx.getMountInfo()));
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.get().acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();

      assertFalse("Cannot delete non-existent directory",
          ufs.deleteExistingDirectory("union://(A)/non-existent-dir"));
      assertFalse("Cannot delete non-existent directory",
          ufs.deleteDirectory("union://(A)/non-existent-dir"));
    }
  }

  @Test
  public void deleteNonExistentFile() throws Exception {
    String mountPoint = "/union";
    allUfsMount(mountPoint);
    URIStatus mountStatus = mFileSystem.getStatus(new AlluxioURI(mountPoint));

    AtomicReference<MountTable.Resolution> resolution = new AtomicReference<>();
    mFileSystemMaster.exec(mountStatus.getFileId(), LockPattern.READ, ctx ->
        resolution.set(ctx.getMountInfo()));
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.get().acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();

      assertFalse("Cannot delete non-existent file",
          ufs.deleteExistingFile("union://(A)/non-existent-file"));
      assertFalse("Cannot delete non-existent file",
          ufs.deleteFile("union://(A)/non-existent-file"));
    }
  }

  private boolean localUfsExists(String path) {
    File f = new File(path);
    return f.exists();
  }

  private void mountUnion(String alluxioPath, String unionUri, Map<String, String> properties)
      throws Exception {
    String parent = PathUtils.getParent(alluxioPath);
    if (!mFileSystem.exists(new AlluxioURI(parent))) {
      mFileSystem.createDirectory(new AlluxioURI(parent),
          CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    }
    mFileSystem.mount(new AlluxioURI(alluxioPath), new AlluxioURI(unionUri),
        MountPOptions.newBuilder().putAllProperties(properties).build());
  }

  /**
   * creates a mount that reads from UFS A first, then UFS B. Writes to both.
   *
   * @param path path to mount at
   */
  private void allUfsMount(String path) throws Exception {
    allUfsMount(path, "union:///");
  }

  /**
   * creates a mount that reads from UFS A first, then UFS B. Writes to both.
   *
   * @param path path to mount at
   * @param unionUri the union uri to mount
   */
  private void allUfsMount(String path, String unionUri) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("alluxio-union.A.uri", mUfsPathA);
    props.put("alluxio-union.B.uri", mUfsPathB);
    props.put("alluxio-union.priority.read", "A,B");
    props.put("alluxio-union.collection.create", "A,B");
    mountUnion(path, unionUri, props);
  }

  /**
   * creates a mount that reads from UFS A, then B. Writes to only B.
   *
   * @param path path to mount at
   */
  private void lowPriorityCreateMount(String path) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("alluxio-union.A.uri", mUfsPathA);
    props.put("alluxio-union.B.uri", mUfsPathB);
    props.put("alluxio-union.priority.read", "A,B");
    props.put("alluxio-union.collection.create", "B");
    mountUnion(path, "union:///", props);
  }

  private void testDualFileLifeCycle(String basePath) throws Exception {
    String filename = "test-file";
    AlluxioURI uri = new AlluxioURI(PathUtils.concatPath(basePath, filename));
    // Initially the file should not exist in either UFS.
    assertFalse(localUfsExists(PathUtils.concatPath(mUfsPathA, filename)));
    assertFalse(localUfsExists(PathUtils.concatPath(mUfsPathB, filename)));
    assertFalse(mFileSystem.exists(uri));
    // Create it with CACHE_THROUGH and check it exists in both UFSes.
    touch(uri);
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathA, filename)));
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathB, filename)));
    assertTrue(mFileSystem.exists(uri));
    // Rename it and check it is renamed in both UFSes.
    String newFilename = "test-file-new";
    AlluxioURI newUri = new AlluxioURI(PathUtils.concatPath(basePath, newFilename));
    mFileSystem.rename(uri, newUri);
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathA, newFilename)));
    assertTrue(localUfsExists(PathUtils.concatPath(mUfsPathB, newFilename)));
    assertTrue(mFileSystem.exists(newUri));
    assertFalse(localUfsExists(PathUtils.concatPath(mUfsPathA, filename)));
    assertFalse(localUfsExists(PathUtils.concatPath(mUfsPathB, filename)));
    assertFalse(mFileSystem.exists(uri));
    // Delete it and check it is deleted in both UFSes.
    mFileSystem.delete(newUri);
    assertFalse(localUfsExists(PathUtils.concatPath(mUfsPathA, newFilename)));
    assertFalse(localUfsExists(PathUtils.concatPath(mUfsPathB, newFilename)));
    assertFalse(mFileSystem.exists(newUri));
  }

  private void touch(AlluxioURI uri) throws Exception {
    CreateFilePOptions options =
        CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();
    mFileSystem.createFile(uri, options).close();
  }

  private void freeFile(String path) throws Exception {
    CommonUtils.waitFor("Wait for file to be freed", () -> {
      boolean freed = false;
      try {
        URIStatus s = mFileSystem.getStatus(new AlluxioURI(path),
            GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build());
        if (s.getInAlluxioPercentage() == 0) {
          freed = true;
        } else {
          mFileSystem.free(new AlluxioURI(path));
        }
      } catch (Exception e) {
        fail();
      }
      return freed;
    });
  }

  private void fileAndDirLifecycle(String mountPoint, List<String> ufsPaths) throws Exception {
    //
    // File lifecycle
    //
    String filepath = "/test/file";
    CreateFilePOptions.Builder createOpts =
        CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).setRecursive(true);
    int fileSize = 1000;
    String fullPath = PathUtils.concatPath(mountPoint, filepath);
    AlluxioURI uriPath = new AlluxioURI(fullPath);
    // Create file
    FileSystemTestUtils
        .createByteFile(mFileSystem, new AlluxioURI(fullPath),
            createOpts.setReplicationMax(3).build(),
            fileSize);
    // Make sure it exists in alluxio and UFS
    ufsPaths.forEach((ufs) -> assertTrue(localUfsExists(PathUtils.concatPath(ufs, filepath))));
    assertTrue(mFileSystem.exists(uriPath));

    // Free file
    freeFile(fullPath);
    ufsPaths.forEach((ufs) -> assertTrue(localUfsExists(PathUtils.concatPath(ufs, filepath))));

    // Make sure we can list status on the file, and that replication is set from the create
    List<URIStatus> stats = mFileSystem.listStatus(uriPath,
        ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build());
    assertEquals(1, stats.size());
    assertEquals(3, stats.get(0).getReplicationMax());

    // Make sure get status is the same
    URIStatus s = mFileSystem.getStatus(uriPath,
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build());
    assertEquals(3, s.getReplicationMax());
    assertFalse(s.isPinned());

    // Pin the file (opposite from current state)
    mFileSystem.setAttribute(uriPath, SetAttributePOptions.newBuilder()
        .setPinned(true).build());
    // Make sure the file is pinned afterwards
    s = mFileSystem.getStatus(uriPath,
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build());
    assertTrue(s.isPinned());

    // Unpin and check the state again.
    mFileSystem.setAttribute(uriPath, SetAttributePOptions.newBuilder()
        .setPinned(false).build());
    s = mFileSystem.getStatus(uriPath,
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build());
    assertFalse(s.isPinned());

    // Rename one direction
    String newName = "/renamedFile";
    AlluxioURI newPath = new AlluxioURI(PathUtils.concatPath(mountPoint, newName));
    mFileSystem.rename(uriPath, newPath);
    assertFalse(mFileSystem.exists(uriPath));
    assertTrue(mFileSystem.exists(newPath));
    ufsPaths.forEach((ufs) -> assertFalse(localUfsExists(PathUtils.concatPath(ufs, filepath))));
    ufsPaths.forEach((ufs) -> assertTrue(localUfsExists(PathUtils.concatPath(ufs, newName))));

    // Read from file
    readFile(newPath.toString(), fileSize);

    // Rename back
    mFileSystem.rename(newPath, uriPath);
    assertTrue(mFileSystem.exists(uriPath));
    assertFalse(mFileSystem.exists(newPath));
    ufsPaths.forEach((ufs) -> assertTrue(localUfsExists(PathUtils.concatPath(ufs, filepath))));
    ufsPaths.forEach((ufs) -> assertFalse(localUfsExists(PathUtils.concatPath(ufs, newName))));

    // Read file again
    readFile(fullPath, fileSize);

    mFileSystem.delete(uriPath, DeletePOptions.newBuilder().setAlluxioOnly(false).build());
    assertFalse(mFileSystem.exists(uriPath));
    ufsPaths.forEach((ufs) -> assertFalse(localUfsExists(PathUtils.concatPath(ufs, filepath))));

    //
    // Directory lifecycle
    //
    String directoryPath = "/test/directory/path";
    AlluxioURI dirUri = new AlluxioURI(PathUtils.concatPath(mountPoint, directoryPath));
    mFileSystem.createDirectory(dirUri, CreateDirectoryPOptions.newBuilder()
            .setWriteType(WritePType.THROUGH).setRecursive(true).build());
    assertTrue(mFileSystem.exists(dirUri));
    assertEquals(0, mFileSystem.getStatus(dirUri).getInAlluxioPercentage());
    assertTrue(mFileSystem.getStatus(dirUri).isFolder());
    ufsPaths.forEach((ufs) -> assertTrue(localUfsExists(PathUtils.concatPath(ufs, directoryPath))));

    // Pin the dir (opposite from current state)
    mFileSystem.setAttribute(dirUri, SetAttributePOptions.newBuilder()
        .setPinned(true).build());
    // Make sure the dir is pinned afterwards
    s = mFileSystem.getStatus(dirUri,
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build());
    assertTrue(s.isPinned());

    // Unpin and check the state again.
    mFileSystem.setAttribute(dirUri, SetAttributePOptions.newBuilder()
        .setPinned(false).build());
    s = mFileSystem.getStatus(dirUri,
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build());
    assertFalse(s.isPinned());

    String renamedDir = "/test/directory/path2";
    AlluxioURI newDir = new AlluxioURI(PathUtils.concatPath(mountPoint, renamedDir));
    mFileSystem.rename(dirUri, newDir);
    assertFalse(mFileSystem.exists(dirUri));
    assertTrue(mFileSystem.exists(newDir));
    ufsPaths.forEach((ufs) -> assertFalse(localUfsExists(PathUtils.concatPath(ufs, directoryPath))));
    ufsPaths.forEach((ufs) -> assertTrue(localUfsExists(PathUtils.concatPath(ufs, renamedDir))));

    assertTrue(mFileSystem.getStatus(newDir).isFolder());

    // Rename back
    mFileSystem.rename(newDir, dirUri);
    assertTrue(mFileSystem.exists(dirUri));
    assertFalse(mFileSystem.exists(newDir));
    ufsPaths.forEach((ufs) -> assertTrue(localUfsExists(PathUtils.concatPath(ufs, directoryPath))));
    ufsPaths.forEach((ufs) -> assertFalse(localUfsExists(PathUtils.concatPath(ufs, renamedDir))));

    mFileSystem.delete(dirUri, DeletePOptions.newBuilder().setAlluxioOnly(false).build());
    assertFalse(mFileSystem.exists(dirUri));
    ufsPaths.forEach((ufs) -> assertFalse(localUfsExists(PathUtils.concatPath(ufs, directoryPath))));
  }

  private void readFile(String path, int size) throws Exception {
    try (FileInStream in = mFileSystem.openFile(new AlluxioURI(path),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build())) {
      byte[] buf = new byte[size];
      int r;
      int ct = 0;
      while (true) {
        r = in.read(buf);
        if (r == -1) {
          break;
        }
        ct += r;
      }
      assertEquals(size, ct);
    }
  }
}
