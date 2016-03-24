/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */
package alluxio.job.move;

import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.job.JobMasterContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link MoveDefinition#selectExecutors(MoveConfig, List, JobMasterContext)}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class, JobMasterContext.class})
public final class MoveDefinitionSelectExecutorsTest {
  private static final String TEST_SOURCE = "/TEST_SOURCE";
  private static final String TEST_DESTINATION = "/TEST_DESTINATION";
  private static final MoveCommand SIMPLE_MOVE_COMMAND =
      new MoveCommand(TEST_SOURCE, TEST_DESTINATION, WriteType.THROUGH);

  private static final List<WorkerInfo> WORKERS = new ImmutableList.Builder<WorkerInfo>()
      .add(new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host0")))
      .add(new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host1")))
      .add(new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host2")))
      .add(new WorkerInfo().setAddress(new WorkerNetAddress().setHost("host3")))
      .build();

  private JobMasterContext mMockJobMasterContext;
  private FileSystemMaster mMockFileSystemMaster;

  @Before
  public void before() throws Exception {
    mMockJobMasterContext = PowerMockito.mock(JobMasterContext.class);
    mMockFileSystemMaster = PowerMockito.mock(FileSystemMaster.class);
    when(mMockJobMasterContext.getFileSystemMaster()).thenReturn(mMockFileSystemMaster);

    // Root is a directory.
    createDirectory("/");
    // TEST_DESTINATION does not exist.
    when(mMockFileSystemMaster.getFileInfo(new AlluxioURI(TEST_DESTINATION)))
        .thenThrow(new FileDoesNotExistException(""));
    // TEST_SOURCE has one block on worker 0.
    createFileWithBlocksOnWorkers(TEST_SOURCE, 0);
  }

  /**
   * Tests that a file move will be assigned to the worker with the most blocks from that file.
   */
  @Test
  public void assignToLocalWorkerTest() throws Exception {
    Map<WorkerInfo, List<MoveCommand>> expected =
        ImmutableMap.of(WORKERS.get(0), Collections.singletonList(SIMPLE_MOVE_COMMAND));
    Assert.assertEquals(expected, assignMoves(TEST_SOURCE, TEST_DESTINATION));

    createFileWithBlocksOnWorkers(TEST_SOURCE, 3, 1, 1, 3, 1);
    expected = ImmutableMap.of(WORKERS.get(1), Collections.singletonList(SIMPLE_MOVE_COMMAND));
    Assert.assertEquals(expected, assignMoves(TEST_SOURCE, TEST_DESTINATION));
  }

  /**
   * Tests that with multiple files in a directory, each is assigned to the most local worker.
   */
  @Test
  public void assignToLocalWorkerMultipleTest() throws Exception {
    // Should go to worker 0.
    FileInfo info1 = createFileWithBlocksOnWorkers("/dir/src1", 0, 1, 2, 3, 0);
    // Should go to worker 2.
    FileInfo info2 = createFileWithBlocksOnWorkers("/dir/src2", 1, 1, 2, 2, 2);
    // Should go to worker 0.
    FileInfo info3 = createFileWithBlocksOnWorkers("/dir/src3", 2, 0, 0, 1, 1, 0);
    setChildren("/dir", info1, info2, info3);
    // Say the destination doesn't exist.
    when(mMockFileSystemMaster.getFileInfo(new AlluxioURI("/dst")))
        .thenThrow(new FileDoesNotExistException(""));

    List<MoveCommand> moveCommandsWorker0 = Lists.newArrayList(
        new MoveCommand("/dir/src1", "/dst/src1", WriteType.THROUGH),
        new MoveCommand("/dir/src3", "/dst/src3", WriteType.THROUGH));
    List<MoveCommand> moveCommandsWorker2 = Lists.newArrayList(
        new MoveCommand("/dir/src2", "/dst/src2", WriteType.THROUGH));
    ImmutableMap<WorkerInfo, List<MoveCommand>> expected = ImmutableMap.of(
            WORKERS.get(0), moveCommandsWorker0,
            WORKERS.get(2), moveCommandsWorker2);
    Assert.assertEquals(expected, assignMoves("/dir", "/dst"));
  }

  /**
   * Tests that when the source file doesn't exist the correct exception is thrown.
   */
  @Test
  public void sourceMissingTest() throws Exception {
    Exception exception = new FileDoesNotExistException("/notExist");
    when(mMockFileSystemMaster.getFileInfoList(new AlluxioURI("/notExist"))).thenThrow(exception);
    try {
      assignMovesFail("/notExist", TEST_DESTINATION);
    } catch (FileDoesNotExistException e) {
      Assert.assertSame(exception, e);
    }
  }

  /**
   * Tests that when the destination exists as a file and overwrite is false, the correct exception
   * is thrown.
   */
  @Test
  public void destinationExistsNoOverwriteTest() throws Exception {
    createFile("/dst");
    try {
      assignMovesFail(TEST_SOURCE, "/dst");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage("/dst"),
          e.getMessage());
    }
  }

  /**
   * Tests that when the destination exists as a file and overwrite is true, the move works
   * correctly.
   */
  @Test
  public void destinationExistsOverwriteTest() throws Exception {
    createFileWithBlocksOnWorkers("/src", 0);
    createFile("/dst");

    Map<WorkerInfo, List<MoveCommand>> expected = ImmutableMap.of(WORKERS.get(0),
        Collections.singletonList(new MoveCommand("/src", "/dst", WriteType.THROUGH)));
    // Set overwrite to true.
    Assert.assertEquals(expected, assignMoves("/src", "/dst", "THROUGH", true));
  }

  /**
   * Tests that when the source is a file and the destination is a directory, the source is moved
   * inside of destination.
   */
  @Test
  public void fileIntoDirectoryTest() throws Exception {
    createFileWithBlocksOnWorkers("/src", 0);
    createDirectory("/dst");
    Map<WorkerInfo, List<MoveCommand>> expected = ImmutableMap.of(WORKERS.get(0),
        Collections.singletonList(new MoveCommand("/src", "/dst/src", WriteType.THROUGH)));
    Assert.assertEquals(expected, assignMoves("/src", "/dst"));
  }

  /**
   * Tests that if the source is a nested directory and the destination is a directory, the is moved
   * inside directory.
   */
  @Test
  public void nestedDirectoryIntoDirectoryTest() throws Exception {
    createDirectory("/src");
    FileInfo nested = createDirectory("/src/nested");
    FileInfo moreNested = createDirectory("/src/nested/moreNested");
    FileInfo file1 = createFileWithBlocksOnWorkers("/src/file1", 2);
    FileInfo file2 = createFileWithBlocksOnWorkers("/src/nested/file2", 1);
    FileInfo file3 = createFileWithBlocksOnWorkers("/src/nested/moreNested/file3", 1);
    setChildren("/src", nested, file1);
    setChildren("/src/nested", moreNested, file2);
    setChildren("/src/nested/moreNested", file3);
    createDirectory("/dst");

    List<MoveCommand> moveCommandsWorker1 = Lists.newArrayList(
        new MoveCommand("/src/nested/file2", "/dst/src/nested/file2", WriteType.THROUGH),
        new MoveCommand("/src/nested/moreNested/file3", "/dst/src/nested/moreNested/file3",
            WriteType.THROUGH));
    List<MoveCommand> moveCommandsWorker2 =
        Lists.newArrayList(new MoveCommand("/src/file1", "/dst/src/file1", WriteType.THROUGH));
    ImmutableMap<WorkerInfo, List<MoveCommand>> expected = ImmutableMap.of(
        WORKERS.get(1), moveCommandsWorker1,
        WORKERS.get(2), moveCommandsWorker2);
    Assert.assertEquals(expected, assignMoves("/src", "/dst"));
  }

  /**
   * Tests that a worker is chosen even when no workers have any blocks.
   */
  @Test
  public void uncachedTest() throws Exception {
    createFileWithBlocksOnWorkers("/src");
    Assert.assertEquals(1, assignMoves("/src", TEST_DESTINATION).size());
  }

  /**
   * Tests that setting the write type in config sets the write type passed to the workers.
   */
  @Test
  public void useWriteTypeTest() throws Exception {
    Assert.assertEquals(WriteType.NONE,
        assignMoves(TEST_SOURCE, TEST_DESTINATION, "NONE", false).get(WORKERS.get(0)).get(0).getWriteType());
    Assert.assertEquals(WriteType.MUST_CACHE, assignMoves(TEST_SOURCE, TEST_DESTINATION, "MUST_CACHE", false)
        .get(WORKERS.get(0)).get(0).getWriteType());
  }

  /**
   * Tests that the THROUGH write type is used by default when the file is uncached and persisted.
   */
  @Test
  public void useThroughDefault() throws Exception {
    FileInfo persistedOnlyInfo = new FileInfo().setInMemoryPercentage(0).setPersisted(true);
    createFileWithBlocksOnWorkers("/src", persistedOnlyInfo, 0);
    Assert.assertEquals(WriteType.THROUGH,
        assignMoves("/src", TEST_DESTINATION, null, false).get(WORKERS.get(0)).get(0).getWriteType());
  }

  /**
   * Tests that the MUST_CACHE write type is used by default when the file is cached and not
   * persisted.
   */
  @Test
  public void useMustCacheDefault() throws Exception {
    FileInfo cachedOnlyInfo = new FileInfo().setInMemoryPercentage(100).setPersisted(false);
    createFileWithBlocksOnWorkers("/src", cachedOnlyInfo, 0);
    Assert.assertEquals(WriteType.MUST_CACHE,
        assignMoves("/src", TEST_DESTINATION, null, false).get(WORKERS.get(0)).get(0).getWriteType());
  }

  /**
   * Tests that the CACHE_THROUGH write type is used by default when the file is persisted and at
   * least partially cached.
   */
  @Test
  public void useCacheThroughDefault() throws Exception {
    FileInfo cachedPersistedInfo = new FileInfo().setInMemoryPercentage(10).setPersisted(true);
    createFileWithBlocksOnWorkers("/src", cachedPersistedInfo, 0);
    Assert.assertEquals(WriteType.CACHE_THROUGH,
        assignMoves("/src", TEST_DESTINATION, null, false).get(WORKERS.get(0)).get(0).getWriteType());
  }

  /**
   * Runs selectExecutors for the move from source to destination.
   */
  private Map<WorkerInfo, List<MoveCommand>> assignMoves(String source, String destination)
      throws Exception {
    return assignMoves(source, destination, "THROUGH", false);
  }

  /**
   * Runs selectExecutors for the move from source to destination with the given writeType and
   * overwrite value.
   */
  private Map<WorkerInfo, List<MoveCommand>> assignMoves(String source, String destination,
      String writeType, boolean overwrite) throws Exception {
    return new MoveDefinition().selectExecutors(
        new MoveConfig(source, destination, writeType, overwrite), WORKERS, mMockJobMasterContext);
  }

  /**
   * Runs selectExecutors with the expectation that it will throw an exception.
   */
  private void assignMovesFail(String source, String destination) throws Exception {
    Map<WorkerInfo, List<MoveCommand>> assignment = assignMoves(source, destination);
    Assert.fail(
        "Selecting executors should have failed, but it succeeded with assignment " + assignment);
  }

  private void createFile(String name) throws Exception {
    createFileWithBlocksOnWorkers(name);
  }

  private FileInfo createFileWithBlocksOnWorkers(String testFile, int... workerInds) throws Exception {
    return createFileWithBlocksOnWorkers(testFile, new FileInfo(), workerInds);
  }

  /**
   * Creates a file with the given name and a block on each specified worker. Workers may be
   * repeated to give them multiple blocks.
   *
   * @param testFile the name of the file to create
   * @param fileInfo file info to apply to the created file
   * @param workerInds the workers to put blocks on, specified by their indices
   * @return file info for the created file
   */
  private FileInfo createFileWithBlocksOnWorkers(String testFile, FileInfo fileInfo,
      int... workerInds) throws Exception {
    AlluxioURI uri = new AlluxioURI(testFile);
    List<FileBlockInfo> blockInfos = Lists.newArrayList();
    for (int workerInd : workerInds) {
      WorkerNetAddress address = WORKERS.get(workerInd).getAddress();
      blockInfos.add(new FileBlockInfo().setBlockInfo(new BlockInfo()
          .setLocations(Lists.newArrayList(new BlockLocation().setWorkerAddress(address)))));
    }
    FileInfo testFileInfo = fileInfo.setFolder(false).setPath(testFile);
    when(mMockFileSystemMaster.getFileInfoList(uri)).thenReturn(Lists.newArrayList(testFileInfo));
    when(mMockFileSystemMaster.getFileBlockInfoList(uri)).thenReturn(blockInfos);
    when(mMockFileSystemMaster.getFileInfo(uri)).thenReturn(testFileInfo);
    when(mMockFileSystemMaster.getFileInfo(uri.getParent())).thenReturn(new FileInfo().setFolder(true));
    return testFileInfo;
  }

  /**
   * Creates a directory with the given name.
   *
   * @return file info for the created directory
   */
  private FileInfo createDirectory(String name) throws Exception {
    FileInfo info = new FileInfo().setFolder(true).setPath(name);
    when(mMockFileSystemMaster.getFileInfo(new AlluxioURI(name))).thenReturn(info);
    return info;
  }

  /**
   * Informs the mock that the given fileInfos are children of the parent.
   */
  private void setChildren(String parent, FileInfo... children) throws Exception {
    when(mMockFileSystemMaster.getFileInfoList(new AlluxioURI(parent)))
        .thenReturn(Lists.newArrayList(children));
  }
}
