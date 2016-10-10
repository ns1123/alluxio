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

package alluxio.master.file.replication;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.CreatePathOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.security.authorization.Permission;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Unit tests for {@link ReplicationChecker}.
 */
public final class ReplicationCheckerTest {
  private static final Permission TEST_PERMISSION = new Permission("user1", "", (short) 0755);
  private static final AlluxioURI TEST_FILE_1 = new AlluxioURI("/test1");
  private static final AlluxioURI TEST_FILE_2 = new AlluxioURI("/test2");
  private static final List<Long> NO_BLOCKS = ImmutableList.of();
  private static final Map<String, List<Long>> NO_BLOCKS_ON_TIERS = ImmutableMap.of();
  private static final Map<Long, Integer> EMPTY = ImmutableMap.of();

  /**
   * A mock class of ReplicateHandler, used to test the output of ReplicationChecker.
   */
  @ThreadSafe
  private class MockReplicateHandler implements ReplicateHandler {
    private final Map<Long, Integer> mReplicateRequest = Maps.newHashMap();

    @Override
    public void scheduleReplicate(long blockId, int numReplicas) throws AlluxioException {
      mReplicateRequest.put(blockId, numReplicas);
    }

    public Map<Long, Integer> getReplicateRequest() {
      return mReplicateRequest;
    }
  }

  /**
   * A mock class of EvictHandler, used to test the output of ReplicationChecker.
   */
  @ThreadSafe
  private static class MockEvictHandler implements EvictHandler {
    private Map<Long, Integer> mEvictRequest = Maps.newHashMap();

    @Override
    public void scheduleEvict(long blockId, int numReplicas) throws AlluxioException {
      mEvictRequest.put(blockId, numReplicas);
    }

    public Map<Long, Integer> getEvictRequest() {
      return mEvictRequest;
    }
  }

  private InodeTree mInodeTree;
  private BlockMaster mBlockMaster;
  private ReplicationChecker mReplicationChecker;
  private MockReplicateHandler mMockReplicateHandler;
  private MockEvictHandler mMockEvictHandler;
  private CreateFileOptions mFileOptions =
      CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setPermission(TEST_PERMISSION);
  private Set<Long> mKnownWorkers = Sets.newHashSet();

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());

    mBlockMaster = new BlockMaster(blockJournal);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    MountTable mountTable = new MountTable();
    mInodeTree = new InodeTree(mBlockMaster, directoryIdGenerator, mountTable);

    mBlockMaster.start(true);

    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test-supergroup");
    mInodeTree.initializeRoot(TEST_PERMISSION);

    mMockReplicateHandler = new MockReplicateHandler();
    mMockEvictHandler = new MockEvictHandler();
    mReplicationChecker =
        new ReplicationChecker(mInodeTree, mBlockMaster, mMockReplicateHandler, mMockEvictHandler);
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Helper to create a file with a single block.
   *
   * @param path Alluxio path of the file
   * @param options options to create the file
   * @return the block ID
   */
  private long createBlockHelper(AlluxioURI path, CreatePathOptions<?> options) throws Exception {
    try (LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      InodeTree.CreatePathResult result = mInodeTree.createPath(inodePath, options);
      InodeFile inodeFile = (InodeFile) result.getCreated().get(0);
      inodeFile.setBlockSizeBytes(1);
      inodeFile.complete(1);
      return ((InodeFile) result.getCreated().get(0)).getBlockIdByIndex(0);
    }
  }

  /**
   * Helper to create and add a given number of locations for block ID.
   *
   * @param blockId ID of the block to add location
   * @param numLocations number of locations to add
   */
  private void addBlockLocationHelper(long blockId, int numLocations) throws Exception {
    // Commit blockId to the first worker.
    mBlockMaster.commitBlock(createWorkerHelper(0), 50L, "MEM", blockId, 20L);

    // Send a heartbeat from other workers saying that it's added blockId.
    for (int i = 1; i < numLocations; i++) {
      heartbeatToAddLocationHelper(blockId, createWorkerHelper(i));
    }
  }

  /**
   * Helper to register a new worker.
   *
   * @param workerIndex the index of the worker in all workers
   * @return the created worker ID
   */
  private long createWorkerHelper(int workerIndex) throws Exception {
    WorkerNetAddress address = new WorkerNetAddress().setHost("host" + workerIndex).setRpcPort(1000)
        .setDataPort(2000).setWebPort(3000);
    long workerId = mBlockMaster.getWorkerId(address);
    if (!mKnownWorkers.contains(workerId)) {
      // Do not re-register works, otherwise added block will be removed
      mBlockMaster.workerRegister(workerId, ImmutableList.of("MEM"), ImmutableMap.of("MEM", 100L),
          ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS);
      mKnownWorkers.add(workerId);
    }
    return workerId;
  }

  /**
   * Helper to heartbeat to a worker and report a newly added block.
   *
   * @param blockId ID of the block to add location
   * @param workerId ID of the worker to heartbeat
   */
  private void heartbeatToAddLocationHelper(long blockId, long workerId) throws Exception {
    List<Long> addedBlocks = ImmutableList.of(blockId);
    mBlockMaster.workerHeartbeat(workerId, ImmutableMap.of("MEM", 0L), NO_BLOCKS,
        ImmutableMap.of("MEM", addedBlocks));
  }

  @Test
  public void heartbeatWhenTreeIsEmpty() throws Exception {
    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(EMPTY, mMockEvictHandler.getEvictRequest());
  }

  @Test
  public void heartbeatFileWithinRange() throws Exception {
    long blockId =
        createBlockHelper(TEST_FILE_1, mFileOptions.setReplicationMin(1).setReplicationMax(3));
    // One replica, meeting replication min
    addBlockLocationHelper(blockId, 1);
    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(EMPTY, mMockEvictHandler.getEvictRequest());

    // Two replicas, good
    heartbeatToAddLocationHelper(blockId, createWorkerHelper(1));
    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(EMPTY, mMockEvictHandler.getEvictRequest());

    // Three replicas, meeting replication max, still good
    heartbeatToAddLocationHelper(blockId, createWorkerHelper(2));
    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(EMPTY, mMockEvictHandler.getEvictRequest());
  }

  @Test
  public void heartbeatFileUnderReplicatedBy1() throws Exception {
    long blockId = createBlockHelper(TEST_FILE_1, mFileOptions.setReplicationMin(1));

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId, 1);
    Assert.assertEquals(expected, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(EMPTY, mMockEvictHandler.getEvictRequest());
  }

  @Test
  public void heartbeatFileUnderReplicatedBy10() throws Exception {
    long blockId = createBlockHelper(TEST_FILE_1, mFileOptions.setReplicationMin(10));

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId, 10);
    Assert.assertEquals(expected, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(EMPTY, mMockEvictHandler.getEvictRequest());
  }

  @Test
  public void heartbeatMultipleFilesUnderReplicated() throws Exception {
    long blockId1 = createBlockHelper(TEST_FILE_1, mFileOptions.setReplicationMin(1));
    long blockId2 = createBlockHelper(TEST_FILE_2, mFileOptions.setReplicationMin(2));

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId1, 1, blockId2, 2);
    Assert.assertEquals(expected, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(EMPTY, mMockEvictHandler.getEvictRequest());
  }

  @Test
  public void heartbeatFileOverReplicatedBy1() throws Exception {
    long blockId = createBlockHelper(TEST_FILE_1, mFileOptions.setReplicationMax(1));
    addBlockLocationHelper(blockId, 2);

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId, -1);
    Assert.assertEquals(EMPTY, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(expected, mMockEvictHandler.getEvictRequest());
  }

  @Test
  public void heartbeatFileOverReplicatedBy10() throws Exception {
    long blockId = createBlockHelper(TEST_FILE_1, mFileOptions.setReplicationMax(1));
    addBlockLocationHelper(blockId, 11);

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId, 10);
    Assert.assertEquals(EMPTY, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(expected, mMockEvictHandler.getEvictRequest());
  }

  @Test
  public void heartbeatMultipleFilesOverReplicated() throws Exception {
    long blockId1 = createBlockHelper(TEST_FILE_1, mFileOptions.setReplicationMax(1));
    long blockId2 = createBlockHelper(TEST_FILE_2, mFileOptions.setReplicationMax(2));
    addBlockLocationHelper(blockId1, 2);
    addBlockLocationHelper(blockId2, 4);

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId1, 1, blockId2, 2);
    Assert.assertEquals(expected, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(EMPTY, mMockEvictHandler.getEvictRequest());
  }

  @Test
  public void heartbeatFilesUnderAndOverReplicated() throws Exception {
    long blockId1 =
        createBlockHelper(TEST_FILE_1, mFileOptions.setReplicationMin(2).setReplicationMax(-1));
    long blockId2 =
        createBlockHelper(TEST_FILE_2, mFileOptions.setReplicationMin(0).setReplicationMax(3));
    addBlockLocationHelper(blockId1, 1);
    addBlockLocationHelper(blockId2, 5);

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected1 = ImmutableMap.of(blockId1, 1);
    Map<Long, Integer> expected2 = ImmutableMap.of(blockId2, 2);

    Assert.assertEquals(expected1, mMockReplicateHandler.getReplicateRequest());
    Assert.assertEquals(expected2, mMockEvictHandler.getEvictRequest());
  }
}
