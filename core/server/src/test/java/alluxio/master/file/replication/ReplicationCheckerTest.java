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

import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.security.authorization.Permission;

import com.google.common.collect.Maps;

/**
 * Unit tests for {@link ReplicationChecker}.
 */
public final class ReplicationCheckerTest {
  private static final Permission TEST_PERMISSION = new Permission("user1", "", (short) 0755);


  /**
   * Mock class of ReplicateHandler to test ReplicationChecker
   */
  private class MockReplicateHandler implements ReplicateHandler {
    private Map<Long, Integer> mBlockReplication;

    public MockReplicateHandler(Map<Long, Integer> blockReplication) {
      mBlockReplication = blockReplication;
    }

    @Override
    public void scheduleReplicate(long blockId, int numReplicas) throws AlluxioException {
      int prevReplcas = 0;
      if (!mBlockReplication.containsKey(blockId)) {
        prevReplcas = mBlockReplication.get(blockId);
      }
      mBlockReplication.put(blockId, prevReplcas + numReplicas);
    }
  }

  /**
   * Mock class of ReplicateHandler to test ReplicationChecker
   */
  private static class MockEvictHandler implements EvictHandler {
    private Map<Long, Integer> mBlockReplication;


    public MockEvictHandler(Map<Long, Integer> blockReplication) {
      mBlockReplication = blockReplication;
    }

    @Override
    public void scheduleEvict(long blockId, int numReplicas) throws AlluxioException {
      int prevReplcas = 0;
      if (!mBlockReplication.containsKey(blockId)) {
        prevReplcas = mBlockReplication.get(blockId);
      }
      mBlockReplication.put(blockId, prevReplcas - numReplicas);
    }
  }

  private InodeTree mInodeTree;
  private BlockMaster mBlockMaster;
  private Map<Long, Integer> mBlockReplication = Maps.newHashMap();
  private ReplicationChecker mReplicationChecker;
  private MockReplicateHandler mMockReplicateHandler;
  private MockEvictHandler mMockEvictHandler;

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

    mMockReplicateHandler = new MockReplicateHandler(mBlockReplication);
    mMockEvictHandler = new MockEvictHandler(mBlockReplication);
    mReplicationChecker =
        new ReplicationChecker(mInodeTree, mBlockMaster, mMockReplicateHandler, mMockEvictHandler);
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void heartbeatWithEmptyTree() throws Exception {
    mReplicationChecker.heartbeat();
    Assert.assertEquals(0, mBlockReplication.size());
  }
}
