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

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.job.adjust.AdjustReplicationHandler;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.wire.BlockInfo;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The executor to check block replication level periodically and handle over-replicated and
 * under-replicated blocks correspondingly.
 */
@ThreadSafe
public final class ReplicationChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Handler to the inode tree. */
  private final InodeTree mInodeTree;
  /** Handler to the block master. */
  private final BlockMaster mBlockMaster;
  /** Handler to job service that replicates target blocks. */
  private final AdjustReplicationHandler mReplicateHandler;
  /** Handler to job service that evicts target blocks. */
  private final AdjustReplicationHandler mEvictHandler;

  /**
   * Constructs a new {@link ReplicationChecker} using default (job service) handler to replicate
   * and evict blocks.
   *
   * @param inodeTree inode tree of the filesystem master
   * @param blockMaster block master
   */
  public ReplicationChecker(InodeTree inodeTree, BlockMaster blockMaster) {
    // TODO(binfan): we create Replicate and Evict handlers using reflection, because
    // alluxio-core-server can not depend on alluxio-job-enterprise where these two handlers are
    // implemented to avoid circular dependency. We should move JobConfig and etc into common to
    // to fully solve this problem without using refection.
    this(inodeTree, blockMaster,
        AdjustReplicationHandler.Factory.create("alluxio.job.adjust.ReplicateHandler"),
        AdjustReplicationHandler.Factory.create("alluxio.job.adjust.EvictHandler"));
  }

  /**
   * Constructs a new {@link ReplicationChecker} with specified replicate and evict handlers (for
   * unit testing).
   *
   * @param inodeTree inode tree of the filesystem master
   * @param blockMaster block master
   * @param replicateHandler handler to replicate blocks
   * @param evictHandler handler to evict blocks
   */
  public ReplicationChecker(InodeTree inodeTree, BlockMaster blockMaster,
      AdjustReplicationHandler replicateHandler, AdjustReplicationHandler evictHandler) {
    mInodeTree = inodeTree;
    mBlockMaster = blockMaster;
    mReplicateHandler = replicateHandler;
    mEvictHandler = evictHandler;
  }

  /**
   * {@inheritDoc}
   *
   * During this heartbeat, this class will check:
   * <p>
   * (1) Is there any block from the pinned files becomes under replicated (i.e., the number of
   * existing copies is smaller than the target replication min for this file, possibly due to node
   * failures), and schedule replicate jobs to increase the replication level when found;
   *
   * (2) Is there any blocks over replicated, schedule evict jobs to reduce the replication level.
   */
  @Override
  public void heartbeat() throws InterruptedException {
    Set<Long> inodes;

    // Check the set of files that could possibly be under-replicated
    inodes = mInodeTree.getPinIdSet();
    check(inodes, mReplicateHandler, true);

    // Check the set of files that could possibly be over-replicated
    inodes = mInodeTree.getReplicationLimitedFileIds();
    check(inodes, mEvictHandler, false);
  }

  @Override
  public void close() {
    // Nothing to clean up
  }

  private void check(Set<Long> inodes, AdjustReplicationHandler handler,
      boolean checkUnderReplicated) {
    Set<Long> lostBlocks = mBlockMaster.getLostBlocks();
    Map<Long, Integer> found = Maps.newHashMap();
    for (long inodeId : inodes) {
      // TODO(binfan): calling lockFullInodePath locks the entire path from root to the target
      // file and may increase lock contention in this tree. Investigate if we could avoid
      // locking the entire path but just the inode file since this access is read-only.
      try (LockedInodePath inodePath =
          mInodeTree.lockFullInodePath(inodeId, InodeTree.LockMode.READ)) {
        InodeFile file = inodePath.getInodeFile();
        for (long blockId : file.getBlockIds()) {
          BlockInfo blockInfo = null;
          try {
            blockInfo = mBlockMaster.getBlockInfo(blockId);
          } catch (BlockInfoException e) {
            // Cannot find this block in Alluxio from BlockMaster, possibly persisted in UFS
          }
          int currentReplicas = (blockInfo == null) ? 0 : blockInfo.getLocations().size();
          if (checkUnderReplicated && currentReplicas < file.getReplicationMin()) {
            // if this file is not persisted and block master thinks it is lost, no effort made
            if (!file.isPersisted() && lostBlocks.contains(blockId)) {
              continue;
            }
            found.put(blockId, file.getReplicationMin() - currentReplicas);
          }
          if (!checkUnderReplicated && currentReplicas > file.getReplicationMax()) {
            found.put(blockId, currentReplicas - file.getReplicationMax());
          }
        }
      } catch (FileDoesNotExistException e) {
        LOG.warn("Failed to check inodeId {}", inodeId);
      }
    }

    for (Map.Entry<Long, Integer> entry : found.entrySet()) {
      try {
        handler.adjust(entry.getKey(), entry.getValue());
      } catch (AlluxioException e) {
        LOG.error("Failed to schedule adjust block Id {} by {}", entry.getKey(), handler);
      }
    }
  }
}
