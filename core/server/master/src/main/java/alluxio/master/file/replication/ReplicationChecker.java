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
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.job.replicate.ReplicationHandler;
import alluxio.job.replicate.DefaultReplicationHandler;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.PersistenceState;
import alluxio.wire.BlockInfo;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The executor to check block replication level periodically and handle over-replicated and
 * under-replicated blocks correspondingly.
 */
@ThreadSafe
public final class ReplicationChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationChecker.class);

  /** Handler to the inode tree. */
  private final InodeTree mInodeTree;
  /** Handler to the block master. */
  private final BlockMaster mBlockMaster;
  /** Handler for adjusting block replication level. */
  private final ReplicationHandler mReplicationHandler;

  private enum Mode {
    EVICT,
    REPLICATE
  }

  /**
   * Constructs a new {@link ReplicationChecker} using default (job service) handler to replicate
   * and evict blocks.
   *
   * @param inodeTree inode tree of the filesystem master
   * @param blockMaster block master
   */
  public ReplicationChecker(InodeTree inodeTree, BlockMaster blockMaster) {
    this(inodeTree, blockMaster, new DefaultReplicationHandler());
  }

  /**
   * Constructs a new {@link ReplicationChecker} with specified replicate and evict handlers (for
   * unit testing).
   *
   * @param inodeTree inode tree of the filesystem master
   * @param blockMaster block master
   * @param replicationHandler handler to replicate blocks
   */
  public ReplicationChecker(InodeTree inodeTree, BlockMaster blockMaster,
      ReplicationHandler replicationHandler) {
    mInodeTree = inodeTree;
    mBlockMaster = blockMaster;
    mReplicationHandler = replicationHandler;
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
    check(inodes, mReplicationHandler, Mode.REPLICATE);

    // Check the set of files that could possibly be over-replicated
    inodes = mInodeTree.getReplicationLimitedFileIds();
    check(inodes, mReplicationHandler, Mode.EVICT);
  }

  @Override
  public void close() {
    // Nothing to clean up
  }

  private void check(Set<Long> inodes, ReplicationHandler handler, Mode mode) {
    Set<Long> lostBlocks = mBlockMaster.getLostBlocks();
    Set<Triple<AlluxioURI, Long, Integer>> evictRequests = new HashSet<>();
    Set<Triple<AlluxioURI, Long, Integer>> replicateRequests = new HashSet<>();
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
          switch (mode) {
            case EVICT:
              int maxReplicas = file.getReplicationMax();
              if (file.getPersistenceState() == PersistenceState.TO_BE_PERSISTED
                  && file.getReplicationDurable() > maxReplicas) {
                maxReplicas = file.getReplicationDurable();
              }
              if (currentReplicas > maxReplicas) {
                evictRequests.add(new ImmutableTriple<>(inodePath.getUri(), blockId,
                    currentReplicas - maxReplicas));
              }
              break;
            case REPLICATE:
              int minReplicas = file.getReplicationMin();
              if (file.getPersistenceState() == PersistenceState.TO_BE_PERSISTED
                  && file.getReplicationDurable() > minReplicas) {
                minReplicas = file.getReplicationDurable();
              }
              if (currentReplicas < minReplicas) {
                // if this file is not persisted and block master thinks it is lost, no effort made
                if (!file.isPersisted() && lostBlocks.contains(blockId)) {
                  continue;
                }
                replicateRequests.add(new ImmutableTriple<>(inodePath.getUri(), blockId,
                    minReplicas - currentReplicas));
              }
              break;
            default:
          }
        }
      } catch (FileDoesNotExistException e) {
        LOG.warn("Failed to check replication level for inode id {}", inodeId);
      }
    }
    for (Triple<AlluxioURI, Long, Integer> entry : evictRequests) {
      handler.evict(entry.getLeft(), entry.getMiddle(), entry.getRight());
    }
    for (Triple<AlluxioURI, Long, Integer> entry : replicateRequests) {
      handler.replicate(entry.getLeft(), entry.getMiddle(), entry.getRight());
    }
  }
}
