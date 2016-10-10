/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.replication;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.wire.BlockInfo;

import com.google.common.collect.Maps;

/**
 * The executor to check block replication level periodically.
 */
@ThreadSafe
public final class ReplicationChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Handle to the inode tree. */
  private final InodeTree mInodeTree;
  /** Handle to the block master. */
  private final BlockMaster mBlockMaster;
  /** handle to job service to replicate target blocks. */
  private final ReplicateHandler mReplicateHandler;
  /** handle to job service to evict target blocks. */
  private final EvictHandler mEvictHandler;


  /**
   * Constructs a new {@link ReplicationChecker}.
   */
  public ReplicationChecker(InodeTree inodeTree, BlockMaster blockMaster) {
    this(inodeTree, blockMaster, new DefaultReplicateHandler(), new DefaultEvictHandler());
  }

  /**
   * Constructs a new {@link ReplicationChecker} with specific replicate and evict handler.
   */
  public ReplicationChecker(InodeTree inodeTree, BlockMaster blockMaster,
      ReplicateHandler replicateHandler, EvictHandler evictHandler) {
    mInodeTree = inodeTree;
    mBlockMaster = blockMaster;
    mReplicateHandler = replicateHandler;
    mEvictHandler = evictHandler;
  }

  @Override
  public void heartbeat() throws InterruptedException {
    Set<Long> inodes;

    // Check the set of files that could possibly be under-replicated
    inodes = mInodeTree.getPinIdSet();
    checkUnderReplication(inodes);

    // Check the set of files that could possibly be over-replicated
    inodes = mInodeTree.getReplicationLimitedFileIds();
    checkOverReplication(inodes);
  }

  @Override
  public void close() {
    // Nothing to clean up
  }

  private void checkUnderReplication(Set<Long> inodes) {
    Map<Long, Integer> underReplicated = Maps.newHashMap();
    for (long inodeId : inodes) {
      // TODO(binfan): calling lockFullInodePath locks the entire path from root to the target
      // file and may increase lock contention in this tree. Investigate if we could avoid
      // locking the entire path but just the inode file since this access is read-only.
      try (LockedInodePath inodePath =
          mInodeTree.lockFullInodePath(inodeId, InodeTree.LockMode.READ)) {
        InodeFile file = inodePath.getInodeFile();
        // List<Long> blockIds = file.getBlockIds();
        for (long blockId : file.getBlockIds()) {
          BlockInfo blockInfo = null;
          try {
            blockInfo = mBlockMaster.getBlockInfo(blockId);
          } catch (BlockInfoException e) {
            // Cannot find this block in Alluxio from BlockMaster, possibly persisted in UFS
          }
          int currentReplicas = (blockInfo == null)? 0 : blockInfo.getLocations().size();
          if (currentReplicas < file.getReplicationMin()) {
            // Deal with the under replication by scheduling extra replications
            underReplicated.put(blockId, file.getReplicationMin() - currentReplicas);
          }
        }
      } catch (FileDoesNotExistException e) {
        LOG.warn("Failed to check inodeId {} for replicationMin", inodeId);
      }
    }
    for (Map.Entry<Long, Integer> entry : underReplicated.entrySet()) {
      try {
        mReplicateHandler.scheduleReplicate(entry.getKey(), entry.getValue());
      } catch (AlluxioException e) {
        LOG.error("Failed to schedule replication for block Id {}", entry.getKey());
      }
    }
  }

  private void checkOverReplication(Set<Long> inodes) {
    Map<Long, Integer> overReplicated = Maps.newHashMap();

    for (long inodeId : inodes) {
      try (LockedInodePath inodePath =
          mInodeTree.lockFullInodePath(inodeId, InodeTree.LockMode.READ)) {
        InodeFile file = inodePath.getInodeFile();
        List<Long> blockIds = file.getBlockIds();
        for (BlockInfo blockInfo : mBlockMaster.getBlockInfoList(blockIds)) {
          if (blockInfo.getLocations().size() > file.getReplicationMax()) {
            // deal with over replication by scheduling evictions
            overReplicated.put(blockInfo.getBlockId(),
                blockInfo.getLocations().size() - file.getReplicationMax());
          }
        }
      } catch (FileDoesNotExistException e) {
        LOG.warn("Failed to check inodeId {} for replicationMax", inodeId);
      }
    }

    for (Map.Entry<Long, Integer> entry : overReplicated.entrySet()) {
      try {
        mEvictHandler.scheduleEvict(entry.getKey(), entry.getValue());
      } catch (AlluxioException e) {
        LOG.error("Failed to schedule replication for block Id {}", entry.getKey());
      }
    }

  }
}
