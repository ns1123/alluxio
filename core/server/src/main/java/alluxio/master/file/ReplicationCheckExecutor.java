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

package alluxio.master.file;

import alluxio.Constants;
import alluxio.exception.FileDoesNotExistException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.wire.BlockInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The executor to check block replication level periodically.
 */
@ThreadSafe
public final class ReplicationCheckExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Handle to the inode tree. */
  private final InodeTree mInodeTree;
  /** Handle to the block master. */
  private final BlockMaster mBlockMaster;

  /**
   * Constructs a new {@link ReplicationCheckExecutor}.
   */
  public ReplicationCheckExecutor(InodeTree inodeTree, BlockMaster blockMaster) {
    mInodeTree = inodeTree;
    mBlockMaster = blockMaster;
  }

  @Override
  public void heartbeat() throws InterruptedException {
    Set<Long> inodes;

    // Check the set of files that could possibly be under-replicated
    inodes = mInodeTree.getPinIdSet();
    checkUnderReplication(inodes);

    // Check the set of files that could possibly be over-replicated
    inodes = mInodeTree.getFiniteReplicationMaxFileIds();
    checkOverReplication(inodes);
  }

  @Override
  public void close() {
    // Nothing to clean up
  }

  private void checkUnderReplication(Set<Long> pinnedInodes) {
    for (long inodeId : pinnedInodes) {
      // TODO(binfan): calling lockFullInodePath locks the entire path from root to the target
      // file and may increase lock contention in this tree. Investigate if we could avoid
      // locking the entire path but just the inode file since this access is read-only.
      try (LockedInodePath inodePath = mInodeTree
          .lockFullInodePath(inodeId, InodeTree.LockMode.READ)) {
        InodeFile file = inodePath.getInodeFile();
        List<Long> blockIds = file.getBlockIds();
        for (BlockInfo blockInfo : mBlockMaster.getBlockInfoList(blockIds)) {
          if (blockInfo.getLocations().size() < file.getReplicationMin()) {
            // deal with the under replication case
          }
        }
      } catch (FileDoesNotExistException e) {
        LOG.warn("Failed to check inodeId {} for replicationMin", inodeId);
      }
    }
  }

  private void checkOverReplication(Set<Long> inodes) {
    for (long inodeId : inodes) {
      try (LockedInodePath inodePath = mInodeTree
          .lockFullInodePath(inodeId, InodeTree.LockMode.READ)) {
        InodeFile file = inodePath.getInodeFile();
        List<Long> blockIds = file.getBlockIds();
        for (BlockInfo blockInfo : mBlockMaster.getBlockInfoList(blockIds)) {
          if (blockInfo.getLocations().size() > file.getReplicationMax()) {
            // deal with over replication case
          }
        }
      } catch (FileDoesNotExistException e) {
        LOG.warn("Failed to check inodeId {} for replicationMax", inodeId);
      }
    }
  }
}
