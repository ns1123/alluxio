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

import alluxio.AlluxioURI;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.File.UpdateInodeFileEntry;

import java.util.Collections;
import java.util.List;

/**
 * The data structure exposed to the closure to be run through {@link FileSystemMaster#exec}.
 */
public final class ExecContext {
  private final JournalContext mJournalContext;
  private final LockedInodePath mInodePath;
  private final InodeTree mInodeTree;
  private final MountTable mMountTable;

  /**
   * @param journalContext the journal context
   * @param inodePath the locked inode path
   * @param inodeTree the inode tree
   * @param mountTable the mount table
   */
  public ExecContext(JournalContext journalContext,
      LockedInodePath inodePath,
      InodeTree inodeTree,
      MountTable mountTable) {
    mJournalContext = journalContext;
    mInodePath = inodePath;
    mInodeTree = inodeTree;
    mMountTable = mountTable;
  }

  /**
   * @return the last inode of the locked inode path
   */
  public Inode getInode() {
    try {
      return mInodePath.getInode();
    } catch (FileDoesNotExistException e) {
      // exec will lockFullInodePath before creating ExecContext, so the path must exist.
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the mount information for the path
   */
  public MountTable.Resolution getMountInfo() {
    try {
      return mMountTable.resolve(mInodePath.getUri());
    } catch (InvalidPathException e) {
      // exec will lockFullInodePath before creating ExecContext, so the path must be valid.
      throw new RuntimeException(e);
    }
  }

  /**
   * Updates file inode and journal the change.
   *
   * @param entry the journal entry
   */
  public void updateInodeFile(UpdateInodeFileEntry entry) {
    mInodeTree.updateInodeFile(mJournalContext, entry);
  }

  /**
   * Updates inode and journal the change.
   *
   * @param entry the journal entry
   */
  public void updateInode(UpdateInodeEntry entry) {
    mInodeTree.updateInode(mJournalContext, entry);
  }

  /**
   * Propagates the persisted status to all inodes along the path from the give inode up to the
   * mount point.
   */
  public void propagatePersisted() {
    List<Inode> inodes = mInodePath.getInodeList();
    // Traverse the inodes from target inode to the root.
    Collections.reverse(inodes);
    for (Inode inode : inodes) {
      // WRITE lock is held for the target inode,
      // READ lock is held for parent inodes.
      // Since the persistence status can only change from NOT_PERSISTED to PERSISTED,
      // even when there might be concurrent updates of the persistence status for the same
      // parent inode while holding the READ lock, eventually, the persistence status will be
      // correctly updated to PERSISTED.
      AlluxioURI path;
      try {
        path = mInodeTree.getPath(inode);
      } catch (FileDoesNotExistException e) {
        // Since the full path is locked before creating ExecContext, the path must exist.
        throw new RuntimeException(e);
      }
      if (mMountTable.isMountPoint(path)) {
        // Stop propagating the persisted status at mount points.
        break;
      }
      if (inode.isPersisted()) {
        // Stop if a persisted directory is encountered.
        break;
      }
      mInodeTree.updateInode(mJournalContext, UpdateInodeEntry.newBuilder()
          .setId(inode.getId())
          .setPersistenceState(PersistenceState.PERSISTED.name())
          .build());
    }
  }
}
