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

package alluxio.master.policy.action.data;

import alluxio.AlluxioURI;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.xattr.ExtendedAttribute;
import alluxio.master.policy.action.ActionExecutionContext;
import alluxio.master.policy.action.CommitActionExecution;
import alluxio.master.policy.meta.InodeState;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.ExceptionUtils;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The action to remove a persisted file or an empty directory from a group of sub UFSes in one
 * union UFS.
 */
@ThreadSafe
public final class UfsRemoveActionExecution extends CommitActionExecution {
  private static final Logger LOG = LoggerFactory.getLogger(UfsRemoveActionExecution.class);

  private final List<String> mSubUfses;
  /**
   * A list of futures representing tasks to remove data from sub UFSes.
   * mFutures[i] corresponds to mSubUfses[i].
   */
  private final List<Future<Void>> mFutures;

  /**
   * @param ctx the context
   * @param path the Alluxio path
   * @param inode the inode state
   * @param subUfses a set of sub UFS modifiers, must not be empty
   */
  public UfsRemoveActionExecution(ActionExecutionContext ctx, String path, InodeState inode,
      Set<String> subUfses) {
    super(ctx, path, inode);
    Preconditions.checkState(subUfses.size() > 0);
    mSubUfses = new ArrayList<>(subUfses);
    mFutures = new ArrayList<>(subUfses.size());
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected void doCommit() throws Exception {
    if (mInode.isMountPoint()) {
      // Removing a mount point is a noop.
      return;
    }

    List<String> pathsToRemove = new ArrayList<>(mSubUfses.size());
    mContext.getFileSystemMaster().exec(mInode.getId(), LockPattern.READ, ctx -> {
      MountTable.Resolution resolution = ctx.getMountInfo();

      // Check union UFS.
      AlluxioURI ufsUri = resolution.getUri();
      if (ufsUri.getScheme() == null || !DataActionUtils.isUnionUfs(ufsUri)) {
        throw new IllegalStateException(
            String.format("UFS:REMOVE can only be executed on union UFS, but the UFS URI is %s",
                ufsUri.toString()));
      }

      // Remove path from sub UFSes asynchronously.
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        for (String subUfs : mSubUfses) {
          String pathToRemove = DataActionUtils.createUnionSubUfsUri(ufsUri, subUfs);
          pathsToRemove.add(pathToRemove);
          Future<Void> future = mContext.getExecutorService().submit(() -> {
            boolean deleted;
            if (mInode.isFile()) {
              deleted = ufs.deleteExistingFile(pathToRemove);
            } else {
              deleted = ufs.deleteExistingDirectory(pathToRemove);
            }
            if (deleted) {
              LOG.debug("Path {} is removed from UFS", pathToRemove);
            } else if (!ufs.exists(pathToRemove)) {
              LOG.debug("Path {} does not exist in UFS", pathToRemove);
            } else {
              Preconditions.checkState(mInode.isDirectory());
              throw new IOException(String.format("Directory %s is not empty", pathToRemove));
            }
            return null;
          });
          mFutures.add(future);
        }
      }
    });

    // Wait for all tasks to finish.
    List<Integer> succeededFutures = new LinkedList<>();
    List<IOException> exceptions = new LinkedList<>();
    for (int i = 0; i < mFutures.size(); i++) {
      try {
        mFutures.get(i).get();
        succeededFutures.add(i);
      } catch (InterruptedException e) {
        String err = String.format("Thread interrupted while removing %s", pathsToRemove.get(i));
        exceptions.add(new IOException(err, e));
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        String err = String.format("Failed to remove %s", pathsToRemove.get(i));
        exceptions.add(new IOException(err, e.getCause()));
      }
    }

    // Update and journal sub ufs persistence status of the inode.
    //
    // Since UFS:REMOVE cannot exist without UFS:STORE, and STORE must commit before REMOVE,
    // UFS:REMOVE will not affect the inode's persistence state.
    if (!succeededFutures.isEmpty()) {
      mContext.getFileSystemMaster().exec(mInode.getId(), LockPattern.WRITE_INODE, ctx -> {
        UpdateInodeEntry.Builder builder = UpdateInodeEntry.newBuilder().setId(mInode.getId());
        for (int i : succeededFutures) {
          builder.putAllXAttr(CommonUtils.convertToByteString(ctx.getInode().getXAttr()));
          builder.putXAttr(ExtendedAttribute.PERSISTENCE_STATE.forId(mSubUfses.get(i)),
              ByteString.copyFrom(
                  ExtendedAttribute.PERSISTENCE_STATE.encode(PersistenceState.NOT_PERSISTED)));
        }
        ctx.updateInode(builder.build());
      });
    }

    // If any task fails, throw exception.
    if (!exceptions.isEmpty()) {
      throw new IOException(exceptions.stream().map(ExceptionUtils::getChainedExceptionMessages)
          .collect(Collectors.joining(",\n")));
    }
  }

  @Override
  public void close() throws IOException {
    for (Future<Void> future : mFutures) {
      future.cancel(true);
    }
  }

  @Override
  public String getDescription() {
    return mSubUfses.stream().map(subUfs -> String.format("UFS[%s]:REMOVE", subUfs))
        .collect(Collectors.joining(", "));
  }
}
