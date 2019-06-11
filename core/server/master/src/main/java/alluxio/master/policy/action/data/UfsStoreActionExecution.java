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
import alluxio.client.job.JobMasterClientPool;
import alluxio.job.JobConfig;
import alluxio.job.persist.PersistConfig;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.xattr.ExtendedAttribute;
import alluxio.master.policy.action.AbstractActionExecution;
import alluxio.master.policy.action.ActionExecution;
import alluxio.master.policy.action.ActionExecutionContext;
import alluxio.master.policy.action.ActionStatus;
import alluxio.master.policy.action.JobServiceActionExecution;
import alluxio.master.policy.meta.InodeState;
import alluxio.metrics.MasterMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The action to persist a file from Alluxio to one UFS or a group sub UFSes (at least 1)
 * in one union UFS.
 */
@ThreadSafe
public final class UfsStoreActionExecution extends AbstractActionExecution {
  private static final Logger LOG = LoggerFactory.getLogger(UfsStoreActionExecution.class);

  private final ActionExecutionContext mContext;
  private final InodeState mInode;
  private final Set<String> mSubUfses;
  private final Set<TempUfsStoreActionExecution> mActions;
  private final Set<String> mTempUfsPaths;

  /**
   * @param ctx the context
   * @param inode the inode state
   * @param subUfs the list of sub UFS modifiers when storing to a group of sub UFSes in one union
   *    UFS or empty when storing to one UFS
   */
  public UfsStoreActionExecution(ActionExecutionContext ctx, InodeState inode, Set<String> subUfs) {
    mContext = ctx;
    mInode = inode;
    mSubUfses = subUfs;
    mActions = Collections.synchronizedSet(new HashSet<>(mSubUfses.size()));
    mTempUfsPaths = Collections.synchronizedSet(new HashSet<>(mSubUfses.size()));
  }

  @Override
  public synchronized ActionStatus start() {
    FileInfo fileInfo;
    try {
      fileInfo = mContext.getFileSystemMaster().getFileInfo(mInode.getId());
    } catch (Exception e) {
      mStatus = ActionStatus.FAILED;
      mException = new Exception(String.format("Failed to get FileInfo for inode with id %d",
          mInode.getId()), e);
      return mStatus;
    }

    // Generate temporary paths to be used by the persist jobs.
    AlluxioURI ufsUri = new AlluxioURI(fileInfo.getUfsPath());
    if (mSubUfses.isEmpty()) {
      // Store to one UFS.
      mTempUfsPaths.add(PathUtils.temporaryFileName(System.currentTimeMillis(), ufsUri.toString()));
    } else {
      // Store to a group of sub UFSes.
      if (ufsUri.getScheme() == null || !DataActionUtils.isUnionUfs(ufsUri)) {
        throw new IllegalStateException(String.format("%s is not union UFS", ufsUri.toString()));
      }
      for (String subUfs : mSubUfses) {
        String subUfsUri = DataActionUtils.createUnionSubUfsUri(ufsUri, subUfs);
        mTempUfsPaths.add(PathUtils.temporaryFileName(System.currentTimeMillis(), subUfsUri));
      }
    }

    // Create and start the jobs.
    synchronized (mTempUfsPaths) {
      for (String tempUfsPath : mTempUfsPaths) {
        TempUfsStoreActionExecution action = new TempUfsStoreActionExecution(
            mContext.getJobMasterClientPool(), fileInfo.getPath(), fileInfo.getMountId(),
            tempUfsPath);
        if (action.start() == ActionStatus.FAILED) {
          mStatus = ActionStatus.FAILED;
          mException = action.getException();
          return mStatus;
        }
        mActions.add(action);
      }
    }

    mStatus = ActionStatus.IN_PROGRESS;
    return mStatus;
  }

  @Override
  public ActionStatus update() throws IOException {
    if (mStatus != ActionStatus.IN_PROGRESS) {
      return mStatus;
    }
    int prepared = 0;
    synchronized (mActions) {
      for (TempUfsStoreActionExecution action : mActions) {
        switch (action.update()) {
          case PREPARED:
            prepared++;
            break;
          case FAILED:
            mStatus = ActionStatus.FAILED;
            mException = action.getException();
            return mStatus;
          default:
            // mStatus is still IN_PROGRESS.
            break;
        }
      }
    }
    if (prepared == mActions.size()) {
      mStatus = ActionStatus.PREPARED;
    }
    return mStatus;
  }

  @Override
  public void preCommit() {
    mActions.stream().forEach(ActionExecution::preCommit);
    super.preCommit();
  }

  @Override
  public synchronized ActionStatus commit() {
    super.commit();
    // Skip committing the TempUfsStore actions, since those commits always succeed.

    try {
      mContext.getFileSystemMaster().exec(mInode.getId(), LockPattern.WRITE_INODE, context -> {
        Inode inode = context.getInode();
        MountTable.Resolution resolution = context.getMountInfo();

        try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
          UnderFileSystem ufs = ufsResource.get();
          AlluxioURI ufsUri = resolution.getUri();
          synchronized (mTempUfsPaths) {
            for (String tempUfsPath : mTempUfsPaths) {
              // Rename temporary paths to the latest path in UFS.
              //
              // For example, when persisting starts, Alluxio path is /a, so temporary path is like
              // /a.tmp, but during persisting, /a is renamed to /b, now, after /a.tmp is persisted,
              // rename /a.tmp to /b in UFS.
              //
              // Keep scheme and authority of tempUfsPath, replace its path with the path
              // of ufsUri. This handles union UFS, for example:
              // ufsUri is union:///a,
              // tempUfsPath is union://ufs1/tmp,
              // then tempUfsPath should be renamed to union://ufs1/a.
              URI tempUri = new URI(tempUfsPath);
              URI targetUri =
                  new URI(tempUri.getScheme(), tempUri.getAuthority(), ufsUri.getPath(), null,
                      null);
              String targetPath = targetUri.toString();
              try {
                if (!ufs.renameRenamableFile(tempUfsPath, targetPath)) {
                  boolean targetExists = false;
                  try {
                    ufs.getFileStatus(targetPath);
                    // rename failed, because the target file already exists. Treat the rename
                    // as successful, to be idempotent.
                    targetExists = true;
                  } catch (IOException e) {
                    // ignore the error for file status
                  }
                  if (!targetExists) {
                    throw new IOException(
                        String.format("Failed to rename %s to %s with unknown reason.", tempUfsPath,
                            targetPath));
                  }
                }
                ufs.setOwner(targetPath, inode.getOwner(), inode.getGroup());
                ufs.setMode(targetPath, inode.getMode());
              } catch (IOException e) {
                LOG.error(String.format("Failed to rename %s to %s", tempUfsPath, targetPath), e);
                // Best effort to clean up the temporary file.
                try {
                  if (!ufs.deleteExistingFile(tempUfsPath)) {
                    throw new IOException(String.format("Temporary file %s does not exist",
                        tempUfsPath));
                  }
                } catch (IOException e1) {
                  LOG.error(String.format("Failed to delete temporary file %s", tempUfsPath), e1);
                  e.addSuppressed(e1);
                }
                throw e;
              }

              if (!mSubUfses.isEmpty()) {
                // Update and journal persistence state of the sub ufs.
                UpdateInodeEntry.Builder builder =
                    UpdateInodeEntry.newBuilder().setId(mInode.getId());
                builder.putAllXAttr(CommonUtils.convertToByteString(inode.getXAttr()));
                builder
                    .putXAttr(ExtendedAttribute.PERSISTENCE_STATE.forId(targetUri.getAuthority()),
                        ByteString.copyFrom(ExtendedAttribute.PERSISTENCE_STATE
                            .encode(PersistenceState.PERSISTED)));
                context.updateInode(builder.build());
              }
            }
          }
        }

        // Update and journal persistence state of the inode and its ancestors.
        if (!inode.isPersisted()) {
          context.propagatePersisted();
          MetricsSystem.counter(MasterMetrics.FILES_PERSISTED).inc();
        }
      });

      mStatus = ActionStatus.COMMITTED;
    } catch (Exception e) {
      mStatus = ActionStatus.FAILED;
      mException = new ExecutionException(String.format("UFS:STORE for inode %d failed to commit",
          mInode.getId()), e);
    }
    return mStatus;
  }

  @Override
  public synchronized void close() throws IOException {
    if (mStatus == ActionStatus.PREPARED || mStatus == ActionStatus.COMMITTED) {
      // The jobs must have finished.
      return;
    }

    // Cancel the remaining running jobs.
    synchronized (mActions) {
      for (TempUfsStoreActionExecution action : mActions) {
        action.close();
      }
    }
  }

  @Override
  public String toString() {
    return mSubUfses.stream().map(subUfs -> String.format("UFS[%s]:STORE on Inode(id=%d)", subUfs,
        mInode.getId())).collect(Collectors.joining(", "));
  }

  /**
   * An action to persist an Alluxio path to a temporary UFS path through job service.
   */
  private final class TempUfsStoreActionExecution extends JobServiceActionExecution {
    private final String mAlluxioPath;
    private final long mMountId;
    private final String mTempUfsPath;

    /**
     * @param clientPool the client pool
     * @param alluxioPath the alluxio path
     * @param mountId the mount ID for the alluxio path
     * @param tempUfsPath the temporary UFS path to persist the alluxio path to
     */
    public TempUfsStoreActionExecution(JobMasterClientPool clientPool, String alluxioPath,
        long mountId, String tempUfsPath) {
      super(clientPool);
      mAlluxioPath = alluxioPath;
      mMountId = mountId;
      mTempUfsPath = tempUfsPath;
    }

    @Override
    protected Logger getLogger() {
      return LOG;
    }

    @Override
    protected JobConfig createJobConfig() {
      return new PersistConfig(mAlluxioPath, mMountId, false, mTempUfsPath);
    }

    @Override
    public String toString() {
      return String.format("UFS:STORE(Alluxio file: %s, temporary UFS path: %s)", mAlluxioPath,
          mTempUfsPath);
    }
  }
}
