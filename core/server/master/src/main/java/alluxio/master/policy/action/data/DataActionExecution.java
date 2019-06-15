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

import alluxio.master.policy.action.AbstractActionExecution;
import alluxio.master.policy.action.ActionExecution;
import alluxio.master.policy.action.ActionExecutionContext;
import alluxio.master.policy.action.ActionStatus;
import alluxio.master.policy.meta.InodeState;

import com.google.common.collect.Iterables;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This manages the execution and all state for the DATA action.
 *
 * The DATA action contains STORE and REMOVE sub actions, the execution is a two-phase commit:
 *
 * 1. Prepare:
 * 1.1 When {@link #start()}, all sub actions are started and are {@link ActionStatus#IN_PROGRESS}.
 * 1.2 When all sub actions are {@link ActionStatus#PREPARED}, the DATA action is
 * {@link ActionStatus#PREPARED} too.
 *
 * 2. Commit:
 * {@link #commit()} will first commit the STORE sub actions concurrently, if all succeed, then the
 * REMOVE sub actions will be committed concurrently.
 *
 * If all sub actions are committed, the DATA action is {@link ActionStatus#COMMITTED}.
 * Otherwise, when one of the sub actions fails, the DATA action is {@link ActionStatus#FAILED}.
 */
@ThreadSafe
public class DataActionExecution extends AbstractActionExecution {
  private static final Logger LOG = LoggerFactory.getLogger(DataActionExecution.class);

  private final DataActionDefinition mDefinition;
  private final ActionExecutionContext mContext;
  private final String mPath;
  private final InodeState mInode;
  private final List<ActionExecution> mStoreActions = new ArrayList<>(2);
  private final List<ActionExecution> mRemoveActions = new ArrayList<>(2);
  private final int mNumActions;
  private final Closer mCloser = Closer.create();

  /**
   * Creates an instance of {@link DataActionExecution}.
   *
   * @param ctx the context
   * @param path the Alluxio path
   * @param inode the inode state for the path
   * @param definition the action definition
   */
  public DataActionExecution(ActionExecutionContext ctx, String path, InodeState inode,
      DataActionDefinition definition) {
    LOG.debug("Constructing DataActionExecution for path {} with inode ID {}", path, inode.getId());
    mDefinition = definition;
    mContext = ctx;
    mPath = path;
    mInode = inode;
    if (definition.hasAlluxioStore()) {
      if (mInode.isDirectory()) {
        // TODO(gpang): implement
      } else {
        mStoreActions.add(new AlluxioStoreActionExecution(mContext, mPath));
      }
    }
    if (definition.hasAlluxioRemove()) {
      if (mInode.isDirectory()) {
        // TODO(gpang): implement
      } else {
        mRemoveActions.add(new AlluxioRemoveActionExecution(mContext, mPath));
      }
    }
    if (definition.hasUfsStore()) {
      if (mInode.isDirectory()) {
        // TODO(gpang): implement
      } else {
        mStoreActions.add(new UfsStoreActionExecution(mContext, mInode,
            definition.getUfsStoreLocationModifiers()));
      }
    }
    if (definition.hasUfsRemove()) {
      if (mInode.isDirectory()) {
        // TODO(gpang): implement
      } else {
        mRemoveActions.add(new UfsRemoveActionExecution(mContext, mInode,
            definition.getUfsRemoveLocationModifiers()));
      }
    }
    Stream.concat(mStoreActions.stream(), mRemoveActions.stream()).forEach(mCloser::register);
    mNumActions = mStoreActions.size() + mRemoveActions.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("%s for path %s with inode ID %d is constructed",
          this, path, inode.getId()));
    }
  }

  @Override
  public synchronized ActionStatus start() {
    for (ActionExecution action : Iterables.concat(mStoreActions, mRemoveActions)) {
      if (action.start() == ActionStatus.FAILED) {
        mStatus = ActionStatus.FAILED;
        mException = action.getException();
        return mStatus;
      }
    }
    mStatus = ActionStatus.IN_PROGRESS;
    return mStatus;
  }

  @Override
  public synchronized ActionStatus update() throws IOException {
    if (mStatus != ActionStatus.IN_PROGRESS) {
      return mStatus;
    }
    int prepared = 0;
    for (ActionExecution action : Iterables.concat(mStoreActions, mRemoveActions)) {
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
    if (prepared == mNumActions) {
      mStatus = ActionStatus.PREPARED;
    }
    return mStatus;
  }

  @Override
  public ActionStatus commit() {
    super.commit();
    mStatus = commit(mStoreActions);
    if (mStatus == ActionStatus.FAILED) {
      return mStatus;
    }
    mStatus = commit(mRemoveActions);
    if (mStatus == ActionStatus.FAILED) {
      return mStatus;
    }
    mStatus = ActionStatus.COMMITTED;
    return mStatus;
  }

  private ActionStatus commit(List<ActionExecution> actions) {
    List<Future<ActionStatus>> futures = new ArrayList<>();
    for (ActionExecution action : actions) {
      futures.add(mContext.getExecutorService().submit(action::commit));
    }
    for (int i = 0; i < futures.size(); i++) {
      try {
        if (futures.get(i).get() == ActionStatus.FAILED) {
          mStatus = ActionStatus.FAILED;
          mException = actions.get(i).getException();
          break;
        }
      } catch (InterruptedException e) {
        LOG.warn("Thread interrupted while committing", e);
        Thread.currentThread().interrupt();
        return ActionStatus.FAILED;
      } catch (ExecutionException e) {
        LOG.warn("Commit should not throw exception. path: {} inode id: {} e: {}", mPath,
            mInode.getId(), e.getMessage());
        throw new RuntimeException(e);
      }
    }
    return mStatus;
  }

  @Override
  public synchronized void close() throws IOException {
    mCloser.close();
  }

  @Override
  public String toString() {
    return "DATA("
        + Stream.concat(mStoreActions.stream(), mRemoveActions.stream())
        .map(Object::toString)
        .collect(Collectors.joining(", "))
        + ")";
  }
}
