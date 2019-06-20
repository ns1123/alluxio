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

package alluxio.master.policy.action;

import alluxio.exception.PreconditionMessage;
import alluxio.master.policy.meta.InodeState;

import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * A base class for implementing {@link ActionExecution}.
 */
public abstract class AbstractActionExecution implements ActionExecution {
  /** Resources needed for executing actions. */
  protected final ActionExecutionContext mContext;
  /**
   * The Alluxio path.
   * This might no longer be the path for the inode when executing the action.
   */
  protected final String mPath;
  /** The inode state. */
  protected final InodeState mInode;
  /** The current action status. */
  protected volatile ActionStatus mStatus = ActionStatus.NOT_STARTED;
  /** The latest exception of the action since start. */
  protected volatile Exception mException;

  /**
   * @param ctx the execution context
   * @param path the Alluxio path
   * @param inode the inode
   */
  public AbstractActionExecution(ActionExecutionContext ctx, String path, InodeState inode) {
    mContext = ctx;
    mPath = path;
    mInode = inode;
  }

  @Override
  public ActionStatus start() {
    Preconditions.checkState(mStatus == ActionStatus.NOT_STARTED,
        PreconditionMessage.UNEXPECTED_ACTION_STATUS.toString(),
        getClass().getName(), mStatus, ActionStatus.NOT_STARTED);
    return mStatus;
  }

  @Override
  public ActionStatus update() throws IOException {
    return mStatus;
  }

  @Override
  public ActionStatus commit() {
    Preconditions.checkState(mStatus == ActionStatus.PREPARED,
        PreconditionMessage.UNEXPECTED_ACTION_STATUS.toString(),
        getClass().getName(), mStatus, ActionStatus.PREPARED);
    return mStatus;
  }

  @Override
  public Exception getException() {
    return mException;
  }

  @Override
  public void close() throws IOException {
    // NOOP
  }

  @Override
  public String toString() {
    return String.format("%s on %s(%s) with id(%d)", getDescription(),
        mInode.isDirectory() ? "directory" : "file", mPath, mInode.getId());
  }
}
