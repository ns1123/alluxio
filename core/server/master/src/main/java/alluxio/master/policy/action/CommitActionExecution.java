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

import alluxio.master.policy.meta.InodeState;

import org.slf4j.Logger;

/**
 * Base class for ActionExecution implementations that only execute actions in {@link #commit()}.
 *
 * When {@link #start()}, the status becomes {@link ActionStatus#PREPARED} immediately.
 * The actual action is executed during {@link #commit()}.
 * This class handles status and exception management.
 */
public abstract class CommitActionExecution extends AbstractActionExecution {
  /**
   * @param ctx the execution context
   * @param path the Alluxio path
   * @param inode the inode
   */
  public CommitActionExecution(ActionExecutionContext ctx, String path, InodeState inode) {
    super(ctx, path, inode);
  }

  @Override
  public ActionStatus start() {
    super.start();
    mStatus = ActionStatus.PREPARED;
    return mStatus;
  }

  @Override
  public ActionStatus commit() {
    super.commit();
    try {
      doCommit();
      mStatus = ActionStatus.COMMITTED;
    } catch (Exception e) {
      String err = String.format("Action %s failed to commit", toString());
      getLogger().error(err, e);
      mStatus = ActionStatus.FAILED;
      mException = new Exception(err, e);
    }
    return mStatus;
  }

  /**
   * @return the logger
   */
  protected abstract Logger getLogger();

  /**
   * Do the actual commit action.
   * It blocks until success or failure.
   * If it tails, {@link #close()} should clean up resources.
   * This method is called by {@link #commit()}, status update and exception bookkeeping are taken
   * care of by {@link #commit()}.
   *
   * @throws Exception when failed
   */
  protected abstract void doCommit() throws Exception;
}
