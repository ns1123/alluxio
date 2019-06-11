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
import alluxio.master.policy.action.ActionStatus;

import org.slf4j.Logger;

import java.io.IOException;

/**
 * Base class for both ALLUXIO and UFS REMOVE actions.
 * When {@link #start()}, the status becomes {@link ActionStatus#PREPARED} immediately.
 * The actual REMOVE action is executed during {@link #commit()}.
 *
 * It's designed like this so that STORE and REMOVE actions can be started concurrently, when they
 * all become PREPARED, commit STORE actions before REMOVE. REMOVE must happen after STORE for
 * data safety reason.
 */
public abstract class RemoveActionExecution extends AbstractActionExecution {
  @Override
  public synchronized ActionStatus start() {
    mStatus = ActionStatus.PREPARED;
    return mStatus;
  }

  @Override
  public ActionStatus update() throws IOException {
    return mStatus;
  }

  @Override
  public synchronized ActionStatus commit() {
    super.commit();
    try {
      remove();
      mStatus = ActionStatus.COMMITTED;
    } catch (Exception e) {
      getLogger().warn("Remove action failed", e);
      mStatus = ActionStatus.FAILED;
      mException = e;
    }
    return mStatus;
  }

  /**
   * @return the logger
   */
  protected abstract Logger getLogger();

  /**
   * Do the actual data removal.
   * It blocks until success or failure.
   * If it tails, {@link #close()} should clean up resources.
   * This method is called by {@link #commit()}, status update and exception bookkeeping are taken
   * care of by {@link #commit()}.
   *
   * @throws Exception when failed
   */
  protected abstract void remove() throws Exception;

  @Override
  public synchronized void close() throws IOException {
    // No-op by default.
  }
}
