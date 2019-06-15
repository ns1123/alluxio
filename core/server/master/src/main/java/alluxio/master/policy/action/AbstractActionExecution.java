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

import com.google.common.base.Preconditions;

/**
 * A base class for implementing {@link ActionExecution}.
 */
public abstract class AbstractActionExecution implements ActionExecution {
  /** The current action status. */
  protected volatile ActionStatus mStatus = ActionStatus.NOT_STARTED;
  /** The latest exception of the action since start. */
  protected volatile Exception mException;

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
}
