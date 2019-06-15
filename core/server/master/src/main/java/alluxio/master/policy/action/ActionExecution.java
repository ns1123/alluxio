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

import java.io.Closeable;
import java.io.IOException;

/**
 * This manages the state for the execution of an action.
 */
public interface ActionExecution extends Closeable {
  /**
   * Starts the execution for the action.
   *
   * If the action or any sub action fails to be started due to an exception, the status returned is
   * {@link ActionStatus#FAILED}, {@link #getException()} tells why it fails to be started.
   *
   * When the returned status is {@link ActionStatus#FAILED}, {@link #close()} should be called to
   * clean up resources.
   *
   * If the action is started successfully, the returned status might be
   * {@link ActionStatus#IN_PROGRESS}, or {@link ActionStatus#PREPARED}.
   *
   * @return the action status
   */
  ActionStatus start();

  /**
   * Updates the action execution, and returns the updated status. This should be periodically
   * invoked after {@link #start()} until the status becomes {@link ActionStatus#PREPARED} or
   * {@link ActionStatus#FAILED}.
   * If the status becomes {@link ActionStatus#PREPARED}, then the outside caller should call
   * {@link #commit()}.
   * After {@link #commit()}, this method should return {@link ActionStatus#COMMITTED} or
   * {@link ActionStatus#FAILED} deterministically.
   *
   * @return the current action status
   * @throws IOException when failed to get status update, this does not mean the action fails
   */
  ActionStatus update() throws IOException;

  /**
   * Commits the action.
   * The current action status must be {@link ActionStatus#PREPARED}.
   * This call blocks until the status {@link ActionStatus#isTerminal()}.
   *
   * @return one of the terminal status
   * @throws IllegalStateException when the current status is not {@link ActionStatus#PREPARED}
   */
  ActionStatus commit();

  /**
   * @return the exception of the action since {@link #start()} or null if there is no exception
   */
  Exception getException();
}
