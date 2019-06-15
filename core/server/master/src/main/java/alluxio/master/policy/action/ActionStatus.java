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

/**
 * The execution status of an action.
 *
 * The transitions are:
 * NOT_STARTED -> IN_PROGRESS -> PREPARED -> (COMMITTED | FAILED)
 *
 * COMMITTED and FAILED are the final states.
 */
public enum ActionStatus {
  NOT_STARTED,
  IN_PROGRESS,
  PREPARED,
  // The end states
  COMMITTED,
  FAILED,
  ;

  /**
   * @return whether the status is a terminal status, which means the status will not change anymore
   */
  public boolean isTerminal() {
    return this == COMMITTED || this == FAILED;
  }
}
