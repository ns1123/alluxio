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

package alluxio.master.policy.meta;

import alluxio.proto.journal.Policy;

/**
 * The enforcement scope of a policy.
 */
public enum PolicyScope {
  /** The policy is enforced only on the single path, and no descendants. */
  SINGLE(1),
  /** The policy is enforced on the path, and all descendants. */
  RECURSIVE(2),
  /** The policy is enforced on the path, and all descendants within the same mount point. */
  RECURSIVE_MOUNT(3),
  ;

  private final int mValue;

  PolicyScope(int value) {
    mValue = value;
  }

  /**
   * @return the proto representation
   */
  public Policy.PolicyScope toProto() {
    return Policy.PolicyScope.values()[mValue - 1];
  }

  /**
   * @param proto  the proto representation
   * @return the PolicyScope from the proto representation
   */
  public static PolicyScope fromProto(Policy.PolicyScope proto) {
    // proto.getNumber() starts from 0
    return PolicyScope.values()[proto.getNumber()];
  }
}
