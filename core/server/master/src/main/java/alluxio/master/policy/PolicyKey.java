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

package alluxio.master.policy;

import alluxio.master.policy.meta.PolicyDefinition;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * A class uniquely identifies a policy.
 */
public class PolicyKey {
  private final long mCreatedAt;
  private final String mName;

  /**
   * @param policy the policy definition
   * @return the policy key
   */
  public static PolicyKey fromDefinition(PolicyDefinition policy) {
    return new PolicyKey(policy.getCreatedAt(), policy.getName());
  }

  /**
   * @param createdAt policy creation timestamp
   * @param name name of the policy
   */
  public PolicyKey(long createdAt, String name) {
    mCreatedAt = createdAt;
    mName = name;
  }

  /**
   * @return policy creation timestamp
   */
  public long getCreatedAt() {
    return mCreatedAt;
  }

  /**
   * @return name of the policy
   */
  public String getName() {
    return mName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCreatedAt, mName);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof PolicyKey)) {
      return false;
    }
    PolicyKey key = (PolicyKey) obj;
    return Objects.equal(mCreatedAt, key.mCreatedAt) && Objects.equal(mName, key.mName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("createdAt", mCreatedAt)
        .add("name", mName)
        .toString();
  }
}
