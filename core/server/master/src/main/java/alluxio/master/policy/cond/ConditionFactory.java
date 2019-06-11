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

package alluxio.master.policy.cond;

/**
 * A factory for creating conditions.
 */
public interface ConditionFactory {
  /**
   * Creates a condition from a serialized string.
   *
   * @param serialized the serialized string to deserialize to a condition
   * @return the condition if the input can be deserialized, null otherwise
   */
  Condition deserialize(String serialized);
}
