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
 * A factory for creating action definitions.
 */
public interface ActionDefinitionFactory {
  /**
   * Creates an action definition from a serialized string.
   *
   * @param body the body content of the action definition
   * @return the action definition with the input body
   */
  ActionDefinition create(String body);

  /**
   * @return the name of the action
   */
  String getName();
}
