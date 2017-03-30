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

package alluxio.master.privilege;

import alluxio.wire.Privilege;

import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A mapping from group to privileges. Implementations are expected to be threadsafe.
 */
@ThreadSafe
public interface PrivilegeService {
  /**
   * @param group a group
   * @param privilege a privilege
   * @return whether the given group has the specified privilege
   */
  boolean hasPrivilege(String group, Privilege privilege);

  /**
   * @param group a group
   * @return the set of privileges granted to the group
   */
  Set<Privilege> getPrivileges(String group);
}
