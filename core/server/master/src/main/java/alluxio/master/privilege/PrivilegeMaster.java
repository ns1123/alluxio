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

import alluxio.master.Master;
import alluxio.wire.Privilege;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A mapping from group to privileges. Implementations are expected to be threadsafe.
 */
@ThreadSafe
public interface PrivilegeMaster extends Master {
   /**
   * @param group a group
   * @param privilege a privilege
   * @return whether the given group has the specified privilege
   */
  boolean hasPrivilege(String group, Privilege privilege);

  /**
   * @return a snapshot of all group privilege information
   */
  Map<String, Set<Privilege>> getGroupToPrivilegesMapping();

  /**
   * @param group a group
   * @return the set of privileges granted to the group
   */
  Set<Privilege> getPrivileges(String group);

  /**
   * Updates privileges and journals the update.
   *
   * @param group the group to grant or revoke the privileges for
   * @param privileges the privileges to grant or revoke
   * @param grant if true, grant the privileges; otherwise revoke them
   * @return the updated privileges for the group
   */
  Set<Privilege> updatePrivileges(String group, List<Privilege> privileges, boolean grant);
}
