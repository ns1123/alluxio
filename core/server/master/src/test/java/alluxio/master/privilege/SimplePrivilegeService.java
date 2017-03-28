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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A privilege checker controlled by a privileges map defined at construction.
 */
public final class SimplePrivilegeService implements PrivilegeService {
  private final Map<String, Set<Privilege>> mGroupToPrivilegeMap;

  public SimplePrivilegeService(Map<String, Set<Privilege>> groupToPrivilegeMap) {
    mGroupToPrivilegeMap = groupToPrivilegeMap;
  }

  @Override
  public boolean hasPrivilege(String group, Privilege privilege) {
    Set<Privilege> privileges = mGroupToPrivilegeMap.get(group);
    return privileges != null && privileges.contains(privilege);
  }

  @Override
  public Set<Privilege> getPrivileges(String group) {
    Set<Privilege> privileges = mGroupToPrivilegeMap.get(group);
    return privileges == null ? new HashSet<Privilege>() : privileges;
  }
}
