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

import alluxio.proto.journal.Privilege.PPrivilege;
import alluxio.util.CommonUtils;
import alluxio.wire.Privilege;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Util methods for working with privileges.
 */
public final class PrivilegeUtils {
  /**
   * @param pprivileges a list of protocol buffer type privileges
   * @return a corresponding list of {@link Privilege}
   */
  public static List<Privilege> fromProto(Collection<PPrivilege> pprivileges) {
    List<Privilege> privileges = new ArrayList<>();
    for (PPrivilege pprivilege : pprivileges) {
      privileges.add(fromProto(pprivilege));
    }
    return privileges;
  }

  /**
   * @param pprivilege a protocol buffer type privilege
   * @return the corresponding {@link Privilege}
   */
  public static Privilege fromProto(PPrivilege pprivilege) {
    switch (pprivilege) {
      case FREE_PRIVILEGE:
        return Privilege.FREE;
      case PIN_PRIVILEGE:
        return Privilege.PIN;
      case TTL_PRIVILEGE:
        return Privilege.TTL;
      case REPLICATION_PRIVILEGE:
        return Privilege.REPLICATION;
      default:
        throw new IllegalArgumentException("Unrecognized pprivilege: " + pprivilege);
    }
  }

  /**
   * @param privileges a list of {@link Privilege}
   * @return a corresponding list of protocol buffer type privileges
   */
  public static List<PPrivilege> toProto(Collection<Privilege> privileges) {
    List<PPrivilege> pprivileges = new ArrayList<>();
    for (Privilege privilege : privileges) {
      pprivileges.add(toProto(privilege));
    }
    return pprivileges;
  }

  /**
   * @param privilege an {@link Privilege}
   * @return the corresponding protocol buffer type privilege
   */
  public static PPrivilege toProto(Privilege privilege) {
    switch (privilege) {
      case FREE:
        return PPrivilege.FREE_PRIVILEGE;
      case PIN:
        return PPrivilege.PIN_PRIVILEGE;
      case TTL:
        return PPrivilege.TTL_PRIVILEGE;
      case REPLICATION:
        return PPrivilege.REPLICATION_PRIVILEGE;
      default:
        throw new IllegalArgumentException("Unrecognized privilege: " + privilege);
    }
  }

  /**
   * @param privilegeService a privilege service owning the mapping from groups to privileges
   * @param user the user to fetch the privileges for
   * @return the privileges for the user
   */
  public static Set<Privilege> getUserPrivileges(PrivilegeMaster privilegeService, String user) {
    List<String> groups;
    try {
      groups = CommonUtils.getGroups(user);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Set<Privilege> privileges = new HashSet<>();
    for (String group : groups) {
      Set<Privilege> groupPrivileges = privilegeService.getPrivileges(group);
      if (groupPrivileges != null) {
        privileges.addAll(groupPrivileges);
      }
    }
    return privileges;
  }

  private PrivilegeUtils() {} // Util class not intended for instantiation.
}
