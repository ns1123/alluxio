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

package alluxio.wire;

import alluxio.thrift.TPrivilege;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Closed-source equivalent of {@link ThriftUtils}.
 */
public final class ClosedSourceThriftUtils {

  /**
   * @param tprivileges a list of thrift type privileges
   * @return a corresponding list of {@link Privilege}
   */
  public static List<Privilege> fromThrift(Collection<TPrivilege> tprivileges) {
    List<Privilege> privileges = new ArrayList<>();
    for (TPrivilege tprivilege : tprivileges) {
      privileges.add(fromThrift(tprivilege));
    }
    return privileges;
  }

  /**
   * @param tprivilege a protocol buffer type privilege
   * @return the corresponding {@link Privilege}
   */
  public static Privilege fromThrift(TPrivilege tprivilege) {
    switch (tprivilege) {
      case FREE:
        return Privilege.FREE;
      case PIN:
        return Privilege.PIN;
      case TTL:
        return Privilege.TTL;
      case REPLICATION:
        return Privilege.REPLICATION;
      default:
        throw new IllegalArgumentException("Unrecognized tprivilege: " + tprivilege);
    }
  }

  /**
   * @param privileges a list of {@link Privilege}
   * @return a corresponding list of thrift type privileges
   */
  public static List<TPrivilege> toThrift(Collection<Privilege> privileges) {
    List<TPrivilege> tprivileges = new ArrayList<>();
    for (Privilege privilege : privileges) {
      tprivileges.add(toThrift(privilege));
    }
    return tprivileges;
  }

  /**
   * @param privilege an {@link Privilege}
   * @return the corresponding protocol buffer type privilege
   */
  public static TPrivilege toThrift(Privilege privilege) {
    switch (privilege) {
      case FREE:
        return TPrivilege.FREE;
      case PIN:
        return TPrivilege.PIN;
      case TTL:
        return TPrivilege.TTL;
      case REPLICATION:
        return TPrivilege.REPLICATION;
      default:
        throw new IllegalArgumentException("Unrecognized privilege: " + privilege);
    }
  }

  private ClosedSourceThriftUtils() {} // not intended for instantiation
}
