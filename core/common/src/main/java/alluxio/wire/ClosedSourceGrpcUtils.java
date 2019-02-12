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

import alluxio.grpc.GrpcUtils;
import alluxio.grpc.PPrivilege;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Closed-source equivalent of {@link GrpcUtils}.
 */
public final class ClosedSourceGrpcUtils {

  /**
   * @param pPrivileges a list of proto type privileges
   * @return a corresponding list of {@link Privilege}
   */
  public static List<Privilege> fromProto(Collection<PPrivilege> pPrivileges) {
    List<Privilege> privileges = new ArrayList<>();
    for (PPrivilege pPrivilege : pPrivileges) {
      privileges.add(fromProto(pPrivilege));
    }
    return privileges;
  }

  /**
   * @param pPrivilege a protocol buffer type privilege
   * @return the corresponding {@link Privilege}
   */
  public static Privilege fromProto(PPrivilege pPrivilege) {
    switch (pPrivilege) {
      case FREE:
        return Privilege.FREE;
      case PIN:
        return Privilege.PIN;
      case TTL:
        return Privilege.TTL;
      case REPLICATION:
        return Privilege.REPLICATION;
      default:
        throw new IllegalArgumentException("Unrecognized pPrivilege: " + pPrivilege);
    }
  }

  /**
   * @param privileges a list of {@link Privilege}
   * @return a corresponding list of proto type privileges
   */
  public static List<PPrivilege> toProto(Collection<Privilege> privileges) {
    List<PPrivilege> pPrivileges = new ArrayList<>();
    for (Privilege privilege : privileges) {
      pPrivileges.add(toProto(privilege));
    }
    return pPrivileges;
  }

  /**
   * @param privilege an {@link Privilege}
   * @return the corresponding protocol buffer type privilege
   */
  public static PPrivilege toProto(Privilege privilege) {
    switch (privilege) {
      case FREE:
        return PPrivilege.FREE;
      case PIN:
        return PPrivilege.PIN;
      case TTL:
        return PPrivilege.TTL;
      case REPLICATION:
        return PPrivilege.REPLICATION;
      default:
        throw new IllegalArgumentException("Unrecognized privilege: " + privilege);
    }
  }

  private ClosedSourceGrpcUtils() {} // not intended for instantiation
}
