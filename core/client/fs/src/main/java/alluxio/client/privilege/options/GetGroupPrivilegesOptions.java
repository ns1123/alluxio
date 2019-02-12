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

package alluxio.client.privilege.options;

import alluxio.grpc.GetGroupPrivilegesPOptions;

import com.google.common.base.MoreObjects;

/**
 * Options for querying the privileges of a group.
 */
public final class GetGroupPrivilegesOptions {
  /**
   * @return the default {@link GetGroupPrivilegesOptions}
   */
  public static GetGroupPrivilegesOptions defaults() {
    return new GetGroupPrivilegesOptions();
  }

  private GetGroupPrivilegesOptions() {
    // No options currently
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof GetGroupPrivilegesOptions;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  /**
   * @return gRPC representation of the options
   */
  public GetGroupPrivilegesPOptions toProto() {
    return GetGroupPrivilegesPOptions.getDefaultInstance();
  }
}
