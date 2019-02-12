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

import alluxio.grpc.GetGroupToPrivilegesMappingPOptions;

import com.google.common.base.MoreObjects;

/**
 * Options for querying the privileges of all groups.
 */
public final class GetGroupToPrivilegesMappingOptions {
  /**
   * @return the default {@link GetGroupToPrivilegesMappingOptions}
   */
  public static GetGroupToPrivilegesMappingOptions defaults() {
    return new GetGroupToPrivilegesMappingOptions();
  }

  private GetGroupToPrivilegesMappingOptions() {
    // No options currently
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof GetGroupToPrivilegesMappingOptions;
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
  public GetGroupToPrivilegesMappingPOptions toProto() {
    return GetGroupToPrivilegesMappingPOptions.getDefaultInstance();
  }
}
