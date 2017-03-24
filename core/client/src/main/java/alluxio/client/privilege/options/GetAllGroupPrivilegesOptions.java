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

import alluxio.thrift.GetAllGroupPrivilegesTOptions;

import com.google.common.base.Objects;

/**
 * Options for querying the privileges of all groups.
 */
public final class GetAllGroupPrivilegesOptions {
  /**
   * @return the default {@link GetAllGroupPrivilegesOptions}
   */
  public static GetAllGroupPrivilegesOptions defaults() {
    return new GetAllGroupPrivilegesOptions();
  }

  private GetAllGroupPrivilegesOptions() {
    // No options currently
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof GetAllGroupPrivilegesOptions;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public GetAllGroupPrivilegesTOptions toThrift() {
    return new GetAllGroupPrivilegesTOptions();
  }
}
