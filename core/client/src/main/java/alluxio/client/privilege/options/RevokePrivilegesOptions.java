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

import alluxio.thrift.RevokePrivilegesTOptions;

import com.google.common.base.Objects;

/**
 * Options for revoking privileges from a group.
 */
public final class RevokePrivilegesOptions {
  /**
   * @return the default {@link RevokePrivilegesOptions}
   */
  public static RevokePrivilegesOptions defaults() {
    return new RevokePrivilegesOptions();
  }

  private RevokePrivilegesOptions() {
    // No options currently
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof RevokePrivilegesOptions;
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
  public RevokePrivilegesTOptions toThrift() {
    return new RevokePrivilegesTOptions();
  }
}
