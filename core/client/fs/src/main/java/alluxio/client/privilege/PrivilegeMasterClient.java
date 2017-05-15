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

package alluxio.client.privilege;

import alluxio.Client;
import alluxio.client.privilege.options.GetGroupPrivilegesOptions;
import alluxio.client.privilege.options.GetGroupToPrivilegesMappingOptions;
import alluxio.client.privilege.options.GetUserPrivilegesOptions;
import alluxio.client.privilege.options.GrantPrivilegesOptions;
import alluxio.client.privilege.options.RevokePrivilegesOptions;
import alluxio.wire.Privilege;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A client to use for interacting with a privilege master.
 */
@ThreadSafe
public interface PrivilegeMasterClient extends Client {

  /**
   * Factory for {@link PrivilegeMasterClient}.
   */
  class Factory {
    private Factory() {} // prevent instantiation

    /**
     * Factory method for {@link PrivilegeMasterClient}.
     *
     * @param subject the parent subject
     * @param masterAddress the master address
     * @return a new {@link PrivilegeMasterClient} instance
     */
    public static PrivilegeMasterClient create(Subject subject, InetSocketAddress masterAddress) {
      return RetryHandlingPrivilegeMasterClient.create(subject, masterAddress);
    }
  }

  /**
   * @param group the name of a group
   * @param options get group privileges options
   * @return the privilege information for the given group
   */
  List<Privilege> getGroupPrivileges(String group, GetGroupPrivilegesOptions options)
      throws IOException;

  /**
   * @param user the name of a user
   * @param options get user privileges options
   * @return the privilege information for the given user
   */
  List<Privilege> getUserPrivileges(String user, GetUserPrivilegesOptions options)
      throws IOException;

  /**
   * @param options get all group privileges options
   * @return the privilege information for all groups
   */
  Map<String, List<Privilege>> getGroupToPrivilegesMapping(
      GetGroupToPrivilegesMappingOptions options) throws IOException;

  /**
   * Grants the given privileges to the given group.
   *
   * @param group the name of the group
   * @param privileges the privileges to grant
   * @param options grant privileges options
   * @return the updated privileges for the group
   */
  List<Privilege> grantPrivileges(String group, List<Privilege> privileges,
      GrantPrivilegesOptions options) throws IOException;

  /**
   * Revokes the given privileges from the given group.
   *
   * @param group the name of the group
   * @param privileges the privileges to revoke
   * @param options revoke privileges options
   * @return the updated privileges for the group
   */
  List<Privilege> revokePrivileges(String group, List<Privilege> privileges,
      RevokePrivilegesOptions options) throws IOException;
}
