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

package alluxio.cli.privileges;

import alluxio.client.privilege.PrivilegeMasterClient;
import alluxio.client.privilege.options.RevokePrivilegesOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.Privilege;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Command for revoking privileges from a group.
 */
@Parameters(commandDescription = "Revoke privileges from a group")
public final class RevokePrivilegesCommand implements Callable<String> {
  @Parameter(names = "-group", description = "the group to revoke privileges from", required = true)
  private String mGroup;

  @Parameter(names = "-privileges", description = "the privileges to revoke", variableArity = true,
      required = true)
  private List<String> mPrivileges;

  /**
   * No-arg constructor for use with JCommander.
   */
  public RevokePrivilegesCommand() {}

  /**
   * Runs the revoke privileges command.
   *
   * @return the command output
   * @throws Exception if the command fails
   */
  public String call() throws Exception {
    List<Privilege> privileges = new ArrayList<>();
    for (String p : mPrivileges) {
      String pUppercase = p.toUpperCase();
      if (pUppercase.equals(Privilege.ALL)) {
        Preconditions.checkArgument(mPrivileges.size() == 1,
            "The ALL privilege should be specified on its own");
        privileges.addAll(Arrays.asList(Privilege.values()));
      } else if (pUppercase.equals(Privilege.NONE)) {
        throw new IllegalArgumentException("Cannot revoke NONE privilege");
      } else {
        try {
          privileges.add(Privilege.valueOf(pUppercase));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Unrecognized privilege: " + p);
        }
      }
    }

    PrivilegeMasterClient client = PrivilegeMasterClient.Factory.create(null,
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));
    List<Privilege> newPrivileges =
        client.revokePrivileges(mGroup, privileges, RevokePrivilegesOptions.defaults());
    return ListPrivilegesCommand.formatPrivileges(mGroup, newPrivileges);
  }
}
