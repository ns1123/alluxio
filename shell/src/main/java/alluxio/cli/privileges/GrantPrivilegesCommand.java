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
import alluxio.client.privilege.options.GrantPrivilegesOptions;
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
 * Command for granting privileges to a group.
 */
@Parameters(commandDescription = "Grant privileges to a group")
public final class GrantPrivilegesCommand implements Callable<String> {
  @Parameter(names = "-group", description = "the group to grant privileges to", required = true)
  private String mGroup;

  @Parameter(names = "-privileges", description = "the privileges to grant", variableArity = true,
      required = true)
  private List<String> mPrivileges;

  /**
   * No-arg constructor for use with JCommander.
   */
  public GrantPrivilegesCommand() {}

  /**
   * Runs the grant privileges command.
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
        Preconditions.checkArgument(mPrivileges.size() == 1,
            "The NONE privilege should be specified on its own");
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
        client.grantPrivileges(mGroup, privileges, GrantPrivilegesOptions.defaults());
    return ListPrivilegesCommand.formatPrivileges(mGroup, newPrivileges);
  }
}
