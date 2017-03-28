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
import alluxio.client.privilege.options.GetGroupPrivilegesOptions;
import alluxio.client.privilege.options.GetGroupToPrivilegesMappingOptions;
import alluxio.client.privilege.options.GetUserPrivilegesOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.Privilege;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

/**
 * Command for listing privileges.
 */
@Parameters(commandDescription = "List privileges")
public final class ListPrivilegesCommand implements Callable<String> {
  @Parameter(names = "-group",
      description = "the group to list privileges for. If user and group are omitted, privileges "
          + "for all groups will be printed")
  private String mGroup;
  @Parameter(names = "-user",
      description = "the user to list privileges for. If user and group are omitted, privileges "
          + "for all groups will be printed")
  private String mUser;

  /**
   * No-arg constructor for use with JCommander.
   */
  public ListPrivilegesCommand() {}

  /**
   * Runs the list privileges command. Groups and privileges are printed in alphabetical order.
   *
   * @return the command output
   * @throws Exception if the command fails
   */
  public String call() throws Exception {
    PrivilegeMasterClient client = PrivilegeMasterClient.Factory.create(null,
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));

    if (mGroup != null && mUser != null) {
      throw new IllegalArgumentException("Cannot specify both user and group");
    } else if (mGroup != null) {
      List<Privilege> privileges =
          client.getGroupPrivileges(mGroup, GetGroupPrivilegesOptions.defaults());
      return formatPrivileges(mGroup, privileges);
    } else if (mUser != null) {
      List<Privilege> privileges =
          client.getUserPrivileges(mUser, GetUserPrivilegesOptions.defaults());
      return formatPrivileges(mUser, privileges);
    } else {
      List<String> outputLines = new ArrayList<>();
      Map<String, List<Privilege>> perGroupPrivileges =
          client.getGroupToPrivilegesMapping(GetGroupToPrivilegesMappingOptions.defaults());
      for (Entry<String, List<Privilege>> entry : perGroupPrivileges.entrySet()) {
        outputLines.add(formatPrivileges(entry.getKey(), entry.getValue()));
      }
      Collections.sort(outputLines);
      return Joiner.on("\n").join(outputLines);
    }
  }

  /**
   * @param name the name of a user or group
   * @param privileges the privileges for the named user or group
   * @return a formatted string showing the privileges for the user or group
   */
  public static String formatPrivileges(String name, List<Privilege> privileges) {
    List<String> privilegeNames = new ArrayList<>();
    for (Privilege p : privileges) {
      privilegeNames.add(p.toString());
    }
    Collections.sort(privilegeNames);
    return String.format("%s: [%s]", name, Joiner.on(", ").join(privilegeNames));
  }
}
