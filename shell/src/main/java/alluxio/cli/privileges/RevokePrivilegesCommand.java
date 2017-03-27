/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.cli.privileges;

import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

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
    PrivilegeMasterClient client = PrivilegeMasterClient.Factory.create(null,
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));

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

    List<Privilege> newPrivileges =
        client.revokePrivileges(mGroup, privileges, RevokePrivilegesOptions.defaults());
    return ListPrivilegesCommand.formatPrivileges(mGroup, newPrivileges);
  }
}
