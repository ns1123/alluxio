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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.privilege.PrivilegeMasterClient;
import alluxio.client.privilege.options.GrantPrivilegesOptions;
import alluxio.security.group.GroupMappingService;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.Privilege;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for privileges command integration tests. It runs a test cluster and provides
 * convenient methods for setting up privileges and getting command output.
 *
 * Tests are run under a user-group mapping where all users are in the supergroup, except one user
 * named "Bob" who is only in the group "nonsuper".
 */
public class PrivilegesTest {
  protected static final String NONSUPER = "nonsuper";
  protected static final String SUPERGROUP = "supergroup";

  protected static final Privilege FREE = Privilege.FREE;
  protected static final Privilege PIN = Privilege.PIN;
  protected static final Privilege REPLICATION = Privilege.REPLICATION;
  protected static final Privilege TTL = Privilege.TTL;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.SECURITY_PRIVILEGES_ENABLED, true)
          .setProperty(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
              PrivilegeTestUserGroupsMapping.class.getName())
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, SUPERGROUP)
          .build();

  /**
   * Runs the privilege command with the given arguments and returns the output printed to the
   * system out stream.
   *
   * @return the output
   */
  protected String run(String... args) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    System.setOut(new PrintStream(output));
    try {
      new Privileges().run(args);
    } finally {
      System.setOut(System.out);
    }
    return output.toString();
  }

  /**
   * Grants privileges to a group.
   *
   * @param group the group
   * @param privileges the privileges
   */
  protected void grantPrivileges(String group, Privilege... privileges) throws Exception {
    PrivilegeMasterClient client = PrivilegeMasterClient.Factory.create(null,
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));
    client.grantPrivileges(group, Arrays.asList(privileges), GrantPrivilegesOptions.defaults());
  }

  /** Everyone is in the supergroup except Bob. */
  public static class PrivilegeTestUserGroupsMapping implements GroupMappingService {
    public PrivilegeTestUserGroupsMapping() {}

    @Override
    public List<String> getGroups(String user) throws IOException {
      if (user.equals("Bob")) {
        return Arrays.asList(NONSUPER);
      }
      return Arrays.asList(SUPERGROUP);
    }
  }

  @Test
  public void unknownCommand() {
    assertThat(run("blah"), containsString("Expected a command, got blah"));
  }
}
