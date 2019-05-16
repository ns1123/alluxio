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

package alluxio.server.privileges;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.ClientContext;
import alluxio.client.privilege.PrivilegeMasterClient;
import alluxio.client.privilege.options.GetGroupPrivilegesOptions;
import alluxio.client.privilege.options.GetGroupToPrivilegesMappingOptions;
import alluxio.client.privilege.options.GetUserPrivilegesOptions;
import alluxio.client.privilege.options.GrantPrivilegesOptions;
import alluxio.client.privilege.options.RevokePrivilegesOptions;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterClientContext;
import alluxio.security.group.GroupMappingService;
import alluxio.security.user.TestUserState;
import alluxio.security.user.UserState;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.wire.Privilege;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for the privileges service.
 *
 * In each test, TEST_GROUP begins with FREE and TTL privileges.
 *
 * Groups are set up so that TEST_USER is in only TEST_GROUP, while SUPER_USER is in only the
 * supergroup.
 */
public final class PrivilegesServiceIntegrationTest extends BaseIntegrationTest {
  private static final String TEST_USER = "testuser";
  private static final String SUPER_USER = "superuser";
  private static final String TEST_GROUP = "testgroup";

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
      .setProperty(PropertyKey.SECURITY_PRIVILEGES_ENABLED, true)
      .setProperty(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
          PrivilegesServiceIntegrationTest.TestGroupsMapping.class.getName())
      .build();

  private String mSupergroup;

  @Before
  public void before() throws Exception {
    mSupergroup = ServerConfiguration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
    PrivilegeMasterClient client = getPrivilegeClient(SUPER_USER);
    client.grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.FREE, Privilege.TTL),
        GrantPrivilegesOptions.defaults());
  }

  @Test
  public void superUserHasAllPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(SUPER_USER);
    List<Privilege> superUserPrivileges =
        client.getUserPrivileges(SUPER_USER, GetUserPrivilegesOptions.defaults());
    assertEquals(new HashSet<>(Arrays.asList(Privilege.values())),
        new HashSet<>(superUserPrivileges));
  }

  @Test
  public void superGroupHasAllPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(SUPER_USER);
    List<Privilege> superUserPrivileges =
        client.getGroupPrivileges(mSupergroup, GetGroupPrivilegesOptions.defaults());
    assertEquals(new HashSet<>(Arrays.asList(Privilege.values())),
        new HashSet<>(superUserPrivileges));
  }

  @Test
  public void superUserCanGetAllPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(SUPER_USER);
    Map<String, List<Privilege>> allPrivileges =
        client.getGroupToPrivilegesMapping(GetGroupToPrivilegesMappingOptions.defaults());
    assertEquals(1, allPrivileges.keySet().size());
    List<Privilege> testGroupPrivileges = allPrivileges.get(TEST_GROUP);
    assertEquals(2, testGroupPrivileges.size());
  }

  @Test
  public void testUserCannotGetAllPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(TEST_USER);
    mThrown.expectMessage("Only members of the supergroup 'supergroup' can list all privileges");
    client.getGroupToPrivilegesMapping(GetGroupToPrivilegesMappingOptions.defaults());
  }

  @Test
  public void superUserCanGetAnyGroupPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(SUPER_USER);
    List<Privilege> testGroupPrivileges =
        client.getGroupPrivileges(TEST_GROUP, GetGroupPrivilegesOptions.defaults());
    assertEquals(2, testGroupPrivileges.size());
  }

  @Test
  public void testUserCannotGetSupergroupPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(TEST_USER);
    mThrown.expectMessage(
        "Only members of group 'supergroup' and members of the supergroup 'supergroup' can list"
            + " privileges for group 'supergroup'");
    client.getGroupPrivileges(mSupergroup, GetGroupPrivilegesOptions.defaults());
  }

  @Test
  public void testUserCanGetOwnPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(TEST_USER);
    List<Privilege> testUserPrivileges =
        client.getUserPrivileges(TEST_USER, GetUserPrivilegesOptions.defaults());
    assertEquals(2, testUserPrivileges.size());
  }

  @Test
  public void testUserCannotGetSuperuserPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(TEST_USER);
    mThrown.expectMessage(
        "Only user 'superuser' and members of the supergroup 'supergroup' can list privileges for"
            + " user 'superuser'");
    client.getUserPrivileges(SUPER_USER, GetUserPrivilegesOptions.defaults());
  }

  @Test
  public void testUserCannotGrantPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(TEST_USER);
    mThrown.expectMessage("Only members of the supergroup 'supergroup' can grant privileges");
    client.grantPrivileges(TEST_USER, Arrays.asList(Privilege.PIN),
        GrantPrivilegesOptions.defaults());
  }

  @Test
  public void testUserCannotRevokePrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(TEST_USER);
    mThrown.expectMessage("Only members of the supergroup 'supergroup' can revoke privileges");
    client.revokePrivileges(TEST_USER, Arrays.asList(Privilege.FREE),
        RevokePrivilegesOptions.defaults());
  }

  @Test
  public void superUserCanGrantPrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(SUPER_USER);
    List<Privilege> newPrivileges = client.grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.PIN),
        GrantPrivilegesOptions.defaults());
    assertEquals(3, newPrivileges.size());
    assertTrue(newPrivileges.contains(Privilege.PIN));
  }

  @Test
  public void superUserCanRevokePrivileges() throws Exception {
    PrivilegeMasterClient client = getPrivilegeClient(SUPER_USER);
    List<Privilege> newPrivileges = client
        .revokePrivileges(TEST_GROUP, Arrays.asList(Privilege.FREE),
            RevokePrivilegesOptions.defaults());
    assertEquals(1, newPrivileges.size());
    assertFalse(newPrivileges.contains(Privilege.FREE));
  }

  private PrivilegeMasterClient getPrivilegeClient(String user) throws Exception {
    UserState s = new TestUserState(user, ServerConfiguration.global());
    return PrivilegeMasterClient.Factory.create(MasterClientContext
        .newBuilder(ClientContext.create(s.getSubject(), ServerConfiguration.global())).build());
  }

  /**
   * The group mapping used by this test.
   */
  public static class TestGroupsMapping implements GroupMappingService {
    /**
     * Constructs the groups mapping used by this test.
     */
    public TestGroupsMapping() {}

    @Override
    public List<String> getGroups(String user) throws IOException {
      List<String> groups = new ArrayList<>();
      switch (user) {
        case TEST_USER:
          groups.add(TEST_GROUP);
          break;
        case SUPER_USER:
          groups.add(ServerConfiguration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP));
          break;
        default:
          // don't add any groups.
      }
      return groups;
    }
  }
}
