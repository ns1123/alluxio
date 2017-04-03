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

package alluxio.master.privilege;

import alluxio.AuthenticatedUserRule;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PrivilegeDeniedException;
import alluxio.security.group.GroupMappingService;
import alluxio.wire.Privilege;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Unit tests for {@link PrivilegeChecker}.
 */
public final class PrivilegeCheckerTest {
  private static final String TEST_USER = "testuser";
  private static final String TEST_GROUP = "testgroup";

  @Rule
  public AuthenticatedUserRule mAuthUser = new AuthenticatedUserRule(TEST_USER);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public ConfigurationRule mConfig = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.SECURITY_PRIVILEGES_ENABLED, "true",
      PropertyKey.SECURITY_GROUP_MAPPING_CLASS, PrivilegeCheckerTest.GroupsMapping.class.getName()));

  @Test
  public void checkPass() {
    Set<Privilege> privileges = new HashSet<>(Arrays.asList(Privilege.FREE));
    SimplePrivilegeService privilegeService =
        new SimplePrivilegeService(ImmutableMap.of(TEST_GROUP, privileges));
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    checker.check(Privilege.FREE);
  }

  @Test
  public void checkMissingPermission() {
    Set<Privilege> privileges = new HashSet<>(Arrays.asList(Privilege.FREE));
    SimplePrivilegeService privilegeService =
        new SimplePrivilegeService(ImmutableMap.of(TEST_GROUP, privileges));
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    mThrown.expect(PrivilegeDeniedException.class);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.TTL));
    checker.check(Privilege.TTL);
  }

  @Test
  public void checkGroupWithNoPermissions() {
    Set<Privilege> privileges = new HashSet<>();
    SimplePrivilegeService privilegeService =
        new SimplePrivilegeService(ImmutableMap.of(TEST_GROUP, privileges));
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    mThrown.expect(PrivilegeDeniedException.class);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.TTL));
    checker.check(Privilege.TTL);
  }

  @Test
  public void checkUserWithNoGroups() {
    Set<Privilege> privileges = new HashSet<>(Arrays.asList(Privilege.FREE));
    SimplePrivilegeService privilegeService =
        new SimplePrivilegeService(ImmutableMap.of(TEST_GROUP, privileges));
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    mThrown.expect(PrivilegeDeniedException.class);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage("otheruser", Privilege.TTL));
    checker.check("otheruser", Privilege.TTL);
  }

  @Test
  public void checkPermissionsDisabled() throws Exception {
    SimplePrivilegeService privilegeService =
        new SimplePrivilegeService(ImmutableMap.<String, Set<Privilege>>of());
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    try (Closeable c =
        new ConfigurationRule(PropertyKey.SECURITY_PRIVILEGES_ENABLED, "false").toResource()) {
      checker.check("otheruser", Privilege.TTL);
    }
  }

  @Test
  public void checkAuthDisabled() throws Exception {
    SimplePrivilegeService privilegeService =
        new SimplePrivilegeService(ImmutableMap.<String, Set<Privilege>>of());
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    try (Closeable c =
        new ConfigurationRule(PropertyKey.SECURITY_AUTHENTICATION_TYPE, "NOSASL").toResource()) {
      checker.check(Privilege.TTL);
    }
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
      if (user.equals(TEST_USER)) {
        return Arrays.asList(TEST_GROUP);
      }
      return new ArrayList<>();
    }
  }
}
