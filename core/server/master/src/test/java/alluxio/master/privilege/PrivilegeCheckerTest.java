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
import alluxio.exception.status.PermissionDeniedException;
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
import java.util.Collections;
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
      PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
      PrivilegeCheckerTest.TestGroupsMapping.class.getName()));

  @Test
  public void checkPass() throws Exception {
    Set<Privilege> privileges = new HashSet<>(Collections.singletonList(Privilege.FREE));
    SimplePrivilegeMaster privilegeService =
        new SimplePrivilegeMaster(ImmutableMap.of(TEST_GROUP, privileges));
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    checker.check(Privilege.FREE);
  }

  @Test
  public void checkMissingPermission() throws Exception {
    Set<Privilege> privileges = new HashSet<>(Collections.singletonList(Privilege.FREE));
    SimplePrivilegeMaster privilegeService =
        new SimplePrivilegeMaster(ImmutableMap.of(TEST_GROUP, privileges));
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    mThrown.expect(PermissionDeniedException.class);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.TTL));
    checker.check(Privilege.TTL);
  }

  @Test
  public void checkGroupWithNoPermissions() throws Exception {
    Set<Privilege> privileges = new HashSet<>();
    SimplePrivilegeMaster privilegeService =
        new SimplePrivilegeMaster(ImmutableMap.of(TEST_GROUP, privileges));
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    mThrown.expect(PermissionDeniedException.class);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.TTL));
    checker.check(Privilege.TTL);
  }

  @Test
  public void checkUserWithNoGroups() throws Exception {
    Set<Privilege> privileges = new HashSet<>(Collections.singletonList(Privilege.FREE));
    SimplePrivilegeMaster privilegeService =
        new SimplePrivilegeMaster(ImmutableMap.of(TEST_GROUP, privileges));
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    mThrown.expect(PermissionDeniedException.class);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage("otheruser", Privilege.TTL));
    checker.check("otheruser", Privilege.TTL);
  }

  @Test
  public void checkPermissionsDisabled() throws Exception {
    SimplePrivilegeMaster privilegeService =
        new SimplePrivilegeMaster(ImmutableMap.<String, Set<Privilege>>of());
    PrivilegeChecker checker = new PrivilegeChecker(privilegeService);
    try (Closeable c =
        new ConfigurationRule(PropertyKey.SECURITY_PRIVILEGES_ENABLED, "false").toResource()) {
      checker.check("otheruser", Privilege.TTL);
    }
  }

  @Test
  public void checkAuthDisabled() throws Exception {
    SimplePrivilegeMaster privilegeService =
        new SimplePrivilegeMaster(ImmutableMap.<String, Set<Privilege>>of());
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