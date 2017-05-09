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

package alluxio.master.privileges;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.privilege.PrivilegeMasterClient;
import alluxio.client.privilege.options.GetGroupPrivilegesOptions;
import alluxio.client.privilege.options.GetGroupToPrivilegesMappingOptions;
import alluxio.client.privilege.options.GetUserPrivilegesOptions;
import alluxio.client.privilege.options.GrantPrivilegesOptions;
import alluxio.client.privilege.options.RevokePrivilegesOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.Privilege;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

/**
 * Tests that privileges RPCs will fail when privileges or authentication are disabled.
 */
public final class PrivilegesDisabledTest {
  private PrivilegeMasterClient mPrivilegeClient;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Before
  public void before() {
    mPrivilegeClient = PrivilegeMasterClient.Factory.create(null,
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_PRIVILEGES_ENABLED, "false"})
  public void privilegesDisabled() throws Exception {
    String privilegesDisabledMessage = "Privilege controls are disabled. To enable them, "
        + "set alluxio.security.privileges.enabled=true in master configuration";
    try {
      mPrivilegeClient.revokePrivileges("group", Arrays.asList(Privilege.FREE),
          RevokePrivilegesOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
    try {
      mPrivilegeClient.grantPrivileges("group", Arrays.asList(Privilege.FREE),
          GrantPrivilegesOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
    try {
      mPrivilegeClient.getGroupPrivileges("group", GetGroupPrivilegesOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
    try {
      mPrivilegeClient.getUserPrivileges("user", GetUserPrivilegesOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
    try {
      mPrivilegeClient.getGroupToPrivilegesMapping(GetGroupToPrivilegesMappingOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_PRIVILEGES_ENABLED, "true",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL",
          PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false"})
  public void authenticationDisabled() throws Exception {
    String privilegesDisabledMessage = "Privilege controls are disabled because authentication is "
        + "disabled by alluxio.security.authentication.type=NOSASL";
    try {
      mPrivilegeClient.revokePrivileges("group", Arrays.asList(Privilege.FREE),
          RevokePrivilegesOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
    try {
      mPrivilegeClient.grantPrivileges("group", Arrays.asList(Privilege.FREE),
          GrantPrivilegesOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
    try {
      mPrivilegeClient.getGroupPrivileges("group", GetGroupPrivilegesOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
    try {
      mPrivilegeClient.getUserPrivileges("user", GetUserPrivilegesOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
    try {
      mPrivilegeClient.getGroupToPrivilegesMapping(GetGroupToPrivilegesMappingOptions.defaults());
      fail("Expected an exception to be thrown.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(privilegesDisabledMessage));
    }
  }
}
