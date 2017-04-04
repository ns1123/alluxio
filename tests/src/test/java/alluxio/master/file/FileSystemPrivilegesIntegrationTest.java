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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.LocalAlluxioClusterResource;
import alluxio.LoginUserRule;
import alluxio.PropertyKey;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.privilege.PrivilegeMasterClient;
import alluxio.client.privilege.options.GrantPrivilegesOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.security.authorization.Mode;
import alluxio.security.group.GroupMappingService;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.Privilege;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Integration tests to verify that privileges are enforced by the file system.
 *
 * Groups are set up so that TEST_USER is in only TEST_GROUP, while SUPER_USER is in only the
 * supergroup.
 */
public final class FileSystemPrivilegesIntegrationTest {
  private static final String TEST_USER = "testuser";
  private static final String SUPER_USER = "superuser";
  private static final String TEST_GROUP = "testgroup";
  private static final AlluxioURI TEST_FILE = new AlluxioURI("/file");

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public LoginUserRule mLoginUser = new LoginUserRule(TEST_USER);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.SECURITY_PRIVILEGES_ENABLED, true)
          .setProperty(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
              FileSystemPrivilegesIntegrationTest.TestGroupsMapping.class.getName())
          .build();

  private PrivilegeMasterClient mPrivilegeClient;
  private FileSystem mFileSystem;

  @Before
  public void before() throws Exception {
    mPrivilegeClient = PrivilegeMasterClient.Factory.create(null,
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));
    try (Closeable u = new LoginUserRule(SUPER_USER).toResource()) {
      refreshFileSystemClient();
      FileSystemTestUtils.createByteFile(mFileSystem, TEST_FILE, CreateFileOptions.defaults()
          .setMode(Mode.createFullAccess()).setWriteType(WriteType.CACHE_THROUGH), 10);
    }
    refreshFileSystemClient();
  }

  @Test
  public void freeWithPrivilege() throws Exception {
    // Become superuser to modify privileges.
    try (Closeable u = new LoginUserRule(SUPER_USER).toResource()) {
      mPrivilegeClient.grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.FREE),
          GrantPrivilegesOptions.defaults());
    }
    mFileSystem.free(TEST_FILE);
  }

  @Test
  public void freeWithoutPrivilege() throws Exception {
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.FREE));
    mFileSystem.free(TEST_FILE);
  }

  @Test
  public void pinWithPrivilege() throws Exception {
    // Become superuser to modify privileges.
    try (Closeable u = new LoginUserRule(SUPER_USER).toResource()) {
      mPrivilegeClient.grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.PIN),
          GrantPrivilegesOptions.defaults());
    }
    mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setPinned(true));
  }

  @Test
  public void pinWithoutPrivilege() throws Exception {
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.PIN));
    mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setPinned(true));
  }

  @Test
  public void unpinWithPrivilege() throws Exception {
    // Become superuser to modify privileges and pin.
    try (Closeable u = new LoginUserRule(SUPER_USER).toResource()) {
      mPrivilegeClient.grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.PIN),
          GrantPrivilegesOptions.defaults());
      refreshFileSystemClient();
      mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setPinned(true));
    }
    refreshFileSystemClient();
    mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setPinned(false));
  }

  @Test
  public void unpinWithoutPrivilege() throws Exception {
    // Become superuser to pin.
    try (Closeable u = new LoginUserRule(SUPER_USER).toResource()) {
      refreshFileSystemClient();
      mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setPinned(true));
    }
    refreshFileSystemClient();
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.PIN));
    mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setPinned(false));
  }

  @Test
  public void setReplicationWithPrivilege() throws Exception {
    // Become superuser to modify privileges.
    try (Closeable u = new LoginUserRule(SUPER_USER).toResource()) {
      mPrivilegeClient.grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.REPLICATION),
          GrantPrivilegesOptions.defaults());
    }
    mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setReplicationMin(1));
  }

  @Test
  public void setReplicationWithoutPrivilege() throws Exception {
    mThrown.expectMessage(
        ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.REPLICATION));
    mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setReplicationMin(1));
  }

  @Test
  public void setTtlWithPrivilege() throws Exception {
    // Become superuser to modify privileges.
    try (Closeable u = new LoginUserRule(SUPER_USER).toResource()) {
      mPrivilegeClient.grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.TTL),
          GrantPrivilegesOptions.defaults());
    }
    mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setTtl(1));
  }

  @Test
  public void setTtlWithoutPrivilege() throws Exception {
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.TTL));
    mFileSystem.setAttribute(TEST_FILE, SetAttributeOptions.defaults().setTtl(1));
  }

  /**
   * Updates {@link #mFileSystem} to a new filesystem client capable of acting as the current login
   * user. This is necessary whenever the login user changes and a filesystem client is needed for
   * the new user.
   */
  private void refreshFileSystemClient() throws Exception {
    // Need to reset the pool in case we have a cached client for a different login user.
    FileSystemContext.INSTANCE.reset();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
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
          groups.add(Configuration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP));
          break;
        default:
          // don't add any groups.
      }
      return groups;
    }
  }
}
