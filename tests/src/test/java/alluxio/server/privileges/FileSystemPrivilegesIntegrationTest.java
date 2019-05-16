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

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.privilege.PrivilegeMasterClient;
import alluxio.client.privilege.options.GrantPrivilegesOptions;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.MasterClientContext;
import alluxio.security.authorization.Mode;
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
import java.util.List;

/**
 * Integration tests to verify that privileges are enforced by the file system.
 *
 * Groups are set up so that TEST_USER is in only TEST_GROUP, while SUPER_USER is in only the
 * supergroup.
 */
public final class FileSystemPrivilegesIntegrationTest extends BaseIntegrationTest {
  private static final String TEST_USER = "testuser";
  private static final String SUPER_USER = "superuser";
  private static final String TEST_GROUP = "testgroup";
  private static final AlluxioURI TEST_FILE = new AlluxioURI("/file");

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.SECURITY_PRIVILEGES_ENABLED, true)
          .setProperty(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
              FileSystemPrivilegesIntegrationTest.TestGroupsMapping.class.getName())
          .build();

  @Before
  public void before() throws Exception {
    FileSystem client = getFileSystemClient(SUPER_USER);
    FileSystemTestUtils.createByteFile(client, TEST_FILE,
        CreateFilePOptions.newBuilder().setMode(Mode.createFullAccess().toProto())
            .setWriteType(WritePType.CACHE_THROUGH).build(), 10);
  }

  @Test
  public void freeWithPrivilege() throws Exception {
    grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.FREE));
    FileSystem client = getFileSystemClient(TEST_USER);
    client.free(TEST_FILE);
  }

  @Test
  public void freeWithoutPrivilege() throws Exception {
    FileSystem client = getFileSystemClient(TEST_USER);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.FREE));
    client.free(TEST_FILE);
  }

  @Test
  public void pinWithPrivilege() throws Exception {
    grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.PIN));
    FileSystem client = getFileSystemClient(TEST_USER);
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder().setPinned(true).build());
  }

  @Test
  public void pinWithoutPrivilege() throws Exception {
    FileSystem client = getFileSystemClient(TEST_USER);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.PIN));
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder().setPinned(true).build());
  }

  @Test
  public void unpinWithPrivilege() throws Exception {
    grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.PIN));

    // Become superuser to pin.
    FileSystem client = getFileSystemClient(SUPER_USER);
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder().setPinned(true).build());

    client = getFileSystemClient(TEST_USER);
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder().setPinned(false).build());
  }

  @Test
  public void unpinWithoutPrivilege() throws Exception {
    // Become superuser to pin.
    FileSystem client = getFileSystemClient(SUPER_USER);
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder().setPinned(true).build());

    client = getFileSystemClient(TEST_USER);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.PIN));
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder().setPinned(false).build());
  }

  @Test
  public void setReplicationWithPrivilege() throws Exception {
    grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.REPLICATION));
    FileSystem client = getFileSystemClient(TEST_USER);
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder().setReplicationMin(1).build());
  }

  @Test
  public void setReplicationWithoutPrivilege() throws Exception {
    FileSystem client = getFileSystemClient(TEST_USER);
    mThrown.expectMessage(
        ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.REPLICATION));
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder().setReplicationMin(1).build());
  }

  @Test
  public void setTtlWithPrivilege() throws Exception {
    grantPrivileges(TEST_GROUP, Arrays.asList(Privilege.TTL));
    FileSystem client = getFileSystemClient(TEST_USER);
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(1)).build());
  }

  @Test
  public void setTtlWithoutPrivilege() throws Exception {
    FileSystem client = getFileSystemClient(TEST_USER);
    mThrown.expectMessage(ExceptionMessage.PRIVILEGE_DENIED.getMessage(TEST_USER, Privilege.TTL));
    client.setAttribute(TEST_FILE, SetAttributePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(1)).build());
  }

  private void grantPrivileges(String group, List<Privilege> privileges) throws Exception {
    UserState s = new TestUserState(SUPER_USER, ServerConfiguration.global());
    PrivilegeMasterClient client = PrivilegeMasterClient.Factory.create(MasterClientContext
        .newBuilder(ClientContext.create(s.getSubject(), ServerConfiguration.global())).build());
    client.grantPrivileges(group, privileges, GrantPrivilegesOptions.defaults());
  }

  private FileSystem getFileSystemClient(String user) throws Exception {
    UserState s = new TestUserState(user, ServerConfiguration.global());
    return mLocalAlluxioClusterResource.get()
        .getClient(FileSystemContext.create(s.getSubject(), ServerConfiguration.global()));
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
