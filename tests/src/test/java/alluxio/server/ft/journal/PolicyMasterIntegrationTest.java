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

package alluxio.server.ft.journal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedClientUserResource;
import alluxio.AuthenticatedUserRule;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.AddPolicyPOptions;
import alluxio.grpc.Bits;
import alluxio.grpc.ListPolicyPOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.PolicyInfo;
import alluxio.grpc.RemovePolicyPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.policy.PolicyMaster;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

/**
 * Integration tests for policy master functionality.
 */
public class PolicyMasterIntegrationTest {
  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("test-policy",
      ServerConfiguration.global());
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .build();
  private LocalAlluxioCluster mCluster;

  @Before
  public void before() {
    mCluster = mClusterResource.get();
  }

  @Test
  public void addPolicy() throws Exception {
    FileSystem fs = mCluster.getClient();
    PolicyMaster policyMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(PolicyMaster.class);
    AlluxioURI file = new AlluxioURI("/test");
    FileSystemTestUtils.createByteFile(fs, file, WritePType.MUST_CACHE, 10);
    fs.setAttribute(file, SetAttributePOptions.newBuilder().setMode(PMode.newBuilder()
        .setOwnerBits(Bits.ALL)
        .setGroupBits(Bits.READ_WRITE)
        .setOtherBits(Bits.READ_WRITE)).build());
    policyMaster.addPolicy(file.getPath(),
        "ufsMigrate(1d, UFS[A]:STORE, UFS[B]:REMOVE)",
        AddPolicyPOptions.getDefaultInstance());
    assertNotNull(policyMaster.getPolicy("ufsMigrate-" + file.getPath()));
  }

  @Test
  public void addPolicyWithoutPermission() throws Exception {
    mThrown.expect(AccessControlException.class);
    FileSystem fs = mCluster.getClient();
    PolicyMaster policyMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(PolicyMaster.class);
    AlluxioURI file = new AlluxioURI("/test");
    FileSystemTestUtils.createByteFile(fs, file, WritePType.MUST_CACHE, 10);
    fs.setAttribute(file, SetAttributePOptions.newBuilder().setMode(PMode.newBuilder()
        .setOwnerBits(Bits.ALL)
        .setGroupBits(Bits.READ)
        .setOtherBits(Bits.READ)).build());
    policyMaster.addPolicy(file.getPath(),
        "ufsMigrate(1d, UFS[A]:STORE, UFS[B]:REMOVE)",
        AddPolicyPOptions.getDefaultInstance());
  }

  @Test
  public void addPolicyPathNotExist() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    FileSystem fs = mCluster.getClient();
    PolicyMaster policyMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(PolicyMaster.class);
    AlluxioURI file = new AlluxioURI("/test");
    policyMaster.addPolicy(file.getPath(),
        "ufsMigrate(1d, UFS[A]:STORE, UFS[B]:REMOVE)",
        AddPolicyPOptions.getDefaultInstance());
  }

  @Test
  public void removePolicy() throws Exception {
    FileSystem fs = mCluster.getClient();
    URIStatus status = fs.getStatus(new AlluxioURI("/"));
    status.getOwner();

    try (AuthenticatedClientUserResource r = new AuthenticatedClientUserResource(status.getOwner(),
        ServerConfiguration.global())) {
      PolicyMaster policyMaster =
          mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(PolicyMaster.class);
      policyMaster.addPolicy("/", "ufsMigrate(1d, UFS[A]:STORE, UFS[B]:REMOVE)",
          AddPolicyPOptions.getDefaultInstance());
      List<PolicyInfo> policies = policyMaster.listPolicy(ListPolicyPOptions.getDefaultInstance());
      assertEquals(1, policies.size());
      policyMaster
          .removePolicy(policies.get(0).getName(), RemovePolicyPOptions.getDefaultInstance());
      policies = policyMaster.listPolicy(ListPolicyPOptions.getDefaultInstance());
      assertTrue(policies.isEmpty());
    }
  }

  @Test
  public void removePolicyWithoutPermission() throws Exception {
    FileSystem fs = mCluster.getClient();
    AlluxioURI file = new AlluxioURI("/test");
    FileSystemTestUtils.createByteFile(fs, file, WritePType.MUST_CACHE, 10);

    PolicyMaster policyMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(PolicyMaster.class);
    URIStatus status = fs.getStatus(file);

    try (AuthenticatedClientUserResource r = new AuthenticatedClientUserResource(status.getOwner(),
        ServerConfiguration.global())) {
      policyMaster.addPolicy(file.getPath(), "ufsMigrate(1d, UFS[A]:STORE, UFS[B]:REMOVE)",
          AddPolicyPOptions.getDefaultInstance());
    }
    List<PolicyInfo> policies = policyMaster.listPolicy(ListPolicyPOptions.getDefaultInstance());
    assertEquals(1, policies.size());
    fs.setAttribute(file, SetAttributePOptions.newBuilder().setMode(PMode.newBuilder()
        .setOwnerBits(Bits.ALL)
        .setGroupBits(Bits.READ)
        .setOtherBits(Bits.READ)).build());
    mThrown.expect(AccessControlException.class);
    policyMaster
        .removePolicy(policies.get(0).getName(), RemovePolicyPOptions.getDefaultInstance());
  }

  @Test
  public void removePolicyPathNotExist() throws Exception {
    FileSystem fs = mCluster.getClient();
    AlluxioURI file = new AlluxioURI("/test");
    FileSystemTestUtils.createByteFile(fs, file, WritePType.MUST_CACHE, 10);

    PolicyMaster policyMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(PolicyMaster.class);
    URIStatus status = fs.getStatus(file);
    status.getOwner();

    try (AuthenticatedClientUserResource r = new AuthenticatedClientUserResource(status.getOwner(),
        ServerConfiguration.global())) {
      policyMaster.addPolicy(file.getPath(), "ufsMigrate(1d, UFS[A]:STORE, UFS[B]:REMOVE)",
          AddPolicyPOptions.getDefaultInstance());
    }
    List<PolicyInfo> policies = policyMaster.listPolicy(ListPolicyPOptions.getDefaultInstance());
    assertEquals(1, policies.size());
    fs.delete(file);
    policyMaster
        .removePolicy(policies.get(0).getName(), RemovePolicyPOptions.getDefaultInstance());
    try (AuthenticatedClientUserResource r = new AuthenticatedClientUserResource(status.getOwner(),
        ServerConfiguration.global())) {
      policies = policyMaster.listPolicy(ListPolicyPOptions.getDefaultInstance());
      assertTrue(policies.isEmpty());
    }
  }
}
