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
import alluxio.grpc.AddPolicyPOptions;
import alluxio.grpc.ListPolicyPOptions;
import alluxio.grpc.PolicyInfo;
import alluxio.grpc.RemovePolicyPOptions;
import alluxio.grpc.WritePType;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.policy.PolicyMaster;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

/**
 * Integration tests for policy master functionality.
 */
public class PolicyMasterJournalIntegrationTest {
  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("asdf",
      ServerConfiguration.global());

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
  public void journalPolicyCreation() throws Exception {
    FileSystem fs = mCluster.getClient();
    PolicyMaster policyMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(PolicyMaster.class);
    AlluxioURI file = new AlluxioURI("/test");
    FileSystemTestUtils.createByteFile(fs, file, WritePType.MUST_CACHE, 10);
    policyMaster.addPolicy(file.getPath(),
        "ufsMigrate(1d, UFS[A]:STORE, UFS[B]:REMOVE)",
        AddPolicyPOptions.getDefaultInstance());
    mCluster.stopMasters();
    mCluster.startMasters();
    AlluxioMasterProcess masterProcess = mCluster.getLocalAlluxioMaster().getMasterProcess();
    policyMaster = masterProcess.getMaster(PolicyMaster.class);
    assertNotNull(policyMaster.getPolicy("ufsMigrate-" + file.getPath()));
  }

  @Test
  public void journalPolicyRemove() throws Exception {
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
      mCluster.stopMasters();
      mCluster.startMasters();
      AlluxioMasterProcess masterProcess = mCluster.getLocalAlluxioMaster().getMasterProcess();
      policyMaster = masterProcess.getMaster(PolicyMaster.class);
      policies = policyMaster.listPolicy(ListPolicyPOptions.getDefaultInstance());
      assertTrue(policies.isEmpty());
    }
  }
}
