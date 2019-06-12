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

package alluxio.server.ft;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.AddPolicyPOptions;
import alluxio.grpc.WritePType;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.master.policy.PolicyMaster;
import alluxio.testutils.BaseIntegrationTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.HashSet;
import java.util.Set;

public class PolicyMasterFaultToleranceIntegrationTest extends BaseIntegrationTest {
  // Fail if the cluster doesn't come up after this amount of time.
  private static final int CLUSTER_WAIT_TIMEOUT_MS = 120 * Constants.SECOND_MS;
  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final int BLOCK_SIZE = 30;
  private static final int MASTERS = 3;

  private MultiMasterLocalAlluxioCluster mMultiMasterLocalAlluxioCluster = null;

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mMultiMasterLocalAlluxioCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    mMultiMasterLocalAlluxioCluster =
        new MultiMasterLocalAlluxioCluster(MASTERS);
    mMultiMasterLocalAlluxioCluster.initConfiguration();
    ServerConfiguration.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "60sec");
    ServerConfiguration.set(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "1sec");
    ServerConfiguration.set(PropertyKey.USER_FILE_BUFFER_BYTES, BLOCK_SIZE);
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE);
    ServerConfiguration.set(PropertyKey.MASTER_GRPC_SERVER_SHUTDOWN_TIMEOUT, "30sec");
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 100);
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, 32);
    ServerConfiguration.set(PropertyKey.WORKER_MEMORY_SIZE, WORKER_CAPACITY_BYTES);
    mMultiMasterLocalAlluxioCluster.start();
  }

  @Test
  public void addPolicyFault() throws Exception {
    boolean isConnected = true;
    try {
      alluxio.client.file.FileSystem fs = mMultiMasterLocalAlluxioCluster.getClient();
      PolicyMaster policyMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
          .getMasterProcess().getMaster(PolicyMaster.class);
      AlluxioURI file = new AlluxioURI("/test");
      FileSystemTestUtils.createByteFile(fs, file, WritePType.MUST_CACHE, 10);
      policyMaster.addPolicy(file.getPath(),
          "ufsMigrate(20m, UFS[A]:STORE, UFS[B]:REMOVE)",
          AddPolicyPOptions.getDefaultInstance());
      Set<PolicyMaster> deadMasters = new HashSet<>();
      // kills leaders one by one and verify policy can be listed
      for (int kills = 0; kills < MASTERS - 1; kills++) {
        assertTrue(mMultiMasterLocalAlluxioCluster.stopLeader());
        deadMasters.add(policyMaster);
        mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);

        // gets a new client
        mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster().clearClients();
        policyMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
            .getMasterProcess().getMaster(PolicyMaster.class);
        assertFalse(deadMasters.contains(policyMaster));
        assertNotNull(policyMaster.getPolicy("ufsMigrate-" + file.getPath()));
      }
    } catch (UnauthenticatedException | UnavailableException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);
  }
}
