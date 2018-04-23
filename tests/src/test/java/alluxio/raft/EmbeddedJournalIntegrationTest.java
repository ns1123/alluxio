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

package alluxio.raft;

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.MultiProcessCluster.DeployMode;
import alluxio.testutils.BaseIntegrationTest;

import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests for the embedded journal.
 */
public final class EmbeddedJournalIntegrationTest extends BaseIntegrationTest {
  private static final int NUM_MASTERS = 3;

  @Rule
  public MultiProcessCluster mCluster = MultiProcessCluster.newBuilder()
      .setClusterName("EmbeddedJournalIntegrationTest")
      .setNumMasters(NUM_MASTERS)
      .setNumWorkers(0)
      .setDeployMode(DeployMode.EMBEDDED_HA)
      .build();

  @Test
  public void failover() throws Exception {
    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    mCluster.waitForAndKillPrimaryMaster(30 * Constants.SECOND_MS);
    assertTrue(fs.exists(testDir));
    mCluster.notifySuccess();
  }

  @Test
  public void restart() throws Exception {
    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    restartMasters();
    assertTrue(fs.exists(testDir));
    restartMasters();
    assertTrue(fs.exists(testDir));
    restartMasters();
    assertTrue(fs.exists(testDir));
    mCluster.saveWorkdir();
    mCluster.notifySuccess();
  }

  private void restartMasters() throws Exception {
    for (int i = 0; i < NUM_MASTERS; i++) {
      mCluster.stopMaster(i);
    }
    for (int i = 0; i < NUM_MASTERS; i++) {
      mCluster.startMaster(i);
    }
  }
}
