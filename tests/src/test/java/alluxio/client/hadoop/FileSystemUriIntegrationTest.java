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

package alluxio.client.hadoop;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.hadoop.FileSystem;
import alluxio.master.ZkMasterInquireClient.ZkMasterConnectDetails;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Test;

import java.net.URI;

/**
 * Integration tests for using URIs with connect details in authorities to connect to
 * Alluxio clusters through {@link FileSystem}.
 */
public class FileSystemUriIntegrationTest extends BaseIntegrationTest {
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;
<<<<<<< HEAD
  public MultiProcessCluster mCluster;
=======
  private MultiProcessCluster mCluster;
>>>>>>> OPENSOURCE/master

  @After
  public void after() throws Exception {
    mCluster.destroy();
  }

  @Test
  public void zookeeperUriTest() throws Exception {
<<<<<<< HEAD
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.MULTI_PROCESS_ZOOKEEPER)
=======
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.ZOOKEEPER_URI)
>>>>>>> OPENSOURCE/master
        .setClusterName("ZookeeperUriFileSystemIntegrationTest")
        .setNumMasters(3)
        .setNumWorkers(2)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        .build();
    mCluster.start();
    // Get the zookeeper address
    ZkMasterConnectDetails connectDetails =
        (ZkMasterConnectDetails) mCluster.getMasterInquireClient().getConnectDetails();
    String zkAddress = connectDetails.getZkAddress();

<<<<<<< HEAD
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());

    URI uri = URI.create("alluxio://zk@" + zkAddress + "/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    mCluster.waitForAllNodesRegistered(WAIT_TIMEOUT_MS);

    Path file = new Path("/testFile-Zookeeper");
    FsPermission permission = FsPermission.createImmutable((short) 0666);
    FSDataOutputStream o = fs.create(file, permission, false /* ignored */, 10 /* ignored */,
        (short) 1 /* ignored */, 512 /* ignored */, null /* ignored */);
    o.writeBytes("Test Bytes");
    o.close();
    // with mark of delete-on-exit, the close method will try to delete it
    fs.deleteOnExit(file);
    fs.close();
    mCluster.notifySuccess();
  }
  // ALLUXIO CS ADD

  @Test
  public void multiMasterUriTest() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.MULTI_PROCESS_JOURNAL)
=======
    testConnection("zk@" + zkAddress);
  }

  @Test
  public void multiMasterUriTest() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.MULTI_MASTER_URI)
>>>>>>> OPENSOURCE/master
        .setClusterName("MultiMastersUriFileSystemIntegrationTest")
        .setNumMasters(3)
        .setNumWorkers(1)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .build();
    mCluster.start();
    // Get master rpc addresses
    String address = mCluster.getMasterAddresses().stream()
        .map(a -> (a.getHostname() + ":" + a.getRpcPort()))
        .collect(java.util.stream.Collectors.joining(","));

<<<<<<< HEAD
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());

    URI uri = URI.create("alluxio://" + address + "/tmp/path.txt");
=======
    testConnection(address);
  }

  /**
   * Tests connections to Alluxio cluster using URIs with connect details in authorities.
   *
   * @param authority the authority to test
   */
  private void testConnection(String authority) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());
    URI uri = URI.create("alluxio://" + authority + "/tmp/path.txt");
>>>>>>> OPENSOURCE/master
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    mCluster.waitForAllNodesRegistered(WAIT_TIMEOUT_MS);

<<<<<<< HEAD
    Path file = new Path("/testFile-MultiMaster");
=======
    Path file = new Path("/testFile");
>>>>>>> OPENSOURCE/master
    FsPermission permission = FsPermission.createImmutable((short) 0666);
    FSDataOutputStream o = fs.create(file, permission, false /* ignored */, 10 /* ignored */,
        (short) 1 /* ignored */, 512 /* ignored */, null /* ignored */);
    o.writeBytes("Test Bytes");
    o.close();
    // with mark of delete-on-exit, the close method will try to delete it
    fs.deleteOnExit(file);
    fs.close();
    mCluster.notifySuccess();
  }
<<<<<<< HEAD
  // ALLUXIO CS END
=======
>>>>>>> OPENSOURCE/master
}
