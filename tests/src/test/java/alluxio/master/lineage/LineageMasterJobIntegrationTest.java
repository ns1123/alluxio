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

package alluxio.master.lineage;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.IntegrationTestConstants;
import alluxio.IntegrationTestUtils;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.lineage.LineageFileSystem;
import alluxio.client.lineage.LineageMasterClient;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.file.async.AsyncPersistHandler;
import alluxio.master.file.async.JobAsyncPersistHandler;
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.wire.LineageInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for the lineage module using job service.
 */
public class LineageMasterJobIntegrationTest {
  private static final int BLOCK_SIZE_BYTES = 128;
  private static final int BUFFER_BYTES = 100;
  private static final int CHECKPOINT_INTERVAL_MS = 100;
  private static final String OUT_FILE = "/test";
  private static final int RECOMPUTE_INTERVAL_MS = 1000;
  private static final long WORKER_CAPACITY_BYTES = Constants.GB;

  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;
  private CommandLineJob mJob;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, WORKER_CAPACITY_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES))
          .setProperty(PropertyKey.WORKER_DATA_SERVER_CLASS,
              IntegrationTestConstants.NETTY_DATA_SERVER)
          .setProperty(PropertyKey.USER_LINEAGE_ENABLED, "true")
          .setProperty(PropertyKey.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS,
              Integer.toString(RECOMPUTE_INTERVAL_MS))
          .setProperty(PropertyKey.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS,
              Integer.toString(CHECKPOINT_INTERVAL_MS))
          .setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, "test")
          .setProperty(PropertyKey.USER_FILE_REPLICATION_DURABLE, 1)
          .build();

  @Before
  public void before() throws Exception {
    AuthenticatedClientUser.set("test");
    mJob = new CommandLineJob("test", new JobConf("output"));
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    // Replace the default async persist handler with the job-based async persist handler.
    Configuration.set(PropertyKey.MASTER_FILE_ASYNC_PERSIST_HANDLER,
        JobAsyncPersistHandler.class.getCanonicalName());
    Whitebox.setInternalState(
        mLocalAlluxioClusterResource.get().getMaster().getInternalMaster().getFileSystemMaster(),
        "mAsyncPersistHandler", AsyncPersistHandler.Factory.create(null));
  }

  @After
  public void after() throws Exception {
    mLocalAlluxioJobCluster.stop();
    AuthenticatedClientUser.remove();
    LoginUserTestUtils.resetLoginUser();
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void lineageCompleteAndAsyncPersistTest() throws Exception {
    try (LineageMasterClient lineageMasterClient = getLineageMasterClient()) {
      ArrayList<String> outFiles = new ArrayList<>();
      Collections.addAll(outFiles, OUT_FILE);
      lineageMasterClient.createLineage(new ArrayList<String>(), outFiles, mJob);

      CreateFileOptions options =
          CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
              .setBlockSizeBytes(BLOCK_SIZE_BYTES);
      LineageFileSystem fs = (LineageFileSystem) mLocalAlluxioClusterResource.get().getClient();
      FileOutStream outputStream = fs.createFile(new AlluxioURI(OUT_FILE), options);
      outputStream.write(1);
      outputStream.close();

      List<LineageInfo> infos = lineageMasterClient.getLineageInfoList();
      AlluxioURI uri = new AlluxioURI(infos.get(0).getOutputFiles().get(0));
      URIStatus status = getFileSystemMasterClient().getStatus(uri);
      Assert.assertNotEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
      Assert.assertTrue(status.isCompleted());

      IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, uri);

      // worker notifies the master
      status = getFileSystemMasterClient().getStatus(uri);
      Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    }
  }

  private LineageMasterClient getLineageMasterClient() {
    return new LineageMasterClient(mLocalAlluxioClusterResource.get().getMaster().getAddress());
  }

  private FileSystemMasterClient getFileSystemMasterClient() {
    return new FileSystemMasterClient(mLocalAlluxioClusterResource.get().getMaster().getAddress());
  }

}
