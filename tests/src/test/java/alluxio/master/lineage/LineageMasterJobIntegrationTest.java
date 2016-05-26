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
import alluxio.Constants;
import alluxio.IntegrationTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.lineage.LineageFileSystem;
import alluxio.client.lineage.LineageMasterClient;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.MasterContext;
import alluxio.master.file.async.AsyncPersistHandler;
import alluxio.master.file.async.JobAsyncPersistHandler;
import alluxio.master.file.meta.PersistenceState;
import alluxio.wire.LineageInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for the lineage module using job service.
 */
public class LineageMasterJobIntegrationTest extends LineageMasterIntegrationTest {
  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Before
  public void before() throws Exception {
    super.before();
    mLocalAlluxioJobCluster =
        new LocalAlluxioJobCluster(mLocalAlluxioClusterResource.get().getWorkerConf());
    mLocalAlluxioJobCluster.start();
    mTestConf = mLocalAlluxioJobCluster.getTestConf();
    // Replace the default async persist handler with the job-based async persist handler.
    mTestConf.set(Constants.MASTER_FILE_ASYNC_PERSIST_HANDLER,
        JobAsyncPersistHandler.class.getCanonicalName());
    Whitebox.setInternalState(
        mLocalAlluxioClusterResource.get().getMaster().getInternalMaster().getFileSystemMaster(),
        "mAsyncPersistHandler", AsyncPersistHandler.Factory.create(MasterContext.getConf(), null));
  }

  @After
  public void after() throws Exception {
    mLocalAlluxioJobCluster.stop();
  }

  @Test
  public void lineageCompleteAndAsyncPersistTest() throws Exception {
    LineageMasterClient lineageMasterClient = getLineageMasterClient();

    try {
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

    } finally {
      lineageMasterClient.close();
    }
  }
}
