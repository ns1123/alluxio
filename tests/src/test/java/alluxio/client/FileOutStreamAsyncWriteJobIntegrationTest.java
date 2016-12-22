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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.IntegrationTestUtils;
import alluxio.PropertyKey;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.file.async.AsyncPersistHandler;
import alluxio.master.file.async.JobAsyncPersistHandler;
import alluxio.master.file.meta.PersistenceState;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

/**
 * Integration tests for {@link FileOutStream} of under storage type being async
 * persist.
 *
 */
public final class FileOutStreamAsyncWriteJobIntegrationTest
    extends AbstractFileOutStreamIntegrationTest {
  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Before
  @Override
  public void before() throws Exception {
    super.before();
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
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  @Ignore
  public void asyncWrite() throws Exception {

    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH));
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    CommonUtils.sleepMs(1);
    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, filePath);

    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    checkFileInAlluxio(filePath, length);
    checkFileInUnderStorage(filePath, length);
  }
}
