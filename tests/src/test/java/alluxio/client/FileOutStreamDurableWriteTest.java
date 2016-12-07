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
import alluxio.IntegrationTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.master.file.meta.PersistenceState;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests on writing a file using {@link WriteType#DURABLE} by Alluxio Client.
 */
public final class FileOutStreamDurableWriteTest extends AbstractFileOutStreamIntegrationTest {
  private static final int LEN = 1024;

  @Test
  public void simpleWrite() throws Exception {
    AlluxioURI uri = new AlluxioURI(PathUtils.uniqPath());
    writeIncreasingByteArrayToFile(uri, LEN,
        CreateFileOptions.defaults().setWriteType(WriteType.DURABLE));

    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertNotEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    //Assert.assertEquals(status.ge);
    Assert.assertTrue(status.isCompleted());
    checkFile(uri, true, false, LEN);

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, uri);

    status = mFileSystem.getStatus(uri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    checkFile(uri, true, true, LEN);
  }

  @Test
  public void renameAfterWrite() throws Exception {
    AlluxioURI uri = new AlluxioURI(PathUtils.uniqPath());
    AlluxioURI renamedUri = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createFile(uri, CreateFileOptions.defaults().setWriteType(WriteType.DURABLE))
        .close();

    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertNotEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    mFileSystem.rename(uri, renamedUri);
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, renamedUri);

    status = mFileSystem.getStatus(uri);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    checkFile(uri, true, true, 0);
  }
}
