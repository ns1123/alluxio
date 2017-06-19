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
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.file.CryptoFileInStream;
import alluxio.client.file.CryptoFileOutStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.underfs.hdfs.LocalMiniDFSCluster;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.InputStream;

/**
 * Integration tests for {@link CryptoFileOutStream}, parameterized by the write types.
 */
@RunWith(Parameterized.class)
public final class CryptoFileOutStreamIntegrationTest extends AbstractFileOutStreamIntegrationTest {
  private alluxio.proto.security.EncryptionProto.Meta mMeta;

  @Parameters
  public static Object[] data() {
    return new Object[] {
        WriteType.ASYNC_THROUGH,
        WriteType.CACHE_THROUGH,
        WriteType.MUST_CACHE,
        WriteType.THROUGH,
    };
  }

  @Parameter
  public WriteType mWriteType;

  @Before
  public void before() throws Exception {
    super.before();
    mMeta = alluxio.client.util.EncryptionMetaTestUtils.create();
  }

  @Override
  protected LocalAlluxioClusterResource buildLocalAlluxioClusterResource() {
    return new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, BUFFER_BYTES)
        .setProperty(PropertyKey.USER_FILE_REPLICATION_DURABLE, 1)
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES)
        .setProperty(PropertyKey.SECURITY_ENCRYPTION_ENABLED, true)
        .build();
  }

  @Test
  public void writeBytes() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      CreateFileOptions op = CreateFileOptions.defaults().setWriteType(mWriteType);
      AlluxioURI filePath =
          new AlluxioURI(PathUtils.concatPath(uniqPath, "file_" + len + "_" + mWriteType));
      writeIncreasingBytesToFile(filePath, len, op);
      if (mWriteType.getAlluxioStorageType().isStore()) {
        checkEncryptedFileInAlluxio(filePath, len);
      }
      if (mWriteType.getUnderStorageType().isSyncPersist()) {
        checkEncryptedFileInUnderStorage(filePath);
      }
    }
  }

  @Test
  public void writeTwoByteArrays() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      CreateFileOptions op = CreateFileOptions.defaults().setWriteType(mWriteType);
      AlluxioURI filePath =
          new AlluxioURI(PathUtils.concatPath(uniqPath, "file_" + len + "_" + mWriteType));
      writeTwoIncreasingByteArraysToFile(filePath, len, op);
      if (mWriteType.getAlluxioStorageType().isStore()) {
        checkEncryptedFileInAlluxio(filePath, len);
      }
      if (mWriteType.getUnderStorageType().isSyncPersist()) {
        checkEncryptedFileInUnderStorage(filePath);
      }
    }
  }

  @Test
  public void writeSpecifyLocal() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length = 2;
    try (FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(mWriteType)
            .setLocationPolicy(new LocalFirstPolicy()))) {
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write((byte) 0);
      os.write((byte) 1);
    }
    if (mWriteType.getAlluxioStorageType().isStore()) {
      checkEncryptedFileInAlluxio(filePath, length);
    }
    if (mWriteType.getUnderStorageType().isSyncPersist()) {
      checkEncryptedFileInUnderStorage(filePath);
    }
  }

  @Test
  public void longWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length = 2;
    try (FileOutStream os = mFileSystem.createFile(
        filePath, CreateFileOptions.defaults().setWriteType(mWriteType))) {
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write((byte) 0);
      Thread.sleep(Configuration.getMs(PropertyKey.USER_HEARTBEAT_INTERVAL_MS) * 2);
      os.write((byte) 1);
    }
    if (mWriteType.getAlluxioStorageType().isStore()) {
      checkEncryptedFileInAlluxio(filePath, length);
    }
    if (mWriteType.getUnderStorageType().isSyncPersist()) {
      checkEncryptedFileInUnderStorage(filePath);
    }
  }

  @Test
  public void outOfOrderWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    // A length greater than 0.5 * BUFFER_BYTES and less than BUFFER_BYTES.
    int length = (BUFFER_BYTES * 3) / 4;
    try (FileOutStream os = mFileSystem.createFile(
        filePath, CreateFileOptions.defaults().setWriteType(mWriteType))) {
      // Write something small, so it is written into the buffer, and not directly to the file.
      os.write((byte) 0);
      // Write a large amount of data (larger than BUFFER_BYTES/2, but will not overflow the buffer.
      os.write(BufferUtils.getIncreasingByteArray(1, length));
    }
    if (mWriteType.getAlluxioStorageType().isStore()) {
      checkEncryptedFileInAlluxio(filePath, length + 1);
    }
    if (mWriteType.getUnderStorageType().isSyncPersist()) {
      checkEncryptedFileInUnderStorage(filePath);
    }
  }

  private void checkEncryptedFileInAlluxio(AlluxioURI filePath, int fileLen) throws Exception {
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(fileLen, status.getLength());
    Assert.assertTrue(status.isEncrypted());
    try (FileInStream is = mFileSystem
        .openFile(filePath, OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE))) {
      Assert.assertTrue(is instanceof CryptoFileInStream);
      byte[] res = new byte[(int) status.getLength()];
      Assert.assertEquals((int) status.getLength(), is.read(res));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(fileLen, res));
    }
  }

  private void checkEncryptedFileInUnderStorage(AlluxioURI filePath) throws Exception {
    URIStatus status = mFileSystem.getStatus(filePath);
    String checkpointPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(checkpointPath);

    UfsFileStatus ufsStatus = ufs.getFileStatus(checkpointPath);
    long ufsLen = ufsStatus.getContentLength();
    long expectedPhysicalFileLength = LayoutUtils.toPhysicalFileLength(mMeta, status.getLength());
    Assert.assertEquals(expectedPhysicalFileLength, ufsLen);
    try (InputStream is = ufs.open(checkpointPath)) {
      byte[] res = new byte[(int) ufsLen];
      String underFSClass = UnderFileSystemCluster.getUnderFSClass();
      if ((LocalMiniDFSCluster.class.getName().equals(underFSClass)) && 0 == res.length) {
        // Returns -1 for zero-sized byte array to indicate no more bytes available here.
        Assert.assertEquals(-1, is.read(res));
      } else {
        Assert.assertEquals(expectedPhysicalFileLength, is.read(res));
      }
    }
  }
}
