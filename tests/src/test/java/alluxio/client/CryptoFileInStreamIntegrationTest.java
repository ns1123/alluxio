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
import alluxio.BaseIntegrationTest;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.file.CryptoFileInStream;
import alluxio.client.file.CryptoFileOutStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.security.authorization.Mode;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.LoadMetadataType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Integration tests for {@link CryptoFileInStream}.
 */
public final class CryptoFileInStreamIntegrationTest extends BaseIntegrationTest {
  // Set the logical block size to be 4 logical encryption chunks.
  private static final int BLOCK_SIZE = 256 * 1024;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.SECURITY_ENCRYPTION_ENABLED, true)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE)
          .build();

  private FileSystem mFileSystem;
  private CreateFileOptions mWriteBoth;
  private CreateFileOptions mWriteAlluxio;
  private CreateFileOptions mWriteUnderStore;
  private String mTestPath;

  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(60);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteBoth = CreateFileOptions.defaults().setMode(Mode.createFullAccess())
        .setWriteType(WriteType.CACHE_THROUGH);
    mWriteAlluxio = CreateFileOptions.defaults().setMode(Mode.createFullAccess())
        .setWriteType(WriteType.MUST_CACHE);
    mWriteUnderStore = CreateFileOptions.defaults().setMode(Mode.createFullAccess())
        .setWriteType(WriteType.THROUGH);
    mTestPath = PathUtils.uniqPath();
  }

  private List<CreateFileOptions> getOptionSet() {
    List<CreateFileOptions> ret = new ArrayList<>(3);
    ret.add(mWriteBoth);
    ret.add(mWriteAlluxio);
    ret.add(mWriteUnderStore);
    return ret;
  }

  @Test
  public void readByteAfterWriteByte() throws Exception {
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + options.getWriteType().toString();
      AlluxioURI uri = new AlluxioURI(filename);
      FileOutStream os = mFileSystem.createFile(uri, options);
      // Write one byte.
      Random random = new Random();
      int b = random.nextInt(64);
      os.write(b);
      os.close();

      FileInStream is = mFileSystem.openFile(uri);
      // Read back one byte and verify the data.
      Assert.assertEquals(b, is.read());
    }
  }

  @Test
  public void readFullAfterWrite() throws Exception {
    // Create a file with a full block. File footer is in a separate physical block.
    int fileLength = BLOCK_SIZE;
    byte[] expected = BufferUtils.getIncreasingByteArray(1, fileLength);
    byte[] actual = new byte[fileLength];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + options.getWriteType().toString();
      AlluxioURI uri = new AlluxioURI(filename);
      FileOutStream os = mFileSystem.createFile(uri, options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(uri);
      Assert.assertTrue(is instanceof CryptoFileInStream);
      // Read back the block and verify the data.
      Assert.assertEquals(fileLength, is.read(actual));
      Assert.assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void readWithOffAndLen() throws Exception {
    // Create a file with a full block and a partial block. File footer is split to two physical
    // blocks.
    int fileLength = BLOCK_SIZE * 2 - 1;
    byte[] expected = BufferUtils.getIncreasingByteArray(1, fileLength);
    byte[] actual = new byte[fileLength];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + options.getWriteType().toString();
      AlluxioURI uri = new AlluxioURI(filename);
      FileOutStream os = mFileSystem.createFile(uri, options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(uri);
      Assert.assertTrue(is instanceof CryptoFileInStream);
      // Read a random len with a random off.
      Random random = new Random();
      int off = random.nextInt(BLOCK_SIZE);
      int len = random.nextInt(BLOCK_SIZE - 1);
      Assert.assertEquals(len, is.read(actual, off, len));
      Assert.assertArrayEquals(Arrays.copyOf(expected, len),
          Arrays.copyOfRange(actual, off, off + len));
    }
  }

  @Test
  public void seek() throws Exception {
    // Create a file with two full blocks and a partial block.
    int fileLength = BLOCK_SIZE * 2 + 1;
    byte[] expected = BufferUtils.getIncreasingByteArray(1, fileLength);
    int len = BLOCK_SIZE - 1;
    byte[] actual = new byte[BLOCK_SIZE - 1];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + options.getWriteType().toString();
      AlluxioURI uri = new AlluxioURI(filename);
      FileOutStream os = mFileSystem.createFile(uri, options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(uri);
      Assert.assertTrue(is instanceof CryptoFileInStream);
      Random random = new Random();
      int seekPos = random.nextInt(BLOCK_SIZE);
      is.seek(seekPos);
      Assert.assertEquals(len, is.read(actual));
      Assert.assertArrayEquals(
          Arrays.copyOfRange(expected, seekPos, seekPos + len), actual);

      int seekBackwardPos = seekPos - 1;
      is.seek(seekBackwardPos);
      Assert.assertEquals(len, is.read(actual));
      Assert.assertArrayEquals(
          Arrays.copyOfRange(expected, seekBackwardPos, seekBackwardPos + len), actual);

      int seekForwardPos = seekBackwardPos + 2;
      is.seek(seekForwardPos);
      Assert.assertEquals(len, is.read(actual));
      Assert.assertArrayEquals(
          Arrays.copyOfRange(expected, seekForwardPos, seekForwardPos + len), actual);
    }
  }

  @Test
  public void seekInSameEncryptionChunk() throws Exception {
    int fileLength = BLOCK_SIZE / 4;
    byte[] expected = BufferUtils.getIncreasingByteArray(1, fileLength);
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + options.getWriteType().toString();
      AlluxioURI uri = new AlluxioURI(filename);
      FileOutStream os = mFileSystem.createFile(uri, options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(uri);
      Assert.assertTrue(is instanceof CryptoFileInStream);
      int firstSeekPos = 16;
      is.seek(firstSeekPos);
      int len = fileLength - firstSeekPos;
      byte[] actual = new byte[len];
      Assert.assertEquals(len, is.read(actual));

      int secondSeekPos = 4;
      is.seek(secondSeekPos);
      Assert.assertEquals(secondSeekPos + 1, is.read());
    }
  }

  @Test
  public void seekException() throws Exception {
    // Create a file with a half block and a partial block. File footer is within the first block.
    int fileLength = BLOCK_SIZE / 2;
    byte[] expected = BufferUtils.getIncreasingByteArray(1, fileLength);
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + options.getWriteType().toString();
      AlluxioURI uri = new AlluxioURI(filename);
      FileOutStream os = mFileSystem.createFile(uri, options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(uri);
      Assert.assertTrue(is instanceof CryptoFileInStream);
      try {
        is.seek(-1);
        Assert.fail("Seek to negative pos should fail.");
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

  @Test
  public void seekEofAndRead() throws Exception {
    // Create a file with a half block and a partial block. File footer is within the first block.
    int fileLength = BLOCK_SIZE / 2;
    byte[] expected = BufferUtils.getIncreasingByteArray(1, fileLength);
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + options.getWriteType().toString();
      AlluxioURI uri = new AlluxioURI(filename);
      FileOutStream os = mFileSystem.createFile(uri, options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(uri);
      Assert.assertTrue(is instanceof CryptoFileInStream);
      // seek to EOF.
      is.seek((long) fileLength);
      // Then read should return -1.
      Assert.assertEquals(-1, is.read());
    }
  }

  @Test
  public void skip() throws Exception {
    // Create a file with 2 full blocks.
    int fileLength = BLOCK_SIZE * 2;
    byte[] expected = BufferUtils.getIncreasingByteArray(1, fileLength);
    int len = BLOCK_SIZE / 2;
    byte[] actual = new byte[len];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + options.getWriteType().toString();
      AlluxioURI uri = new AlluxioURI(filename);
      FileOutStream os = mFileSystem.createFile(uri, options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(uri);
      Assert.assertTrue(is instanceof CryptoFileInStream);
      long skipLen = fileLength - len;
      Assert.assertEquals(skipLen, is.skip(skipLen));
      // Read till EOF after skip.
      Assert.assertEquals(len, is.read(actual));
      Assert.assertArrayEquals(Arrays.copyOfRange(expected, fileLength - len, fileLength),
          actual);
    }
  }

  @Test
  public void positionedRead() throws Exception {
    // Create a file with 2 full blocks.
    int fileLength = BLOCK_SIZE * 2;
    byte[] expected = BufferUtils.getIncreasingByteArray(1, fileLength);
    byte[] actual = new byte[BLOCK_SIZE / 2];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + options.getWriteType().toString();
      AlluxioURI uri = new AlluxioURI(filename);
      FileOutStream os = mFileSystem.createFile(uri, options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      Random random = new Random();
      long pos = random.nextInt(BLOCK_SIZE);
      FileInStream is = mFileSystem.openFile(uri);
      Assert.assertTrue(is instanceof CryptoFileInStream);
      // positionedRead half a block with at a random pos
      Assert.assertEquals(BLOCK_SIZE / 2, is.positionedRead(pos, actual, 0, BLOCK_SIZE / 2));
      Assert.assertArrayEquals(
          Arrays.copyOfRange(expected, (int) pos, (int) (pos + BLOCK_SIZE / 2)), actual);
    }
  }

  @Test
  public void readEncryptedFileFromUfs() throws Exception {
    // Create a file with 2 full blocks.
    int fileLength = BLOCK_SIZE * 2;
    byte[] expected = BufferUtils.getIncreasingByteArray(1, fileLength);
    int len = BLOCK_SIZE - 1;
    byte[] actual = new byte[len];
    CreateFileOptions createFileOptions = mWriteBoth;
    String filename = mTestPath + createFileOptions.getWriteType().toString();
    AlluxioURI uri = new AlluxioURI(filename);
    FileOutStream os = mFileSystem.createFile(uri, createFileOptions);
    Assert.assertTrue(os instanceof CryptoFileOutStream);
    // Write both to Alluxio and UFS.
    os.write(expected);
    os.close();
    long oldFileId = mFileSystem.getStatus(uri).getFileId();
    // Delete the file only in Alluxio.
    mFileSystem.delete(uri, DeleteOptions.defaults().setAlluxioOnly(true));
    // Reload the metadata from UFS.
    mFileSystem.listStatus(
        uri, ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always));
    // Verify the file id is different.
    long newFileId = mFileSystem.getStatus(uri).getFileId();
    Assert.assertNotEquals(oldFileId, newFileId);

    // Verify the encryption id can be read from the file footer and data is available after reload.
    FileInStream is = mFileSystem.openFile(uri);
    Assert.assertTrue(is instanceof CryptoFileInStream);
    Assert.assertEquals(len, is.read(actual));
    Assert.assertArrayEquals(Arrays.copyOfRange(expected, 0, len), actual);
  }
}
