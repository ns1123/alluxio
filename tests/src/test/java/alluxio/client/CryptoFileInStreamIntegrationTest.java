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
import alluxio.security.authorization.Mode;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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
      String filename = mTestPath + "readByteAfterWriteByte" + options.getWriteType().toString();
      FileOutStream os = mFileSystem.createFile(new AlluxioURI(filename), options);
      os.write(1);
      os.close();

      FileInStream is = mFileSystem.openFile(new AlluxioURI(filename));
      Assert.assertEquals(1, is.read());
    }
  }

  @Test
  public void readAfterWrite() throws Exception {
    byte[] expected = BufferUtils.getIncreasingByteArray(1, BLOCK_SIZE);
    byte[] actual = new byte[BLOCK_SIZE];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + "readAfterWrite" + options.getWriteType().toString();
      FileOutStream os = mFileSystem.createFile(new AlluxioURI(filename), options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(new AlluxioURI(filename));
      Assert.assertTrue(is instanceof CryptoFileInStream);
      Assert.assertEquals(BLOCK_SIZE, is.read(actual));
      Assert.assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void readPartialAfterWrite() throws Exception {
    byte[] expected = BufferUtils.getIncreasingByteArray(1, BLOCK_SIZE * 2 - 1);
    byte[] actual = new byte[BLOCK_SIZE];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + "readPartialAfterWrite" + options.getWriteType().toString();
      FileOutStream os = mFileSystem.createFile(new AlluxioURI(filename), options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(new AlluxioURI(filename));
      Assert.assertTrue(is instanceof CryptoFileInStream);
      Assert.assertEquals(BLOCK_SIZE, is.read(actual));
      Assert.assertArrayEquals(Arrays.copyOf(expected, BLOCK_SIZE), actual);
    }
  }

  @Test
  public void seek() throws Exception {
    byte[] expected = BufferUtils.getIncreasingByteArray(1, BLOCK_SIZE * 2 - 1);
    byte[] actual = new byte[BLOCK_SIZE - 1];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + "seek" + options.getWriteType().toString();
      FileOutStream os = mFileSystem.createFile(new AlluxioURI(filename), options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(new AlluxioURI(filename));
      Assert.assertTrue(is instanceof CryptoFileInStream);
      is.seek(BLOCK_SIZE);
      Assert.assertEquals(BLOCK_SIZE - 1, is.read(actual));
      Assert.assertArrayEquals(
          Arrays.copyOfRange(expected, BLOCK_SIZE, 2 * BLOCK_SIZE - 1), actual);
    }
  }

  @Test
  public void skip() throws Exception {
    byte[] expected = BufferUtils.getIncreasingByteArray(1, BLOCK_SIZE * 2);
    byte[] actual = new byte[BLOCK_SIZE / 2];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + "skip" + options.getWriteType().toString();
      FileOutStream os = mFileSystem.createFile(new AlluxioURI(filename), options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      FileInStream is = mFileSystem.openFile(new AlluxioURI(filename));
      Assert.assertTrue(is instanceof CryptoFileInStream);
      is.skip(BLOCK_SIZE * 3 / 2);
      Assert.assertEquals(BLOCK_SIZE / 2, is.read(actual));
      Assert.assertArrayEquals(Arrays.copyOfRange(expected, BLOCK_SIZE * 3 / 2, 2 * BLOCK_SIZE),
          actual);
    }
  }

  @Test
  @Ignore("TODO(chaomin): fix this")
  public void positionedRead() throws Exception {
    byte[] expected = BufferUtils.getIncreasingByteArray(1, BLOCK_SIZE * 2);
    byte[] actual = new byte[BLOCK_SIZE / 2];
    for (CreateFileOptions options : getOptionSet()) {
      String filename = mTestPath + "positionedRead" + options.getWriteType().toString();
      FileOutStream os = mFileSystem.createFile(new AlluxioURI(filename), options);
      Assert.assertTrue(os instanceof CryptoFileOutStream);
      os.write(expected);
      os.close();

      Random random = new Random();
      long pos = random.nextInt(BLOCK_SIZE);
      FileInStream is = mFileSystem.openFile(new AlluxioURI(filename));
      Assert.assertTrue(is instanceof CryptoFileInStream);
      Assert.assertEquals(BLOCK_SIZE / 2, is.positionedRead(pos, actual, 0, BLOCK_SIZE / 2));
      Assert.assertArrayEquals(
          Arrays.copyOfRange(expected, (int) pos, (int) (pos + BLOCK_SIZE / 2)), actual);
    }
  }
}
