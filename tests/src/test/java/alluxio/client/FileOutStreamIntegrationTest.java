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
import alluxio.PropertyKey;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

/**
 * Integration tests for {@link alluxio.client.file.FileOutStream}.
 */
public final class FileOutStreamIntegrationTest extends AbstractFileOutStreamIntegrationTest {
  // TODO(binfan): Run tests with local writes enabled and disabled.
  private static final List<WriteType> TEST_WRITE_TYPES = Lists.newArrayList(
      // ALLUXIO CS ADD
      // WriteType.DURABLE,
      // ALLUXIO CS END
      WriteType.CACHE_THROUGH, WriteType.MUST_CACHE, WriteType.THROUGH, WriteType.ASYNC_THROUGH
  );

  /**
   * Tests {@link FileOutStream#write(int)}.
   */
  @Test
  public void writeBytes() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      for (WriteType type : TEST_WRITE_TYPES) {
        CreateFileOptions op = CreateFileOptions.defaults().setWriteType(type);
        AlluxioURI filePath = new AlluxioURI(PathUtils.concatPath(uniqPath,
            "file_" + len + "_" + type));
        writeIncreasingBytesToFile(filePath, len, op);
        checkFile(filePath, type.getAlluxioStorageType().isStore(),
            type.getUnderStorageType().isSyncPersist(), len);
      }
    }
  }

  /**
   * Tests {@link FileOutStream#write(byte[])}.
   */
  @Test
  public void writeByteArray() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      for (WriteType type : TEST_WRITE_TYPES) {
        CreateFileOptions op = CreateFileOptions.defaults().setWriteType(type);
        AlluxioURI filePath = new AlluxioURI(PathUtils.concatPath(uniqPath,
            "file_" + len + "_" + type));
        writeIncreasingByteArrayToFile(filePath, len, op);
        checkFile(filePath, type.getAlluxioStorageType().isStore(),
            type.getUnderStorageType().isSyncPersist(), len);
      }
    }
  }

  /**
   * Tests {@link FileOutStream#write(byte[], int, int)}.
   */
  @Test
  public void writeTwoByteArrays() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int len = MIN_LEN; len <= MAX_LEN; len += DELTA) {
      for (WriteType type : TEST_WRITE_TYPES) {
        CreateFileOptions op = CreateFileOptions.defaults().setWriteType(type);
        AlluxioURI filePath = new AlluxioURI(PathUtils.concatPath(uniqPath,
            "file_" + len + "_" + type));
        writeTwoIncreasingByteArraysToFile(filePath, len, op);
        checkFile(filePath, type.getAlluxioStorageType().isStore(),
            type.getUnderStorageType().isSyncPersist(), len);
      }
    }
  }

  /**
   * Tests writing to a file and specify the location to be localhost.
   */
  @Test
  public void writeSpecifyLocal() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH)
            .setLocationPolicy(new LocalFirstPolicy()));
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();
    checkFile(filePath, true, true, length);
  }

  /**
   * Tests writing to a file for longer than HEARTBEAT_INTERVAL_MS to make sure the sessionId
   * doesn't change. Tracks [ALLUXIO-171].
   */
  @Test
  public void longWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    final int length = 2;
    FileOutStream os = mFileSystem.createFile(filePath,
            CreateFileOptions.defaults().setWriteType(WriteType.THROUGH));
    os.write((byte) 0);
    Thread.sleep(Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS) * 2);
    os.write((byte) 1);
    os.close();
    checkFile(filePath, false, true, length);
  }

  /**
   * Tests if out-of-order writes are possible. Writes could be out-of-order when the following are
   * both true: - a "large" write (over half the internal buffer size) follows a smaller write. -
   * the "large" write does not cause the internal buffer to overflow.
   */
  @Test
  public void outOfOrderWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath,
            CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE));

    // Write something small, so it is written into the buffer, and not directly to the file.
    os.write((byte) 0);

    // A length greater than 0.5 * BUFFER_BYTES and less than BUFFER_BYTES.
    int length = (BUFFER_BYTES * 3) / 4;

    // Write a large amount of data (larger than BUFFER_BYTES/2, but will not overflow the buffer.
    os.write(BufferUtils.getIncreasingByteArray(1, length));
    os.close();

    checkFile(filePath, true, false, length + 1);
  }
}
