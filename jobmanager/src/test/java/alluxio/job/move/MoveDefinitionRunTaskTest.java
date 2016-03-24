/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job.move;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.MockFileInStream;
import alluxio.client.file.MockFileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.job.JobWorkerContext;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for {@link MoveDefinition#runTask(MoveConfig, List, alluxio.job.JobWorkerContext).
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, JobWorkerContext.class})
public final class MoveDefinitionRunTaskTest {
  private static final String TEST_DIR = "/DIR";
  private static final String TEST_SRC = "/DIR/TEST_SRC";
  private static final String TEST_DST = "/DIR/TEST_DST";
  private static final byte[] TEST_SRC_CONTENTS = BufferUtils.getIncreasingByteArray(100);

  private JobWorkerContext mMockJobWorkerContext;
  private FileSystem mMockFileSystem;

  private MockFileInStream mMockInStream;
  private MockFileOutStream mMockOutStream;

  @Before
  public void before() throws Exception {
    mMockJobWorkerContext = PowerMockito.mock(JobWorkerContext.class);
    mMockFileSystem = PowerMockito.mock(FileSystem.class);
    when(mMockJobWorkerContext.getFileSystem()).thenReturn(mMockFileSystem);

    mMockInStream = new MockFileInStream(TEST_SRC_CONTENTS);
    when(mMockFileSystem.openFile(new AlluxioURI(TEST_SRC))).thenReturn(mMockInStream);
    mMockOutStream = new MockFileOutStream();
    when(mMockFileSystem.createFile(eq(new AlluxioURI(TEST_DST)), any(CreateFileOptions.class)))
        .thenReturn(mMockOutStream);
  }

  /**
   * Tests that the bytes of the file to move are written to the destination stream.
   */
  @Test
  public void basicMoveTest() throws Exception {
    runTask(TEST_SRC, TEST_SRC, TEST_DST, WriteType.THROUGH);
    assertArrayEquals(TEST_SRC_CONTENTS, mMockOutStream.toByteArray());
    verify(mMockFileSystem).delete(new AlluxioURI(TEST_SRC));
  }

  /**
   * Tests that the worker will delete the src directory if the directory contains nothing
   */
  @Test
  public void deleteEmptySrcDir() throws Exception {
    when(mMockFileSystem.listStatus(new AlluxioURI(TEST_DIR)))
        .thenReturn(Lists.<URIStatus>newArrayList());
    runTask(TEST_DIR, TEST_SRC, TEST_DST, WriteType.THROUGH);
    verify(mMockFileSystem).delete(eq(new AlluxioURI(TEST_DIR)), any(DeleteOptions.class));
  }

  /**
   * Tests that the worker will delete the src directory if the directory contains only directories.
   */
  @Test
  public void deleteDirsOnlySrcDir() throws Exception {
    String inner = TEST_DIR + "/innerDir";
    when(mMockFileSystem.listStatus(new AlluxioURI(TEST_DIR))).thenReturn(
        Lists.newArrayList(new URIStatus(new FileInfo().setPath(inner).setFolder(true))));
    when(mMockFileSystem.listStatus(new AlluxioURI(inner)))
        .thenReturn(Lists.<URIStatus>newArrayList());
    runTask(TEST_DIR, TEST_SRC, TEST_DST, WriteType.THROUGH);
    verify(mMockFileSystem).delete(eq(new AlluxioURI(TEST_DIR)), any(DeleteOptions.class));
  }

  /**
   * Tests that the worker will not delete the src directory if the directory still contains files.
   */
  @Test
  public void dontDeleteNonEmptySrcTest() throws Exception {
    when(mMockFileSystem.listStatus(new AlluxioURI(TEST_DIR)))
        .thenReturn(Lists.newArrayList(new URIStatus(new FileInfo())));
    runTask(TEST_DIR, TEST_SRC, TEST_DST, WriteType.THROUGH);
    verify(mMockFileSystem, times(0)).delete(eq(new AlluxioURI(TEST_DIR)), any(DeleteOptions.class));
  }

  /**
   * Tests that the worker writes with the specified write type.
   */
  @Test
  public void writeTypeTest() throws Exception {
    runTask(TEST_SRC, TEST_SRC, TEST_DST, WriteType.CACHE_THROUGH);
    verify(mMockFileSystem).createFile(eq(new AlluxioURI(TEST_DST)),
        eq(CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH)));

    runTask(TEST_SRC, TEST_SRC, TEST_DST, WriteType.MUST_CACHE);
    verify(mMockFileSystem).createFile(eq(new AlluxioURI(TEST_DST)),
        eq(CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)));
  }

  /**
   * Runs the task.
   *
   * @param configSrc {@link MoveConfig} src
   * @param commandSrc {@link MoveCommand} src
   * @param commandDst {@link MoveCommand} dst
   * @param writeType {@link MoveConfig} writeType
   */
  private void runTask(String configSrc, String commandSrc, String commandDst, WriteType writeType)
      throws Exception {
    new MoveDefinition().runTask(new MoveConfig(configSrc, "", writeType.toString(), false),
        Lists.newArrayList(new MoveCommand(commandSrc, commandDst, writeType)),
        mMockJobWorkerContext);
  }
}
