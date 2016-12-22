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

package alluxio.client.block;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Unit test of {@link ReplicatedBlockOutStream}.
 */
public final class ReplicatedBlockOutStreamTest {
  private static final int BLOCK_LENGTH = 512;
  private static final byte[] INCREASING_BYTES =
      BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH);

  private TestBufferedBlockOutStream mBlockOutStream1;
  private TestBufferedBlockOutStream mBlockOutStream2;
  private ReplicatedBlockOutStream mTestStream;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mBlockOutStream1 = new TestBufferedBlockOutStream(1, BLOCK_LENGTH, FileSystemContext.INSTANCE);
    mBlockOutStream2 = new TestBufferedBlockOutStream(2, BLOCK_LENGTH, FileSystemContext.INSTANCE);
    mTestStream = new ReplicatedBlockOutStream(3, BLOCK_LENGTH, FileSystemContext.INSTANCE,
        Lists.newArrayList(mBlockOutStream1, mBlockOutStream2));
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void singleByteWrite() throws Exception {
    mTestStream.write(5);
    mTestStream.flush();
    Assert.assertArrayEquals(new byte[] {5}, mBlockOutStream1.getWrittenData());
    Assert.assertArrayEquals(new byte[] {5}, mBlockOutStream2.getWrittenData());
  }

  @Test
  public void byteArrayWrite() throws IOException {
    mTestStream.write(INCREASING_BYTES, 0, BLOCK_LENGTH);
    mTestStream.flush();
    Assert.assertArrayEquals(INCREASING_BYTES, mBlockOutStream1.getWrittenData());
    Assert.assertArrayEquals(INCREASING_BYTES, mBlockOutStream2.getWrittenData());
  }

  @Test
  public void byteArrayWriteSmallBuffer() throws IOException {
    // Reset the buffer size
    Configuration.set(PropertyKey.USER_FILE_BUFFER_BYTES, "16B");
    mTestStream = new ReplicatedBlockOutStream(3, BLOCK_LENGTH, FileSystemContext.INSTANCE,
        Lists.newArrayList(mBlockOutStream1, mBlockOutStream2));

    for (int i = 0; i < BLOCK_LENGTH; i++) {
      mTestStream.write((byte) i);
    }
    mTestStream.flush();
    Assert.assertEquals(BLOCK_LENGTH, mBlockOutStream1.getWrittenBytes());
    Assert.assertEquals(BLOCK_LENGTH, mBlockOutStream2.getWrittenBytes());
  }

  @Test
  public void writeToClosed() throws Exception {
    mTestStream.close();
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(PreconditionMessage.ERR_CLOSED_BLOCK_OUT_STREAM.toString());
    mTestStream.write(0);
  }

  @Test
  public void flush() throws Exception {
    mTestStream.write(5);
    Assert.assertEquals(0, mBlockOutStream1.getFlushedBytes());
    Assert.assertEquals(0, mBlockOutStream2.getFlushedBytes());

    mTestStream.flush();
    Assert.assertEquals(1, mBlockOutStream1.getFlushedBytes());
    Assert.assertEquals(1, mBlockOutStream2.getFlushedBytes());
  }

  @Test
  public void emptyFlush() throws Exception {
    mTestStream.flush();
    Assert.assertEquals(0, mBlockOutStream1.getFlushedBytes());
    Assert.assertEquals(0, mBlockOutStream2.getFlushedBytes());
  }

  @Test
  public void doubleFlush() throws Exception {
    mTestStream.write(INCREASING_BYTES, 1, 10);
    Assert.assertEquals(0, mBlockOutStream1.getFlushedBytes());
    Assert.assertEquals(0, mBlockOutStream2.getFlushedBytes());
    mTestStream.flush();
    Assert.assertEquals(10, mBlockOutStream1.getFlushedBytes());
    Assert.assertEquals(10, mBlockOutStream2.getFlushedBytes());
    mTestStream.flush();
    Assert.assertEquals(10, mBlockOutStream1.getFlushedBytes());
    Assert.assertEquals(10, mBlockOutStream2.getFlushedBytes());
  }

  @Test
  public void cancel() throws Exception {
    mTestStream.cancel();
    Assert.assertTrue(mBlockOutStream1.isCanceled());
    Assert.assertTrue(mBlockOutStream2.isCanceled());
  }

  @Test
  public void close() throws Exception {
    mTestStream.close();
    Assert.assertTrue(mBlockOutStream1.isClosed());
    Assert.assertTrue(mBlockOutStream2.isClosed());
  }

  @Test
  public void doubleClose() throws Exception {
    mTestStream.close();
    mTestStream.close();
    Assert.assertTrue(mBlockOutStream1.isClosed());
    Assert.assertTrue(mBlockOutStream2.isClosed());
  }
}
