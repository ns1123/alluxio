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

package alluxio.client.block.stream;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;

import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Unit tests for {@link CryptoPacketWriter}.
 */
public final class CryptoPacketWriterTest {
  private static final int CHUNK_SIZE = Constants.KB;
  private static final int CHUNK_FOOTER_SIZE = 16;
  private static final int PHYSICAL_CHUNK_SIZE = CHUNK_SIZE + CHUNK_FOOTER_SIZE;

  @Before
  public void before() {
    Configuration.set(PropertyKey.USER_BLOCK_HEADER_SIZE_BYTES, "0B");
    Configuration.set(PropertyKey.USER_BLOCK_FOOTER_SIZE_BYTES, "0B");
    Configuration.set(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES, "0B");
    Configuration.set(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES, "1KB");
    Configuration.set(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES, "16B");
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void writeAtChunkBoundary() throws Exception {
    for (int numChunks = 1; numChunks <= 5; numChunks++) {
      byte[] testChunks =
          new String(new char[numChunks * CHUNK_SIZE]).replace('\0', 'a').getBytes();
      TestPacketWriter testPacketWriter =
          new TestPacketWriter(ByteBuffer.allocateDirect(numChunks * PHYSICAL_CHUNK_SIZE));
      CryptoPacketWriter cryptoWriter = new CryptoPacketWriter(testPacketWriter);
      Assert.assertEquals(0L, cryptoWriter.pos());
      cryptoWriter.writePacket(Unpooled.wrappedBuffer(testChunks));
      Assert.assertEquals(numChunks * (CHUNK_SIZE + CHUNK_FOOTER_SIZE), testPacketWriter.pos());
      Assert.assertEquals(numChunks * CHUNK_SIZE, cryptoWriter.pos());
      cryptoWriter.close();
    }
  }

  @Test
  public void smallWrite() throws Exception {
    byte[] smallChunk = "smallfile".getBytes();
    TestPacketWriter testPacketWriter =
        new TestPacketWriter(ByteBuffer.allocateDirect(PHYSICAL_CHUNK_SIZE));
    CryptoPacketWriter cryptoWriter = new CryptoPacketWriter(testPacketWriter);
    Assert.assertEquals(0L, cryptoWriter.pos());
    cryptoWriter.writePacket(Unpooled.wrappedBuffer(smallChunk));
    Assert.assertEquals(smallChunk.length + CHUNK_FOOTER_SIZE, testPacketWriter.pos());
    Assert.assertEquals(smallChunk.length, cryptoWriter.pos());
    cryptoWriter.close();
  }

  @Test
  public void writeTwiceWithLastIncompleteChunk() throws Exception {
    byte[] fullChunk = new String(new char[CHUNK_SIZE]).replace('\0', 'a').getBytes();
    byte[] partialChunk = new String("appendthis").getBytes();
    TestPacketWriter testPacketWriter =
        new TestPacketWriter(ByteBuffer.allocateDirect(2 * PHYSICAL_CHUNK_SIZE));
    CryptoPacketWriter cryptoWriter = new CryptoPacketWriter(testPacketWriter);
    Assert.assertEquals(0L, cryptoWriter.pos());
    cryptoWriter.writePacket(Unpooled.wrappedBuffer(fullChunk));
    Assert.assertEquals(CHUNK_SIZE, cryptoWriter.pos());
    Assert.assertEquals(CHUNK_SIZE + CHUNK_FOOTER_SIZE, testPacketWriter.pos());
    cryptoWriter.writePacket(Unpooled.wrappedBuffer(partialChunk));
    Assert.assertEquals(CHUNK_SIZE + partialChunk.length, cryptoWriter.pos());
    Assert.assertEquals(CHUNK_SIZE + CHUNK_FOOTER_SIZE + partialChunk.length + CHUNK_FOOTER_SIZE,
        testPacketWriter.pos());
    cryptoWriter.close();
  }
}
