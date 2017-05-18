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
import alluxio.client.EncryptionMetaFactory;
import alluxio.client.LayoutUtils;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.security.EncryptionProto;

import io.netty.buffer.Unpooled;
import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Unit tests for {@link CryptoPacketReader}.
 */
public final class CryptoPacketReaderTest {
  private static final int CHUNK_SIZE = Constants.KB;
  private static final int CHUNK_FOOTER_SIZE = 16;
  private static final int PHYSICAL_CHUNK_SIZE = CHUNK_SIZE + CHUNK_FOOTER_SIZE;

  private EncryptionProto.Meta mMeta;

  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.USER_BLOCK_HEADER_SIZE_BYTES, "0B");
    Configuration.set(PropertyKey.USER_BLOCK_FOOTER_SIZE_BYTES, "0B");
    Configuration.set(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES, "0B");
    Configuration.set(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES, "1KB");
    Configuration.set(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES, "16B");
    mMeta = EncryptionMetaFactory.createFromConfiguration();
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void readFullChunks() throws Exception {
    for (int numChunks = 1; numChunks <= 5; numChunks++) {
      int physicalLen = numChunks * PHYSICAL_CHUNK_SIZE;
      byte[] testChunks =
          new String(new char[numChunks * CHUNK_SIZE]).replace('\0', 'a').getBytes();
      byte[] ciphertext = prepareCiphertext(testChunks);
      Assert.assertEquals(physicalLen, ciphertext.length);

      TestPacketReader testPacketReader =
          new TestPacketReader(ciphertext, 0, physicalLen, PHYSICAL_CHUNK_SIZE);
      CryptoPacketReader cryptoReader =
          new CryptoPacketReader(testPacketReader, 0, numChunks * CHUNK_SIZE, mMeta);
      for (int i = 1; i <= numChunks; i++) {
        cryptoReader.readPacket();
        Assert.assertEquals(i * CHUNK_SIZE, cryptoReader.pos());
      }
      cryptoReader.close();
    }
  }

  @Test
  public void smallRead() throws Exception {
    byte[] testFile = "smalltestchunk".getBytes();
    byte[] ciphertext = prepareCiphertext(testFile);
    int physicalLen = ciphertext.length;

    TestPacketReader testPacketReader =
        new TestPacketReader(ciphertext, 0, physicalLen, physicalLen);
    CryptoPacketReader cryptoReader =
        new CryptoPacketReader(testPacketReader, 0, testFile.length, mMeta);
    DataBuffer result = cryptoReader.readPacket();
    Assert.assertEquals(testFile.length, cryptoReader.pos());
    byte[] dataRead = new byte[testFile.length];
    result.readBytes(dataRead, 0, result.readableBytes());
    Assert.assertEquals(new String(dataRead), new String(testFile));
    cryptoReader.close();
  }

  @Test
  public void randomRead() throws Exception {
    byte[] testFile = "randomReadTest".getBytes();
    byte[] ciphertext = prepareCiphertext(testFile);
    int physicalLen = ciphertext.length;

    TestPacketReader testPacketReader =
        new TestPacketReader(ciphertext, 0, physicalLen, physicalLen);
    Random random = new Random();
    int off = random.nextInt(testFile.length - 1);
    int len = random.nextInt(testFile.length - off);
    CryptoPacketReader cryptoReader = new CryptoPacketReader(testPacketReader, off, len, mMeta);
    DataBuffer result = cryptoReader.readPacket();
    Assert.assertEquals(off + len, cryptoReader.pos());

    int bytesRead = result.readableBytes();
    byte[] dataRead = new byte[bytesRead];
    result.readBytes(dataRead, 0, bytesRead);
    byte[] expected = ArrayUtils.subarray(testFile, off, off + len);
    Assert.assertEquals(new String(dataRead), new String(expected));
    cryptoReader.close();
  }

  private byte[] prepareCiphertext(byte[] input) throws Exception {
    int physicalLen =
        input.length + (input.length + CHUNK_SIZE - 1) / CHUNK_SIZE * CHUNK_FOOTER_SIZE;
    TestPacketWriter testPacketWriter =
        new TestPacketWriter(ByteBuffer.allocateDirect(physicalLen));
    CryptoPacketWriter cryptoWriter = new CryptoPacketWriter(testPacketWriter, mMeta);
    cryptoWriter.writePacket(Unpooled.wrappedBuffer(input));
    cryptoWriter.close();

    // TODO(chaomin); just work around to leave the file footer space here.
    byte[] ciphertext = new byte[physicalLen + LayoutUtils.getFooterSize()];
    ByteBuffer buf = testPacketWriter.getInternalByteBuffer().asReadOnlyBuffer();
    buf.flip();
    buf.get(ciphertext, 0 , physicalLen);
    return ciphertext;
  }
}
