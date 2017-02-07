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

package alluxio.network.protocol;

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Unit tests for {@link RPCSaslTokenRequest}.
 */
public class RPCSaslTokenRequestTest {
  private long mTokenLength;
  private DataBuffer mToken;
  private ByteBuf mBuffer;

  private void assertValid(RPCSaslTokenRequest req) {
    try {
      req.validate();
    } catch (Exception e) {
      Assert.fail("request should be valid.");
    }
  }

  /**
   * Sets up the buffer before a test runs.
   */
  @Before
  public final void before() {
    mTokenLength = (long) 4;
    ByteBuffer token = ByteBuffer.allocate((int) mTokenLength);
    token.allocate((int) mTokenLength);
    token.wrap("test".getBytes());
    mToken = new DataByteBuffer(token, (int) mTokenLength);
    mBuffer = Unpooled.buffer();
  }

  @Test
  public void validateFromByteArray() {
    byte[] bytes = "test".getBytes();
    RPCSaslTokenRequest req = new RPCSaslTokenRequest(bytes);
    assertValid(req);
    Assert.assertEquals(4, req.getLength());
  }

  @Test
  public void validateFromDataBuffer() {
    RPCSaslTokenRequest req = new RPCSaslTokenRequest(mToken);
    assertValid(req);
    Assert.assertEquals(mTokenLength, req.getLength());
  }

  @Test
  public void encodedLength() {
    RPCSaslTokenRequest req = new RPCSaslTokenRequest(mToken);
    int encodedLength = req.getEncodedLength();
    req.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }
}
