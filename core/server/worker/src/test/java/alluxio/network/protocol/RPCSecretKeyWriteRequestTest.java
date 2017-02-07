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

import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Unit tests for {@link RPCSecretKeyWriteRequest}.
 */
public final class RPCSecretKeyWriteRequestTest {
  private void assertValid(RPCSecretKeyWriteRequest req) {
    try {
      req.validate();
    } catch (Exception e) {
      Assert.fail("request should be valid.");
    }
  }

  private void assertInvalid(RPCSecretKeyWriteRequest req) {
    try {
      req.validate();
      Assert.fail("request should be invalid.");
    } catch (Exception e) {
      return;
    }
  }

  @Test
  public void validateFromByteArray() {
    byte[] bytes = "testkey".getBytes();
    RPCSecretKeyWriteRequest req = new RPCSecretKeyWriteRequest(1L, 100000000L, bytes);
    assertValid(req);
    Assert.assertEquals(2 * Longs.BYTES, req.getEncodedLength());
    Assert.assertEquals(1L, req.getKeyId());
    Assert.assertEquals(100000000L, req.getExpirationTimeMs());
  }

  @Test
  public void validateFromDataBuffer() {
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.wrap("test".getBytes());
    DataBuffer key = new DataByteBuffer(buf, 4L);
    RPCSecretKeyWriteRequest req = new RPCSecretKeyWriteRequest(1L, 1000000000L, key);
    assertValid(req);
    Assert.assertEquals(2 * Longs.BYTES, req.getEncodedLength());
    Assert.assertEquals(1L, req.getKeyId());
    Assert.assertEquals(1000000000L, req.getExpirationTimeMs());
  }

  @Test
  public void invalidKeyId() {
    byte[] bytes = "testkey".getBytes();
    RPCSecretKeyWriteRequest req = new RPCSecretKeyWriteRequest(-1L, 100000000L, bytes);
    assertInvalid(req);
  }

  @Test
  public void invalidExpirationTime() {
    byte[] bytes = "testkey".getBytes();
    RPCSecretKeyWriteRequest req = new RPCSecretKeyWriteRequest(1L, -100000000L, bytes);
    assertInvalid(req);
  }
}
