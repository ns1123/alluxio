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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link RPCSecretKeyWriteResponse}.
 */
public class RPCSecretKeyWriteResponseTest {
  private static final RPCResponse.Status STATUS = RPCResponse.Status.SUCCESS;

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private ByteBuf mBuffer = null;

  private void assertValid(RPCResponse.Status status, RPCSecretKeyWriteResponse resp) {
    Assert.assertEquals(RPCMessage.Type.RPC_SECRET_KEY_WRITE_RESPONSE, resp.getType());
    Assert.assertEquals(status, resp.getStatus());
  }

  private void assertValid(RPCSecretKeyWriteResponse resp) {
    try {
      resp.validate();
    } catch (Exception e) {
      Assert.fail("response should be valid.");
    }
  }

  /**
   * Sets up the buffer before a test runs.
   */
  @Before
  public final void before() {
    mBuffer = Unpooled.buffer();
  }

  @Test
  public void encodedLength() {
    RPCSecretKeyWriteResponse resp = new RPCSecretKeyWriteResponse(STATUS);
    int encodedLength = resp.getEncodedLength();
    resp.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }

  @Test
  public void encodeDecode() {
    RPCSecretKeyWriteResponse resp = new RPCSecretKeyWriteResponse(STATUS);
    resp.encode(mBuffer);
    RPCSecretKeyWriteResponse resp2 = RPCSecretKeyWriteResponse.decode(mBuffer);
    assertValid(STATUS, resp);
    assertValid(STATUS, resp2);
  }

  @Test
  public void validate() {
    RPCSecretKeyWriteResponse resp = new RPCSecretKeyWriteResponse(STATUS);
    assertValid(resp);
  }

  @Test
  public void createErrorResponse() {
    for (RPCResponse.Status status : RPCResponse.Status.values()) {
      if (status == RPCResponse.Status.SUCCESS) {
        // cannot create an error response with a SUCCESS status.
        mThrown.expect(IllegalArgumentException.class);
        RPCSecretKeyWriteResponse.createErrorResponse(status);
      } else {
        RPCSecretKeyWriteResponse resp = RPCSecretKeyWriteResponse.createErrorResponse(status);
        assertValid(status, resp);
      }
    }
  }
}
