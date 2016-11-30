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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Shorts;
import io.netty.buffer.ByteBuf;

/**
 * This represents the response to a capability key write request.
 */
public class RPCSecretKeyWriteResponse extends RPCResponse {
  private final Status mStatus;

  /**
   * Constructs a new RPC response of a {@link RPCSecretKeyWriteRequest}.
   *
   * @param status the status of the response
   */
  public RPCSecretKeyWriteResponse(Status status) {
    mStatus = status;
  }

  @Override
  public Type getType() {
    return Type.RPC_SECRET_KEY_WRITE_RESPONSE;
  }

  /**
   * Creates a {@link RPCSecretKeyWriteResponse} object that indicates an error from the given
   * {@link RPCSecretKeyWriteRequest}.
   *
   * @param status the {@link Status} for the response
   * @return the generated {@link RPCSecretKeyWriteResponse} object
   */
  public static RPCSecretKeyWriteResponse createErrorResponse(final Status status) {
    Preconditions.checkArgument(status != Status.SUCCESS);
    return new RPCSecretKeyWriteResponse(status);
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCSecretKeyWriteResponse} object and
   * returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return the decoded {@link RPCSecretKeyWriteResponse} object
   */
  public static RPCSecretKeyWriteResponse decode(ByteBuf in) {
    short status = in.readShort();
    return new RPCSecretKeyWriteResponse(Status.fromShort(status));
  }

  @Override
  public int getEncodedLength() {
    // 1 short (mStatus)
    return Shorts.BYTES;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeShort(mStatus.getId());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("status", mStatus).toString();
  }

  @Override
  public Status getStatus() {
    return mStatus;
  }
}
