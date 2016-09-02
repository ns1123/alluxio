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
 * This represents the request to complete a Sasl message.
 */
public class RPCSaslCompleteResponse extends RPCResponse {
  private final Status mStatus;

  /**
   * Constructs a new RPC response of a {@link RPCSaslTokenRequest}.
   *
   * @param status the status of the response
   */
  public RPCSaslCompleteResponse(Status status) {
    mStatus = status;
  }

  @Override
  public Type getType() {
    return Type.RPC_SASL_COMPLETE_RESPONSE;
  }

  /**
   * Creates a {@link RPCSaslCompleteResponse} object that indicates an error from the given
   * {@link RPCSaslTokenRequest}.
   *
   * @param status the {@link alluxio.network.protocol.RPCResponse.Status} for the response
   * @return the generated {@link RPCSaslCompleteResponse} object
   */
  public static RPCSaslCompleteResponse createErrorResponse(final Status status) {
    Preconditions.checkArgument(status != Status.SUCCESS);
    return new RPCSaslCompleteResponse(status);
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCSaslCompleteResponse} object and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return the decoded {@link RPCSaslCompleteResponse} object
   */
  public static RPCSaslCompleteResponse decode(ByteBuf in) {
    short status = in.readShort();
    return new RPCSaslCompleteResponse(Status.fromShort(status));
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
