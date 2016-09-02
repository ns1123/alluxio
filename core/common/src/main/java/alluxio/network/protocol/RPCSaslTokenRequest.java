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
import alluxio.network.protocol.databuffer.DataByteArrayChannel;
import alluxio.network.protocol.databuffer.DataByteBuffer;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * This represents the request to send a Sasl token message.
 */
public class RPCSaslTokenRequest extends RPCRequest {
  private final long mLength;
  private final DataBuffer mToken;

  /**
   * Constructs a new request to send a Sasl token.
   *
   * @param length the length of the token in bytes
   * @param token the Sasl token
   */
  public RPCSaslTokenRequest(long length, DataBuffer token) {
    mLength = length;
    mToken = token;
  }

  /**
   * Constructs a new request with given byte array.
   *
   * @param bytes the token in byte array
   */
  public RPCSaslTokenRequest(byte[] bytes) {
    mLength = bytes.length;
    mToken = new DataByteArrayChannel(bytes, 0, (int) mLength);
  }

  @Override
  public Type getType() {
    return Type.RPC_SASL_TOKEN_REQUEST;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCSaslTokenRequest} object and returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded {@link RPCSaslTokenRequest} object
   */
  public static RPCSaslTokenRequest decode(ByteBuf in) {
    long length = in.readLong();
    ByteBuffer buffer = ByteBuffer.allocate((int) length);
    in.readBytes(buffer);
    DataByteBuffer token = new DataByteBuffer(buffer, (int) length);

    return new RPCSaslTokenRequest(length, token);
  }

  @Override
  public int getEncodedLength() {
    // 1 long (mLength)
    return Longs.BYTES;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mLength);
    // The actual payload is not encoded here, since the RPCMessageEncoder will transfer it in a
    // more efficient way.
  }

  @Override
  public DataBuffer getPayloadDataBuffer() {
    return mToken;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("length", mLength).toString();
  }

  @Override
  public void validate() {
    Preconditions.checkState(mLength >= 0, "Length cannot be negative: %s", mLength);
  }

  /**
   * @return the length of token in bytes
   */
  public long getLength() {
    return mLength;
  }
}
