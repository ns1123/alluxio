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
import alluxio.network.protocol.databuffer.DataNettyBuffer;

import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * This represents the request to send a Sasl token message.
 */
public class RPCSaslTokenRequest extends RPCRequest {
  private final DataBuffer mToken;

  /**
   * Constructs a new request to send a Sasl token.
   *
   * @param token the Sasl token
   */
  public RPCSaslTokenRequest(DataBuffer token) {
    mToken = token;
  }

  /**
   * Constructs a new request with given byte array.
   *
   * @param bytes the token in byte array
   */
  public RPCSaslTokenRequest(byte[] bytes) {
    mToken = new DataByteArrayChannel(bytes, 0, bytes.length);
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
    in.readLong(); // Discard the unused length field.

    DataNettyBuffer token = new DataNettyBuffer(in, in.readableBytes());
    return new RPCSaslTokenRequest(token);
  }

  @Override
  public int getEncodedLength() {
    // The length field in the encoded buffer is for backward compatibility.
    return Longs.BYTES;
  }

  @Override
  public void encode(ByteBuf out) {
    // The length field in the encoded buffer is for backward compatibility.
    out.writeLong(mToken.getLength());
    // The actual payload is not encoded here, since the RPCMessageEncoder will transfer it in a
    // more efficient way.
  }

  @Override
  public DataBuffer getPayloadDataBuffer() {
    return mToken;
  }

  /**
   * @return the token as a byte array
   */
  public byte[] getTokenAsArray() {
    ByteBuffer buffer = mToken.getReadOnlyByteBuffer();
    if (buffer.remaining() == 0) {
      return null;
    }
    byte[] token = new byte[buffer.remaining()];
    buffer.get(token);
    return token;
  }

  @Override
  public String toString() {
    return "";
  }

  @Override
  public void validate() {}

  /**
   * @return the length of token in bytes
   */
  public long getLength() {
    return mToken.getLength();
  }
}
