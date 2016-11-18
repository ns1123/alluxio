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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This represents the request to write a block to a DataServer.
 */
@ThreadSafe
public final class RPCSecretKeyWriteRequest extends RPCRequest {
  // TODO(chaomin): consider putting everything in databuffer.
  private final long mKeyId;
  private final long mExpirationTimeMs;
  private final DataBuffer mSecretKey;

  /**
   * Constructs a new request to write a capability key to a server.
   *
   * @param keyId id the id of the capability key
   * @param expirationTimeMs the expiration time in milliseconds
   * @param secretKey the secret key in DataBuffer
   */
  public RPCSecretKeyWriteRequest(long keyId, long expirationTimeMs, DataBuffer secretKey) {
    mKeyId = keyId;
    mExpirationTimeMs = expirationTimeMs;
    mSecretKey = secretKey;
  }

  /**
   * Constructs a new request to write a capability key to a server.
   *
   * @param keyId id the id of the capability key
   * @param expirationTimeMs the expiration time in milliseconds
   * @param bytes the secret key in bytes
   */
  public RPCSecretKeyWriteRequest(long keyId, long expirationTimeMs, byte[] bytes) {
    mKeyId = keyId;
    mExpirationTimeMs = expirationTimeMs;
    mSecretKey = new DataByteArrayChannel(bytes, 0, bytes.length);
  }

  @Override
  public Type getType() {
    return Type.RPC_SECRET_KEY_WRITE_REQUEST;
  }

  /**
   * Decodes the input {@link ByteBuf} into a {@link RPCSecretKeyWriteRequest} object and
   * returns it.
   *
   * @param in the input {@link ByteBuf}
   * @return The decoded RPCSecretKeyWriteRequest object
   */
  public static RPCSecretKeyWriteRequest decode(ByteBuf in) {
    long keyId = in.readLong();
    long expirationTimeMs = in.readLong();
    DataNettyBuffer secretKey = new DataNettyBuffer(in, in.readableBytes());

    return new RPCSecretKeyWriteRequest(keyId, expirationTimeMs, secretKey);
  }

  @Override
  public int getEncodedLength() {
    // 2 longs (mKeyId, mExpirationTime)
    return Longs.BYTES * 2;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeLong(mKeyId);
    out.writeLong(mExpirationTimeMs);
    // The actual payload is not encoded here, since the RPCMessageEncoder will transfer it in a
    // more efficient way.
  }

  @Override
  public void validate() {
    Preconditions.checkState(mKeyId >= 0, "Key id cannot be negative: %s", mKeyId);
    Preconditions.checkState(mExpirationTimeMs > 0,
        "Expiration time must be positive: %s", mExpirationTimeMs);
  }

  @Override
  public DataBuffer getPayloadDataBuffer() {
    return mSecretKey;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("keyId", mKeyId)
        .add("expirationTimeMs", mExpirationTimeMs)
        .toString();
  }

  /**
   * @return the id of the capability key
   */
  public long getKeyId() {
    return mKeyId;
  }

  /**
   * @return the expiration time of the capability key in milliseconds
   */
  public long getExpirationTimeMs() {
    return mExpirationTimeMs;
  }
}
