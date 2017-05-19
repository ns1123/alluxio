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

import alluxio.Constants;
import alluxio.client.LayoutUtils;
import alluxio.client.security.CryptoKey;
import alluxio.client.security.CryptoUtils;
import alluxio.proto.security.EncryptionProto;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * The wrapper on {@link PacketWriter} to write packets with encryption.
 */
public class CryptoPacketWriter implements PacketWriter {
  private static final String CIPHER_NAME = "AES/GCM/NoPadding";

  private PacketWriter mPacketWriter;
  private EncryptionProto.Meta mMeta;

  /**
   * Creates a new {@link CryptoPacketWriter} with a non-crypto {@link PacketWriter}.
   *
   * @param packetWriter the non-crypto packet writer
   * @param meta the encryption metadata
   */
  public CryptoPacketWriter(PacketWriter packetWriter, EncryptionProto.Meta meta) {
    mPacketWriter = packetWriter;
    mMeta = meta;
  }

  /**
   * Encrypts and then writes a packet. This method takes the ownership of this packet even if it
   * fails to write the packet.
   *
   * @param packet the packet in plaintext
   */
  @Override
  public void writePacket(ByteBuf packet) throws IOException {
    CryptoKey encryptKey = new CryptoKey(
        CIPHER_NAME, Constants.ENCRYPTION_KEY_FOR_TESTING.getBytes(),
        Constants.ENCRYPTION_IV_FOR_TESTING.getBytes(), true);
    // Note: packet ByteBuf is released by encryptChunks.
    // TODO(chaomin): need to distinguish the first packet of a block when block header is not empty
    ByteBuf encrypted = CryptoUtils.encryptChunks(mMeta, encryptKey, packet);
    mPacketWriter.writePacket(encrypted);
  }

  @Override
  public void flush() throws IOException {
    // Note: flush at non-chunk-boundary is not support with GCM encryption mode.
    mPacketWriter.flush();
  }

  @Override
  public int packetSize() {
    // The packet size is the physical packet length.
    return (int) LayoutUtils.toLogicalLength(mMeta, 0L, mPacketWriter.packetSize());
  }

  @Override
  public long pos() {
    // map the physical offset back to the logical offset.
    return LayoutUtils.toLogicalOffset(mMeta, mPacketWriter.pos());
  }

  @Override
  public void cancel() throws IOException {
    mPacketWriter.cancel();
  }

  @Override
  public void close() throws IOException {
    mPacketWriter.close();
  }
}
