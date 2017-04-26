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

import alluxio.client.file.FileSystemContext;
import alluxio.proto.dataserver.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper for {@link PacketInStream} with encryption feature.
 */
@NotThreadSafe
public final class CryptoPacketInStream {
  /**
   * Creates a {@link CryptoPacketInStream} to read from a local file with encryption.
   *
   * @param path the local file path
   * @param id the ID
   * @param length the block or file length
   * @return the {@link PacketInStream} created
   * @throws IOException if it fails to create the object
   */
  public static PacketInStream createCryptoLocalPacketInStream(
      String path, long id, long length) throws IOException {
    return new PacketInStream(new CryptoLocalFilePacketReader.Factory(path), id, length);
  }

  /**
   * Creates a {@link CryptoPacketInStream} to read from a netty data server with encryption.
   *
   * @param context the file system context
   * @param address the network address of the netty data server
   * @param id the ID
   * @param lockId the lock ID (set to -1 if not applicable)
   * @param sessionId the session ID (set to -1 if not applicable)
   * @param length the block or file length
   * @param noCache do not cache the block to the Alluxio worker if read from UFS when this is set
   * @param type the read request type (either block read or UFS file read)
   * @return the {@link PacketInStream} created
   */
  public static PacketInStream createCryptoNettyPacketInStream(
      FileSystemContext context, InetSocketAddress address, long id, long lockId, long sessionId,
      long length, boolean noCache, Protocol.RequestType type) {
    PacketReader.Factory factory =
        new CryptoNettyPacketReader.Factory(context, address, id, lockId, sessionId, noCache, type);
    return new PacketInStream(factory, id, length);
  }

  private CryptoPacketInStream() {}  // prevent instantiation
}
