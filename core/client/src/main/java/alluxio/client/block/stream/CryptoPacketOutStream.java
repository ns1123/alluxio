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

import alluxio.client.block.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.network.NetworkAddressUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper for {@link PacketOutStream} with encryption feature.
 */
@NotThreadSafe
public final class CryptoPacketOutStream {

  /**
   * Creates a {@link PacketOutStream} that writes to a local file with encryption.
   *
   * @param client the block worker client
   * @param id the ID
   * @param length the block or file length
   * @param tier the target tier
   * @return the {@link PacketOutStream} created
   * @throws IOException if it fails to create the object
   */
  public static PacketOutStream createCryptoLocalPacketOutStream(
      BlockWorkerClient client, long id, long length, int tier) throws IOException {
    PacketWriter packetWriter = CryptoLocalFilePacketWriter.create(client, id, length, tier);
    return new PacketOutStream(packetWriter, length);
  }

  /**
   * Creates a {@link PacketOutStream} that writes to a netty data server with encryption.
   *
   * @param context the file system context
   * @param address the netty data server address
   * @param sessionId the session ID
   * @param id the ID (block ID or UFS file ID)
   * @param length the block or file length
   * @param tier the target tier
   * @param type the request type (either block write or UFS file write)
   * @return the {@link PacketOutStream} created
   * @throws IOException if it fails to create the object
   */
  public static PacketOutStream createCryptoNettyPacketOutStream(
      FileSystemContext context, InetSocketAddress address, long sessionId, long id, long length,
      int tier, Protocol.RequestType type) throws IOException {
    CryptoNettyPacketWriter packetWriter =
        new CryptoNettyPacketWriter(context, address, id, length, sessionId, tier, type);
    return new PacketOutStream(packetWriter, length);
  }

  /**
   * Creates a {@link PacketOutStream} that writes to a netty data server with encryption.
   *
   * @param context the file system context
   * @param address the netty data server address
   * @param length the block or file length
   * @param partialRequest details of the write request which are constant for all requests
   * @return the {@link PacketOutStream} created
   * @throws IOException if it fails to create the object
   */
  public static PacketOutStream createCryptoNettyPacketOutStream(
      FileSystemContext context, InetSocketAddress address, long length,
      Protocol.WriteRequest partialRequest)
      throws IOException {
    CryptoNettyPacketWriter packetWriter =
        new CryptoNettyPacketWriter(context, address, length, partialRequest);
    return new PacketOutStream(packetWriter, length);
  }

  /**
   * Creates a {@link PacketOutStream} that writes to a list of locations with encryption.
   *
   * @param context the file system context
   * @param clients a list of block worker clients
   * @param id the ID (block ID or UFS file ID)
   * @param length the block or file length
   * @param tier the target tier
   * @param type the request type (either block write or UFS file write)
   * @return the {@link PacketOutStream} created
   * @throws IOException if it fails to create the object
   */
  public static PacketOutStream createCryptoReplicatedPacketOutStream(
      FileSystemContext context, List<BlockWorkerClient> clients, long id, long length, int tier,
      Protocol.RequestType type) throws IOException {
    String localHost = NetworkAddressUtils.getClientHostName();

    List<PacketWriter> packetWriters = new ArrayList<>();
    for (BlockWorkerClient client : clients) {
      if (client.getWorkerNetAddress().getHost().equals(localHost)) {
        packetWriters.add(LocalFilePacketWriter.create(client, id, tier));
      } else {
        packetWriters.add(new CryptoNettyPacketWriter(
            context, client.getDataServerAddress(), id, length, client.getSessionId(), tier, type));
      }
    }
    return new PacketOutStream(packetWriters, length);
  }

  private CryptoPacketOutStream() {}  // prevent instantiation
}
