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
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.io.Closer;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper for {@link BlockOutStream} with encryption feature.
 */
@NotThreadSafe
public final class CryptoBlockOutStream {
  /**
   * Creates a new local block output stream with encryption.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @throws IOException if an I/O error occurs
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createCryptoLocalBlockOutStream(
      long blockId, long blockSize, WorkerNetAddress workerNetAddress, FileSystemContext context,
      OutStreamOptions options)
      throws IOException {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient client = closer.register(context.createBlockWorkerClient(workerNetAddress));
      client.setCapabilityNonRPC(options.getCapabilityFetcher());
      client.updateCapability();
      PacketOutStream outStream = CryptoPacketOutStream
          .createCryptoLocalPacketOutStream(client, blockId, blockSize, options.getWriteTier());
      closer.register(outStream);
      return new BlockOutStream(outStream, blockId, blockSize, client, options);
    } catch (AlluxioException e) {
      closer.close();
      throw new IOException(e);
    } catch (IOException e) {
      CommonUtils.closeQuietly(closer);
      throw e;
    }
  }

  /**
   * Creates a new remote block output stream with encryption.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @throws IOException if an I/O error occurs
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createCryptoRemoteBlockOutStream(
      long blockId, long blockSize, WorkerNetAddress workerNetAddress, FileSystemContext context,
      OutStreamOptions options) throws IOException {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient client = closer.register(context.createBlockWorkerClient(workerNetAddress));
      client.setCapabilityNonRPC(options.getCapabilityFetcher());
      client.updateCapability();

      PacketOutStream outStream = CryptoPacketOutStream
          .createCryptoNettyPacketOutStream(context, client.getDataServerAddress(),
              client.getSessionId(), blockId, blockSize, options.getWriteTier(),
              Protocol.RequestType.ALLUXIO_BLOCK);
      closer.register(outStream);
      return new BlockOutStream(outStream, blockId, blockSize, client, options);
    } catch (AlluxioException e) {
      closer.close();
      throw new IOException(e);
    } catch (IOException e) {
      CommonUtils.closeQuietly(closer);
      throw e;
    }
  }

  /**
   * Creates a new remote block output stream with encryption.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddresses the worker network address
   * @param context the file system context
   * @param options the options
   * @throws IOException if an I/O error occurs
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createCryptoReplicatedBlockOutStream(
      long blockId, long blockSize, java.util.List<WorkerNetAddress> workerNetAddresses,
      FileSystemContext context, OutStreamOptions options) throws IOException {
    Closer closer = Closer.create();
    try {
      java.util.List<BlockWorkerClient> clients =
          new java.util.ArrayList<>(workerNetAddresses.size());
      for (WorkerNetAddress workerNetAddress : workerNetAddresses) {
        BlockWorkerClient client = closer.register(
            context.createBlockWorkerClient(workerNetAddress));
        client.setCapabilityNonRPC(options.getCapabilityFetcher());
        client.updateCapability();
        clients.add(client);
      }
      PacketOutStream outStream = CryptoPacketOutStream.createCryptoReplicatedPacketOutStream(
          context, clients, blockId, blockSize, options.getWriteTier(),
          Protocol.RequestType.ALLUXIO_BLOCK);
      return new BlockOutStream(outStream, blockId, blockSize, clients, options);
    } catch (AlluxioException e) {
      closer.close();
      throw new IOException(e);
    } catch (IOException e) {
      closer.close();
      throw e;
    }
  }

  private CryptoBlockOutStream() {}  // prevent instantiation
}
