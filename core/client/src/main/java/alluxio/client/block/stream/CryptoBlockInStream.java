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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.resource.LockBlockResource;
import alluxio.exception.AlluxioException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper for {@link BlockInStream} with encryption feature.
 */
@NotThreadSafe
public final class CryptoBlockInStream {
  /**
   * Creates an instance of local {@link BlockInStream} that reads from local worker with
   * encryption.
   *
   * @param blockId the block ID
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @throws IOException if it fails to create an instance
   * @return the {@link BlockInStream} created
   */
  // TODO(peis): Use options idiom (ALLUXIO-2579).
  public static BlockInStream createCryptoLocalBlockInStream(
      long blockId, long blockSize, WorkerNetAddress workerNetAddress, FileSystemContext context,
      InStreamOptions options) throws IOException {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      blockWorkerClient.setCapabilityNonRPC(options.getCapabilityFetcher());
      LockBlockResource lockBlockResource =
          closer.register(blockWorkerClient.lockBlock(blockId, LockBlockOptions.defaults()));
      PacketInStream inStream = closer.register(CryptoPacketInStream
          .createCryptoLocalPacketInStream(lockBlockResource.getResult().getBlockPath(), blockId,
              blockSize));
      blockWorkerClient.accessBlock(blockId);
      return new BlockInStream(inStream, blockWorkerClient, closer, options);
    } catch (AlluxioException | IOException e) {
      CommonUtils.closeQuietly(closer);
      throw CommonUtils.castToIOException(e);
    }
  }

  /**
   * Creates an instance of remote {@link BlockInStream} that reads from a remote worker with
   * encryption.
   *
   * @param blockId the block ID
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @throws IOException if it fails to create an instance
   * @return the {@link BlockInStream} created
   */
  // TODO(peis): Use options idiom (ALLUXIO-2579).
  public static BlockInStream createCryptoRemoteBlockInStream(
      long blockId, long blockSize, WorkerNetAddress workerNetAddress, FileSystemContext context,
      InStreamOptions options)
    throws IOException {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      blockWorkerClient.setCapabilityNonRPC(options.getCapabilityFetcher());
      LockBlockResource lockBlockResource =
          closer.register(blockWorkerClient.lockBlock(blockId, LockBlockOptions.defaults()));
      PacketInStream inStream = closer.register(CryptoPacketInStream
          .createCryptoNettyPacketInStream(context, blockWorkerClient.getDataServerAddress(),
              blockId, lockBlockResource.getResult().getLockId(), blockWorkerClient.getSessionId(),
              blockSize, false, Protocol.RequestType.ALLUXIO_BLOCK));
      blockWorkerClient.accessBlock(blockId);
      return new BlockInStream(inStream, blockWorkerClient, closer, options);
    } catch (AlluxioException | IOException e) {
      CommonUtils.closeQuietly(closer);
      throw CommonUtils.castToIOException(e);
    }
  }

  /**
   * Creates an instance of {@link BlockInStream} with encryption.
   *
   * This method keeps polling the block worker until the block is cached to Alluxio or
   * it successfully acquires a UFS read token with a timeout.
   * (1) If the block is cached to Alluxio after polling, it returns {@link BlockInStream}
   *     to read the block from Alluxio storage.
   * (2) If a UFS read token is acquired after polling, it returns {@link BlockInStream}
   *     to read the block from an Alluxio worker that reads the block from UFS.
   * (3) If the polling times out, an {@link IOException} with cause
   *     {@link alluxio.exception.UfsBlockAccessTokenUnavailableException} is thrown.
   *
   * @param context the file system context
   * @param ufsPath the UFS path
   * @param blockId the block ID
   * @param blockSize the block size
   * @param blockStart the position at which the block starts in the file
   * @param workerNetAddress the worker network address
   * @param options the options
   * @throws IOException if it fails to create an instance
   * @return the {@link BlockInStream} created
   */
  // TODO(peis): Use options idiom (ALLUXIO-2579).
  public static BlockInStream createCryptoUfsBlockInStream(
      FileSystemContext context, String ufsPath, long blockId, long blockSize, long blockStart,
      WorkerNetAddress workerNetAddress, InStreamOptions options) throws IOException {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      LockBlockOptions lockBlockOptions =
          LockBlockOptions.defaults().setUfsPath(ufsPath).setOffset(blockStart)
              .setBlockSize(blockSize).setMaxUfsReadConcurrency(options.getMaxUfsReadConcurrency());

      LockBlockResult lockBlockResult =
          closer.register(blockWorkerClient.lockUfsBlock(blockId, lockBlockOptions)).getResult();
      PacketInStream inStream;
      if (lockBlockResult.getLockBlockStatus().blockInAlluxio()) {
        boolean local = blockWorkerClient.getDataServerAddress().getHostName()
            .equals(NetworkAddressUtils.getClientHostName());
        if (local && Configuration.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED)) {
          inStream = closer.register(CryptoPacketInStream
              .createCryptoLocalPacketInStream(lockBlockResult.getBlockPath(), blockId, blockSize));
        } else {
          inStream = closer.register(CryptoPacketInStream
              .createCryptoNettyPacketInStream(context, blockWorkerClient.getDataServerAddress(),
                  blockId, lockBlockResult.getLockId(), blockWorkerClient.getSessionId(), blockSize,
                  false, Protocol.RequestType.ALLUXIO_BLOCK));
        }
        blockWorkerClient.accessBlock(blockId);
      } else {
        Preconditions.checkState(lockBlockResult.getLockBlockStatus().ufsTokenAcquired());
        inStream = closer.register(CryptoPacketInStream
            .createCryptoNettyPacketInStream(context, blockWorkerClient.getDataServerAddress(),
                blockId, lockBlockResult.getLockId(), blockWorkerClient.getSessionId(), blockSize,
                !options.getAlluxioStorageType().isStore(), Protocol.RequestType.UFS_BLOCK));
      }
      return new BlockInStream(inStream, blockWorkerClient, closer, options);
    } catch (AlluxioException | IOException e) {
      CommonUtils.closeQuietly(closer);
      throw CommonUtils.castToIOException(e);
    }
  }

  private CryptoBlockInStream() {}  // prevent instantiation
}
