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

import alluxio.client.BoundedStream;
import alluxio.client.QuietlyCancelable;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.io.Closer;

import java.io.FilterOutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to write a block to Alluxio. An instance of this class can be obtained by
 * calling
 * {@link alluxio.client.block.AlluxioBlockStore#getOutStream(long, long, OutStreamOptions)}.
 */
@NotThreadSafe
public class BlockOutStream extends FilterOutputStream implements BoundedStream, QuietlyCancelable {
  private final long mBlockId;
  private final long mBlockSize;
  private final Closer mCloser;
  // ALLUXIO CS REPLACE
  // private final BlockWorkerClient mBlockWorkerClient;
  // ALLUXIO CS WITH
  private final java.util.List<BlockWorkerClient> mBlockWorkerClients;
  // ALLUXIO CS END
  private final PacketOutStream mOutStream;
  private boolean mClosed;

  /**
   * Creates a new local block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createLocalBlockOutStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context, OutStreamOptions options) {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient client = closer.register(context.createBlockWorkerClient(workerNetAddress));
      // ALLUXIO CS ADD
      client.setCapabilityNonRPC(options.getCapabilityFetcher());
      client.updateCapability();
      // ALLUXIO CS END
      PacketOutStream outStream = PacketOutStream
          .createLocalPacketOutStream(client, blockId, blockSize, options);
      closer.register(outStream);
      return new BlockOutStream(outStream, blockId, blockSize, client, options);
    } catch (RuntimeException e) {
      CommonUtils.closeQuietly(closer);
      throw e;
    }
  }

  /**
   * Creates a new remote block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker network address
   * @param context the file system context
   * @param options the options
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createRemoteBlockOutStream(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context, OutStreamOptions options) {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient client = closer.register(context.createBlockWorkerClient(workerNetAddress));
      // ALLUXIO CS ADD
      client.setCapabilityNonRPC(options.getCapabilityFetcher());
      client.updateCapability();
      // ALLUXIO CS END

      PacketOutStream outStream = PacketOutStream
          .createNettyPacketOutStream(context, client.getDataServerAddress(), client.getSessionId(),
              blockId, blockSize, Protocol.RequestType.ALLUXIO_BLOCK, options);
      closer.register(outStream);
      return new BlockOutStream(outStream, blockId, blockSize, client, options);
    } catch (RuntimeException e) {
      CommonUtils.closeQuietly(closer);
      throw e;
    }
  }

  // ALLUXIO CS ADD
  /**
   * Creates a new remote block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddresses the worker network address
   * @param context the file system context
   * @param options the options
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createReplicatedBlockOutStream(long blockId, long blockSize,
      java.util.List<WorkerNetAddress> workerNetAddresses, FileSystemContext context,
      OutStreamOptions options) {
    Closer closer = Closer.create();
    try {
      java.util.List<BlockWorkerClient> clients =
          new java.util.ArrayList<>(workerNetAddresses.size());
      for (WorkerNetAddress workerNetAddress : workerNetAddresses) {
        BlockWorkerClient client = closer.register(context.createBlockWorkerClient(workerNetAddress));
        client.setCapabilityNonRPC(options.getCapabilityFetcher());
        client.updateCapability();
        clients.add(client);
      }
      PacketOutStream outStream = PacketOutStream.createReplicatedPacketOutStream(context,
          clients, blockId, blockSize, options.getWriteTier(), Protocol.RequestType.ALLUXIO_BLOCK);
      return new BlockOutStream(outStream, blockId, blockSize, clients, options);
    } catch (RuntimeException e) {
      CommonUtils.closeQuietly(closer);
      throw e;
    }
  }

  // ALLUXIO CS END
  // Explicitly overriding some write methods which are not efficiently implemented in
  // FilterOutStream.

  @Override
  public void write(byte[] b) {
    mOutStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    mOutStream.write(b, off, len);
  }

  @Override
  public long remaining() {
    return mOutStream.remaining();
  }

  @Override
  public void cancel() {
    if (mClosed) {
      return;
    }
    Exception exception = null;
    try {
      mOutStream.cancel();
    } catch (Exception e) {
      exception = e;
    }
    // ALLUXIO CS REPLACE
    // try {
    //   mBlockWorkerClient.cancelBlock(mBlockId);
    // } catch (Exception e) {
    //   exception = e;
    // }
    // ALLUXIO CS WITH
    for (BlockWorkerClient client : mBlockWorkerClients) {
      try {
        client.cancelBlock(mBlockId);
      } catch (Exception e) {
        exception = e;
      }
    }
    // ALLUXIO CS END

    if (exception == null) {
      mClosed = true;
      return;
    }

    CommonUtils.closeQuietly(mCloser);
    mClosed = true;
    throw AlluxioStatusException.from(exception);
  }

  @Override
  public void close() {
    if (mClosed) {
      return;
    }
    try {
      mOutStream.close();
      if (remaining() < mBlockSize) {
        // ALLUXIO CS REPLACE
        // mBlockWorkerClient.cacheBlock(mBlockId);
        // ALLUXIO CS WITH
        for (BlockWorkerClient client : mBlockWorkerClients) {
          client.cacheBlock(mBlockId);
        }
        // ALLUXIO CS END
      }
    } finally {
      CommonUtils.close(mCloser);
      mClosed = true;
    }
  }

  /**
   * Creates a new block output stream.
   *
   * @param outStream the {@link PacketOutStream} associated with this {@link BlockOutStream}
   * @param blockId the block id
   * @param blockSize the block size
   * @param blockWorkerClient the block worker client
   * @param options the options
   */
  protected BlockOutStream(PacketOutStream outStream, long blockId, long blockSize,
      BlockWorkerClient blockWorkerClient, OutStreamOptions options) {
    super(outStream);

    mOutStream = outStream;
    mBlockId = blockId;
    mBlockSize = blockSize;
    mCloser = Closer.create();
    // ALLUXIO CS REPLACE
    // mBlockWorkerClient = mCloser.register(blockWorkerClient);
    // ALLUXIO CS WITH
    mCloser.register(blockWorkerClient);
    mBlockWorkerClients = new java.util.ArrayList<>();
    mBlockWorkerClients.add(blockWorkerClient);
    // ALLUXIO CS END
    mClosed = false;
  }
  // ALLUXIO CS ADD

  /**
   * Creates a new block output stream.
   *
   * @param outStream the {@link PacketOutStream} associated with this {@link BlockOutStream}
   * @param blockId the block id
   * @param blockSize the block size
   * @param blockWorkerClients the block worker clients
   * @param options the options
   */
  private BlockOutStream(PacketOutStream outStream, long blockId, long blockSize,
      java.util.List<BlockWorkerClient> blockWorkerClients, OutStreamOptions options) {
    super(outStream);

    mOutStream = outStream;
    mBlockId = blockId;
    mBlockSize = blockSize;
    mCloser = Closer.create();
    for (BlockWorkerClient client : blockWorkerClients) {
      mCloser.register(client);
    }
    mBlockWorkerClients = blockWorkerClients;
    mClosed = false;
  }
  // ALLUXIO CS END
}
