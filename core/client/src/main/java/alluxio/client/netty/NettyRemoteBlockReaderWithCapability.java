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

package alluxio.client.netty;

import alluxio.client.RemoteBlockReader;
import alluxio.client.block.BlockWorkerClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidCapabilityException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Read data from remote data server using Netty.
 */
@NotThreadSafe
public final class NettyRemoteBlockReaderWithCapability implements RemoteBlockReader {
  private BlockWorkerClient mBlockWorkerClient;
  private NettyRemoteBlockReader mNettyRemoteBlockReader;

  /**
   * Creates a new {@link NettyRemoteBlockReaderWithCapability}.
   *
   * @param nettyRemoteBlockReader the normal netty remote block reader
   * @param blockWorkerClient the block worker client
   */
  public NettyRemoteBlockReaderWithCapability(NettyRemoteBlockReader nettyRemoteBlockReader,
      BlockWorkerClient blockWorkerClient) {
    mBlockWorkerClient = blockWorkerClient;
    mNettyRemoteBlockReader = nettyRemoteBlockReader;
  }

  @Override
  public ByteBuffer readRemoteBlock(InetSocketAddress address, long blockId, long offset,
      long length, long lockId, long sessionId) throws IOException {
    try {
      return mNettyRemoteBlockReader.readRemoteBlock(
          address, blockId, offset, length, lockId, sessionId);
    } catch (IOException e) {
      if (!(e.getCause() instanceof InvalidCapabilityException)) {
        throw e;
      }
      try {
        mBlockWorkerClient.fetchAndUpdateCapability();
      } catch (AlluxioException ae) {
        throw new IOException(ae);
      }
      // Retry only once if we see invalid capability.
      return mNettyRemoteBlockReader.readRemoteBlock(
          address, blockId, offset, length, lockId, sessionId);
    }
  }

  @Override
  public void close() throws IOException {
    mNettyRemoteBlockReader.close();
  }
}
