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

import alluxio.client.RemoteBlockWriter;
import alluxio.client.block.BlockWorkerClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidCapabilityException;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Read data from remote data server using Netty.
 */
@NotThreadSafe
public final class NettyRemoteBlockWriterWithCapability implements RemoteBlockWriter {
  private BlockWorkerClient mBlockWorkerClient;
  private NettyRemoteBlockWriter mNettyRemoteBlockWriter;

  /**
   * Creates a new {@link NettyRemoteBlockReaderWithCapability}.
   *
   * @param nettyRemoteBlockWriter the normal netty remote block writer
   * @param blockWorkerClient the block worker client
   */
  public NettyRemoteBlockWriterWithCapability(NettyRemoteBlockWriter nettyRemoteBlockWriter,
      BlockWorkerClient blockWorkerClient) {
    mBlockWorkerClient = blockWorkerClient;
    mNettyRemoteBlockWriter = nettyRemoteBlockWriter;
  }

  @Override
  public void open(InetSocketAddress address, long blockId, long sessionId) throws IOException {
    mNettyRemoteBlockWriter.open(address, blockId, sessionId);
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    try {
      mNettyRemoteBlockWriter.write(bytes, offset, length);
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
      mNettyRemoteBlockWriter.write(bytes, offset, length);
    }
  }

  @Override
  public void close() throws IOException {
    mNettyRemoteBlockWriter.close();
  }
}
