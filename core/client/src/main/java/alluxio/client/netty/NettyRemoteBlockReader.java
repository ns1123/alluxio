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

import alluxio.Constants;
import alluxio.client.RemoteBlockReader;
import alluxio.exception.ExceptionMessage;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Read data from remote data server using Netty.
 */
@NotThreadSafe
public final class NettyRemoteBlockReader implements RemoteBlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Bootstrap mClientBootstrap;
  private final ClientHandler mHandler;
  /** A reference to read response so we can explicitly release the resource after reading. */
  private RPCBlockReadResponse mReadResponse = null;

  /**
   * Creates a new {@link NettyRemoteBlockReader}.
   *
   * TODO(gene): Creating a new remote block reader may be expensive, so consider a connection pool.
   */
  public NettyRemoteBlockReader() {
    mHandler = new ClientHandler();
    mClientBootstrap = NettyClient.createClientBootstrap(mHandler);
  }

  /**
   * Constructor.
   *
   * @param clientBootstrap bootstrap class of the client channel
   * @param clientHandler handler of the client channel
   */
  public NettyRemoteBlockReader(Bootstrap clientBootstrap, ClientHandler clientHandler) {
    mClientBootstrap = clientBootstrap;
    mHandler = clientHandler;
  }

  @Override
  public ByteBuffer readRemoteBlock(InetSocketAddress address, long blockId, long offset,
      long length, long lockId, long sessionId) throws IOException {
    SingleResponseListener listener = null;
    try {
      ChannelFuture f = mClientBootstrap.connect(address).sync();

      LOG.info("Connected to remote machine {}", address);
      Channel channel = f.channel();
      listener = new SingleResponseListener();
      mHandler.addListener(listener);
      // ENTERPRISE REPLACE
      // channel.writeAndFlush(new RPCBlockReadRequest(blockId, offset, length, lockId, sessionId));
      //
      // RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
      // channel.close().sync();
      //
      // switch (response.getType()) {
      //   case RPC_BLOCK_READ_RESPONSE:
      //     RPCBlockReadResponse blockResponse = (RPCBlockReadResponse) response;
      //     LOG.info("Data {} from remote machine {} received", blockId, address);
      //
      //     RPCResponse.Status status = blockResponse.getStatus();
      //     if (status == RPCResponse.Status.SUCCESS) {
      //       // always clear the previous response before reading another one
      //       close();
      //       mReadResponse = blockResponse;
      //       return blockResponse.getPayloadDataBuffer().getReadOnlyByteBuffer();
      //     }
      //     throw new IOException(status.getMessage() + " response: " + blockResponse);
      //   case RPC_ERROR_RESPONSE:
      //     RPCErrorResponse error = (RPCErrorResponse) response;
      //     throw new IOException(error.getStatus().getMessage());
      //   default:
      //     throw new IOException(ExceptionMessage.UNEXPECTED_RPC_RESPONSE
      //         .getMessage(response.getType(), RPCMessage.Type.RPC_BLOCK_READ_RESPONSE));
      // }
      // ENTERPRISE WITH
      RPCBlockReadRequest readRequest = new RPCBlockReadRequest(
          blockId, offset, length, lockId, sessionId);
      if (alluxio.Configuration.get(alluxio.PropertyKey.SECURITY_AUTHENTICATION_TYPE).equals(
          alluxio.security.authentication.AuthType.KERBEROS.getAuthName())) {
        channel.flush();
      } else {
        channel.writeAndFlush(readRequest);
      }
      return handleResponse(channel, listener, readRequest);
      // ENTERPRISE END
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (listener != null) {
        mHandler.removeListener(listener);
      }
    }
  }

  // ENTERPRISE ADD
  private ByteBuffer handleResponse(Channel channel, SingleResponseListener listener,
      RPCBlockReadRequest readRequest) throws Exception {
    RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);

    switch (response.getType()) {
      case RPC_BLOCK_READ_RESPONSE:
        RPCBlockReadResponse blockResponse = (RPCBlockReadResponse) response;
        LOG.debug("Data {} from remote machine received", readRequest.getBlockId());

        RPCResponse.Status status = blockResponse.getStatus();
        if (status == RPCResponse.Status.SUCCESS) {
          // always clear the previous response before reading another one
          close();
          mReadResponse = blockResponse;
          channel.close().sync();
          if (listener != null) {
            mHandler.removeListener(listener);
          }
          return blockResponse.getPayloadDataBuffer().getReadOnlyByteBuffer();
        }
        throw new IOException(status.getMessage() + " response: " + blockResponse);
      case RPC_ERROR_RESPONSE:
        RPCErrorResponse error = (RPCErrorResponse) response;
        channel.close().sync();
        throw new IOException(error.getStatus().getMessage());
      case RPC_SASL_COMPLETE_RESPONSE:
        mHandler.removeListener(listener);
        SingleResponseListener newListener = new SingleResponseListener();
        mHandler.addListener(newListener);
        channel.writeAndFlush(readRequest);
        return handleResponse(channel, newListener, readRequest);
      default:
        channel.close().sync();
        throw new IOException(ExceptionMessage.UNEXPECTED_RPC_RESPONSE
            .getMessage(response.getType(), RPCMessage.Type.RPC_BLOCK_READ_RESPONSE));
    }
  }

  // ENTERPRISE END
  /**
   * {@inheritDoc}
   *
   * Release the underlying buffer of previous/current read response.
   */
  @Override
  public void close() throws IOException {
    if (mReadResponse != null) {
      mReadResponse.getPayloadDataBuffer().release();
      mReadResponse = null;
    }
  }
}
