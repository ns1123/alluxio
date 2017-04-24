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

package alluxio.worker.netty;

import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.RPCSecretKeyWriteRequest;
import alluxio.network.protocol.RPCSecretKeyWriteResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.security.capability.CapabilityKey;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The secure Netty server handler for secure secret key exchange.
 */
@ChannelHandler.Sharable
@NotThreadSafe
public class SecretKeyServerHandler extends SimpleChannelInboundHandler<RPCMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(SecretKeyServerHandler.class);

  private final WorkerProcess mWorkerProcess;

  /**
   * Creates a new {@link SecretKeyServerHandler} instance with alluxio worker service.
   *
   * @param workerProcess the Alluxio worker process
   */
  public SecretKeyServerHandler(final WorkerProcess workerProcess) {
    Preconditions.checkNotNull(workerProcess, "workerProcess");
    mWorkerProcess = workerProcess;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ctx.pipeline().get(SslHandler.class).handshakeFuture().addListener(
        new GenericFutureListener<Future<Channel>>() {
          @Override
          public void operationComplete(Future<Channel> future) throws Exception {
            LOG.debug("Channel active operation completed");
          }
        });
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, final RPCMessage msg) {
    if (msg.getType() != RPCMessage.Type.RPC_SECRET_KEY_WRITE_REQUEST) {
      throw new IllegalArgumentException(
          "No handler implementation for rpc msg type: " + msg.getType());
    }
    assert msg instanceof RPCSecretKeyWriteRequest;
    RPCSecretKeyWriteRequest req = (RPCSecretKeyWriteRequest) msg;
    req.validate();
    final DataBuffer data = req.getPayloadDataBuffer();
    ByteBuffer buffer = data.getReadOnlyByteBuffer();
    if (buffer.remaining() == 0) {
      LOG.error("Received secret key length is 0.");
      ctx.channel().writeAndFlush(
          RPCSecretKeyWriteResponse.createErrorResponse(RPCResponse.Status.INVALID_SECRET_KEY))
          .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      return;
    }
    byte[] secretKey = new byte[buffer.remaining()];
    buffer.get(secretKey);

    CapabilityKey key;
    try {
      key = new CapabilityKey(req.getKeyId(), req.getExpirationTimeMs(), secretKey);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      LOG.error("Received invalid secret key.", e);
      ctx.channel().writeAndFlush(
          RPCSecretKeyWriteResponse.createErrorResponse(RPCResponse.Status.INVALID_SECRET_KEY))
          .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      return;
    }

    Preconditions.checkNotNull(mWorkerProcess.getWorker(BlockWorker.class));
    mWorkerProcess.getWorker(BlockWorker.class).getCapabilityCache().setCapabilityKey(key);
    LOG.debug("Received secret key, id {}", key.getKeyId());
    // Send the secret key write response.
    ctx.channel().writeAndFlush(new RPCSecretKeyWriteResponse(RPCResponse.Status.SUCCESS));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }
}
